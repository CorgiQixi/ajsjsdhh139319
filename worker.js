// ==================== Worker 入口 ====================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const sessionId = url.searchParams.get('sessionId');
    const userId = url.searchParams.get('userId');

    if (!sessionId || !userId) {
      return new Response('Missing sessionId or userId', { status: 400 });
    }

    // 根据 sessionId 生成 Durable Object ID（相同 sessionId 映射到同一实例）
    const id = env.CHAT_SESSION.idFromName(sessionId);
    const stub = env.CHAT_SESSION.get(id);

    // 转发请求（包括 WebSocket 升级）
    return stub.fetch(request);
  }
};

// ==================== Durable Object 定义 ====================
export class ChatSession {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;

    // 初始化 SQLite 数据库
    const sql = ctx.storage.sql;
    sql.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        senderId TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp INTEGER NOT NULL
      );
    `);
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_timestamp ON messages (timestamp);`);

    // 用户信息缓存（内存级，DO 重启或休眠后重新获取）
    this.userCache = {
      timestamp: 0,          // 上次刷新时间（毫秒）
      users: new Map(),      // userId -> { name, avatarUrl }
    };
  }

  // 处理所有请求（WebSocket 升级）
  async fetch(request) {
    const url = new URL(request.url);
    const userId = url.searchParams.get('userId');
    const lastTs = parseInt(url.searchParams.get('lastTs')) || 0;

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // 接受 WebSocket，关联 userId（用于 hibernation 回调识别）
    this.ctx.acceptWebSocket(server, [userId]);

    // 补发离线消息（如果提供了 lastTs）
    if (lastTs > 0) {
      await this.sendHistoricalMessages(server, userId, lastTs);
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // 收到 WebSocket 消息
  async webSocketMessage(ws, message) {
    const [userId] = ws.deserializeAttachment();

    try {
      const data = JSON.parse(message);
      const content = data.content;
      if (!content || typeof content !== 'string') {
        ws.send(JSON.stringify({ error: 'Invalid message format' }));
        return;
      }

      const timestamp = Date.now();
      const sql = this.ctx.storage.sql;

      // 存入消息
      sql.exec(
        'INSERT INTO messages (senderId, content, timestamp) VALUES (?, ?, ?)',
        userId,
        content,
        timestamp
      );

      // 清理旧消息（最多保留 1000 条）
      this.cleanupOldMessages();

      // 获取发送者资料（从缓存或 API）
      const userInfo = await this.getUserInfo(userId);

      // 构造广播消息
      const broadcastMsg = {
        type: 'message',
        senderId: userId,
        content,
        timestamp,
        name: userInfo.name,
        avatarUrl: userInfo.avatarUrl,
      };

      // 广播给所有在线连接（包括发送者自己，便于本地回显）
      this.broadcast(JSON.stringify(broadcastMsg));
    } catch (err) {
      console.error('Error processing message:', err);
      ws.send(JSON.stringify({ error: 'Internal server error' }));
    }
  }

  // WebSocket 关闭（可选）
  webSocketClose(ws, code, reason, wasClean) {
    // 可以在此记录用户离开等
  }

  // ==================== 辅助方法 ====================

  /**
   * 补发历史消息：查询 timestamp > lastTs 的消息，附上用户资料逐条发送
   */
  async sendHistoricalMessages(ws, userId, lastTs) {
    const sql = this.ctx.storage.sql;
    const rows = sql.exec(
      'SELECT senderId, content, timestamp FROM messages WHERE timestamp > ? ORDER BY timestamp ASC',
      lastTs
    ).toArray();

    if (rows.length === 0) return;

    // 收集所有不重复的 senderId，批量获取用户资料（缓存或 API）
    const senderIds = [...new Set(rows.map(row => row.senderId))];
    const userInfoMap = new Map();
    await Promise.all(
      senderIds.map(async (id) => {
        const info = await this.getUserInfo(id);
        userInfoMap.set(id, info);
      })
    );

    // 逐条发送历史消息（带上用户资料）
    for (const row of rows) {
      const info = userInfoMap.get(row.senderId) || {};
      const msg = {
        type: 'history',
        senderId: row.senderId,
        content: row.content,
        timestamp: row.timestamp,
        name: info.name || row.senderId,
        avatarUrl: info.avatarUrl || '',
      };
      ws.send(JSON.stringify(msg));
    }
  }

  /**
   * 广播消息给所有 WebSocket 连接
   */
  broadcast(message) {
    const webSockets = this.ctx.getWebSockets();
    for (const ws of webSockets) {
      try {
        ws.send(message);
      } catch (err) {
        // 忽略已关闭的连接
      }
    }
  }

  /**
   * 清理旧消息，保留最新的 1000 条
   */
  cleanupOldMessages() {
    const sql = this.ctx.storage.sql;
    const countRow = sql.exec('SELECT COUNT(*) as cnt FROM messages').one();
    const count = countRow.cnt;

    if (count > 1000) {
      const toDelete = count - 1000;
      sql.exec(`
        DELETE FROM messages
        WHERE id IN (
          SELECT id FROM messages
          ORDER BY timestamp ASC
          LIMIT ?
        )
      `, toDelete);
    }
  }

  /**
   * 获取用户信息（从缓存或 API）
   * 缓存有效期 60 秒，过期后重新请求 /list-users 接口
   */
  async getUserInfo(userId) {
    // 检查缓存是否过期（60 秒）
    const now = Date.now();
    if (now - this.userCache.timestamp > 60000) {
      await this.refreshUserCache();
    }

    const user = this.userCache.users.get(userId);
    if (user) {
      return user;
    }
    // 未找到的用户返回默认值
    return { name: userId, avatarUrl: '' };
  }

  /**
   * 刷新用户缓存：调用外部 API 获取所有用户信息
   */
  async refreshUserCache() {
    try {
      const apiUrl = this.env.USER_API_URL; // 例如 https://chatlogin.qixibest.workers.dev/list-users
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }
      const data = await response.json();
      if (!data.success || !Array.isArray(data.users)) {
        throw new Error('Invalid API response format');
      }

      // 转换为 Map：userId -> { name, avatarUrl }
      const newMap = new Map();
      for (const user of data.users) {
        // 使用 name 作为显示名，若不存在则使用 id
        const name = user.name || user.id;
        const avatarUrl = user.avatarUrl || '';
        newMap.set(user.id, { name, avatarUrl });
      }

      this.userCache = {
        timestamp: Date.now(),
        users: newMap,
      };
    } catch (err) {
      console.error('Failed to refresh user cache:', err);
      // 缓存刷新失败：保留旧缓存，但更新时间戳防止频繁失败重试
      this.userCache.timestamp = Date.now();
    }
  }
}
