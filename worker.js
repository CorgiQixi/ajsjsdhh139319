export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const sessionId = url.searchParams.get('sessionId');
      const userId = url.searchParams.get('userId');
      if (!sessionId || !userId) {
        return new Response('Missing sessionId or userId', { status: 400 });
      }

      const id = env.CHAT_SESSION.idFromName(sessionId);
      const stub = env.CHAT_SESSION.get(id);
      return stub.fetch(request);
    } catch (err) {
      return new Response(`Entry Error: ${err.message}\n${err.stack}`, { status: 500 });
    }
  }
};

export class ChatSession {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;

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

    this.userCache = {
      timestamp: 0,
      users: new Map(),
    };
  }

  async fetch(request) {
    const url = new URL(request.url);

    // 调试端点：返回房间统计信息
    if (url.pathname === '/stats') {
      try {
        const sql = this.ctx.storage.sql;
        const count = sql.exec('SELECT COUNT(*) as cnt FROM messages').one().cnt;
        const connections = this.ctx.getWebSockets().length;
        return new Response(JSON.stringify({ messageCount: count, connections }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (err) {
        return new Response(`Stats error: ${err.message}`, { status: 500 });
      }
    }

    // 处理 WebSocket 升级
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const userId = url.searchParams.get('userId');
    const lastTs = parseInt(url.searchParams.get('lastTs')) || 0;

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // 接受 WebSocket，并存储 userId 到 attachment
    this.ctx.acceptWebSocket(server, [userId]); // tags 可用于 getWebSockets(tag)
    server.serializeAttachment(userId);          // 关键修复：存储 userId

    // 补发历史消息
    if (lastTs > 0) {
      await this.sendHistoricalMessages(server, userId, lastTs);
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const userId = ws.deserializeAttachment(); // 直接获取 userId 字符串
    try {
      const data = JSON.parse(message);
      const content = data.content;
      if (!content || typeof content !== 'string') {
        ws.send(JSON.stringify({ error: 'Invalid message format' }));
        return;
      }

      const timestamp = Date.now();
      const sql = this.ctx.storage.sql;

      sql.exec(
        'INSERT INTO messages (senderId, content, timestamp) VALUES (?, ?, ?)',
        userId,
        content,
        timestamp
      );

      this.cleanupOldMessages();

      const userInfo = await this.getUserInfo(userId);

      const broadcastMsg = {
        type: 'message',
        senderId: userId,
        content,
        timestamp,
        name: userInfo.name,
        avatarUrl: userInfo.avatarUrl,
      };

      this.broadcast(JSON.stringify(broadcastMsg));
    } catch (err) {
      console.error('webSocketMessage error:', err);
      ws.send(JSON.stringify({ error: 'Internal server error' }));
    }
  }

  webSocketClose(ws, code, reason, wasClean) {
    const userId = ws.deserializeAttachment();
    // 可选：记录用户离开等
  }

  async sendHistoricalMessages(ws, userId, lastTs) {
    try {
      const sql = this.ctx.storage.sql;
      const rows = sql.exec(
        'SELECT senderId, content, timestamp FROM messages WHERE timestamp > ? ORDER BY timestamp ASC',
        lastTs
      ).toArray();

      if (!Array.isArray(rows) || rows.length === 0) return;

      const senderIds = [...new Set(rows.map(row => row.senderId))];
      const userInfoMap = new Map();
      await Promise.all(
        senderIds.map(async (id) => {
          const info = await this.getUserInfo(id);
          userInfoMap.set(id, info);
        })
      );

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
    } catch (err) {
      console.error('sendHistoricalMessages error:', err);
    }
  }

  broadcast(message) {
    const webSockets = this.ctx.getWebSockets();
    for (const ws of webSockets) {
      try {
        ws.send(message);
      } catch (err) {
        // 忽略发送失败的连接
      }
    }
  }

  cleanupOldMessages() {
    try {
      const sql = this.ctx.storage.sql;
      const countRow = sql.exec('SELECT COUNT(*) as cnt FROM messages').one();
      const count = countRow.cnt;
      if (count > 1000) {
        // 删除最旧的超出部分：获取第1000条之后的第一条消息的ID
        const row = sql.exec(
          'SELECT id FROM messages ORDER BY timestamp DESC LIMIT 1 OFFSET 999'
        ).one();
        if (row) {
          sql.exec('DELETE FROM messages WHERE id <= ?', row.id);
        }
      }
    } catch (err) {
      console.error('cleanupOldMessages error:', err);
    }
  }

  async getUserInfo(userId) {
    const now = Date.now();
    if (now - this.userCache.timestamp > 60000) {
      await this.refreshUserCache();
    }
    const user = this.userCache.users.get(userId);
    if (user) {
      return user;
    }
    return { name: userId, avatarUrl: '' };
  }

  async refreshUserCache() {
    try {
      const apiUrl = this.env.USER_API_URL;
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }
      const data = await response.json();
      if (!data || !data.success || !Array.isArray(data.users)) {
        throw new Error('Invalid API response format');
      }
      const newMap = new Map();
      for (const user of data.users) {
        const name = user.realName || user.name || user.id;
        const avatarUrl = user.avatarUrl || '';
        newMap.set(user.id, { name, avatarUrl });
      }
      this.userCache = {
        timestamp: Date.now(),
        users: newMap,
      };
    } catch (err) {
      console.error('refreshUserCache error:', err);
      this.userCache.timestamp = Date.now(); // 防止频繁重试
    }
  }
}
