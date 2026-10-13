const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

// === Каналы: { name: { password, messages: [], members: Set } } ===
const channels = new Map();

const server = http.createServer((req, res) => {
  const file = req.url === '/' ? '/index.html' : req.url;
  const fp = path.join(__dirname, 'public', file);
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); return res.end('404'); }
    const ext = path.extname(fp);
    const mime = { '.html':'text/html; charset=utf-8', '.js':'text/javascript', '.css':'text/css' }[ext] || 'text/plain';
    res.writeHead(200, { 'Content-Type': mime });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });

const genId = () => Math.random().toString(36).slice(2, 10);
const log = (...a) => console.log(`\x1b[32m[${new Date().toLocaleTimeString('ru-RU')}]\x1b[0m`, ...a);

wss.on('connection', (ws) => {
  ws.id = genId();
  ws.channel = null;
  ws.nick = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // === СОЗДАТЬ КАНАЛ ===
    if (msg.type === 'create') {
      const name = String(msg.name || '').trim().toLowerCase();
      const pass = String(msg.password || '').trim();
      if (!name || !pass) return send(ws, { type: 'error', text: 'Укажи название и пароль' });
      if (channels.has(name)) return send(ws, { type: 'error', text: 'Канал уже существует' });

      channels.set(name, { password: pass, messages: [], members: new Set() });
      log(`Канал создан: "${name}"`);
      // сразу подключаем создателя
      joinChannel(ws, name, pass, msg.nick);
      return;
    }

    // === ПРИСОЕДИНИТЬСЯ ===
    if (msg.type === 'join') {
      const name = String(msg.name || '').trim().toLowerCase();
      const pass = String(msg.password || '').trim();
      const ch = channels.get(name);
      if (!ch) return send(ws, { type: 'error', text: 'Канал не найден' });
      if (ch.password !== pass) return send(ws, { type: 'error', text: 'Неверный пароль' });
      joinChannel(ws, name, pass, msg.nick);
      return;
    }

    // === СООБЩЕНИЕ (релей всем в канале) ===
    if (msg.type === 'msg' && ws.channel) {
      const ch = channels.get(ws.channel);
      if (!ch) return;
      const packet = {
        type: 'msg',
        id: genId(),
        nick: ws.nick,
        time: Date.now(),
        data: msg.data   // зашифрованный blob
      };
      ch.messages.push(packet);
      // не даём истории пухнуть бесконечно
      if (ch.messages.length > 200) ch.messages.shift();

      for (const client of ch.members) {
        send(client, packet);
      }
      return;
    }

    // === ИСТОРИЯ ===
    if (msg.type === 'history' && ws.channel) {
      const ch = channels.get(ws.channel);
      if (!ch) return;
      send(ws, { type: 'history', messages: ch.messages });
      return;
    }
  });

  ws.on('close', () => {
    if (ws.channel && channels.has(ws.channel)) {
      const ch = channels.get(ws.channel);
      ch.members.delete(ws);
      broadcast(ws.channel, { type: 'peer-left', nick: ws.nick, count: ch.members.size }, ws);
      log(`${ws.nick || ws.id} покинул "${ws.channel}"`);
    }
  });
});

function joinChannel(ws, name, pass, nick) {
  const ch = channels.get(name);
  if (!ch) return;
  ws.channel = name;
  ws.nick = String(nick || 'anon').slice(0, 20);
  ch.members.add(ws);

  send(ws, { type: 'joined', name, nick: ws.nick, count: ch.members.size });
  broadcast(name, { type: 'peer-joined', nick: ws.nick, count: ch.members.size }, ws);
  log(`+ ${ws.nick} → "${name}" (${ch.members.size})`);
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(channelName, obj, except) {
  const ch = channels.get(channelName);
  if (!ch) return;
  const raw = JSON.stringify(obj);
  for (const c of ch.members) {
    if (c !== except && c.readyState === WebSocket.OPEN) c.send(raw);
  }
}

server.listen(PORT, () => {
  log(`Huksixis server на порту ${PORT}`);
  console.log('\x1b[32m%s\x1b[0m', '  ▓▒░ HUKSIXIS RELAY ONLINE ░▒▓');
});
