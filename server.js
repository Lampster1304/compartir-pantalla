// Servidor de señalización + archivos estáticos.
// El video NO pasa por aquí: viaja directo entre navegadores (WebRTC).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3077;
const PUBLIC = path.join(__dirname, 'public');
const VERSION = Date.now().toString(36);
const TIPOS = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };

// Servidores ICE que se le pasan al navegador. TURN es opcional (redes muy cerradas).
const ICE = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
if (process.env.TURN_URL) {
  ICE.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/config') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers: ICE }));
  }
  let archivo = path.normalize(path.join(PUBLIC, url.pathname === '/' ? 'index.html' : url.pathname));
  if (!archivo.startsWith(PUBLIC) || !fs.existsSync(archivo) || fs.statSync(archivo).isDirectory()) {
    archivo = path.join(PUBLIC, 'index.html');
  }
  if (archivo.endsWith('index.html')) {
    // Cloudflare guarda .js/.css por horas: cada despliegue les pone una versión nueva en la URL.
    res.writeHead(200, { 'Content-Type': TIPOS['.html'], 'Cache-Control': 'no-cache' });
    return res.end(fs.readFileSync(archivo, 'utf8').replace(/\/(app\.js|estilos\.css)"/g, `/$1?v=${VERSION}"`));
  }
  res.writeHead(200, { 'Content-Type': TIPOS[path.extname(archivo)] || 'application/octet-stream' });
  fs.createReadStream(archivo).pipe(res);
});

// salas: nombre -> { gente: Map(id -> {ws, nombre}), presentador: id|null }
const salas = new Map();

function enviar(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}
function aTodos(sala, msg, excepto) {
  for (const [id, p] of sala.gente) if (id !== excepto) enviar(p.ws, msg);
}

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const id = crypto.randomUUID().slice(0, 8);
  let sala = null;
  let nombreSala = null;

  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join' && !sala) {
      nombreSala = String(msg.room || '').slice(0, 64).toLowerCase();
      const nombre = String(msg.name || 'Invitado').slice(0, 40);
      if (!nombreSala) return;
      if (!salas.has(nombreSala)) salas.set(nombreSala, { gente: new Map(), presentador: null });
      sala = salas.get(nombreSala);
      enviar(ws, {
        type: 'welcome', id,
        peers: [...sala.gente].map(([pid, p]) => ({ id: pid, name: p.nombre })),
        presenter: sala.presentador,
      });
      sala.gente.set(id, { ws, nombre });
      aTodos(sala, { type: 'peer-joined', id, name: nombre }, id);
      return;
    }
    if (!sala) return;

    switch (msg.type) {
      case 'start-present':
        // Si otro estaba presentando, se le quita el turno.
        sala.presentador = id;
        aTodos(sala, { type: 'presenter', id });
        break;
      case 'stop-present':
        if (sala.presentador === id) {
          sala.presentador = null;
          aTodos(sala, { type: 'presenter', id: null });
        }
        break;
      case 'signal': {
        const destino = sala.gente.get(msg.to);
        if (destino) enviar(destino.ws, { type: 'signal', from: id, data: msg.data });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!sala) return;
    sala.gente.delete(id);
    aTodos(sala, { type: 'peer-left', id });
    if (sala.presentador === id) {
      sala.presentador = null;
      aTodos(sala, { type: 'presenter', id: null });
    }
    if (sala.gente.size === 0) salas.delete(nombreSala);
  });
});

// Limpia conexiones muertas (laptops que se duermen, wifi que se cae).
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Listo en http://localhost:${PORT}`));
