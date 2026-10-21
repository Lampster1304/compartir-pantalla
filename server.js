// Servidor: junta a la gente en salas y reparte la pantalla del presentador.
// El presentador sube UN solo video ya codificado y el servidor le manda una copia
// a cada espectador (sin recodificar), así 20 personas cuestan lo mismo que 1 al presentador.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3077;
const PUBLIC = path.join(__dirname, 'public');
const VERSION = Date.now().toString(36);
const TIPOS = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };

// Si a un espectador se le acumula más que esto sin salir (su internet no da abasto),
// se le saltan cuadros hasta el próximo cuadro completo en vez de acumularle retraso.
const COLA_MAX = 1_000_000;
// Lo transmitido desde el último cuadro completo se guarda para que quien entra vea al instante.
// Si pesa más que esto, mejor pedir un cuadro completo nuevo que mandarle todo de golpe.
const PONER_AL_DIA_MAX = 600_000;
const GUARDAR_MAX = 8_000_000;
const PEDIR_CLAVE_CADA = 2000;   // ms mínimos entre pedidos de cuadro completo al presentador
const AVISAR_CONGESTION_CADA = 2000;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
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

// salas: nombre -> sala
// sala:    { gente: Map(id -> persona), presentador, config, guardado: Buffer[], guardadoBytes, ultimoPedido, ultimoAviso }
// persona: { ws, nombre, esperaClave, atascadoEn }
const salas = new Map();

function enviar(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}
function aTodos(sala, msg, excepto) {
  for (const [id, p] of sala.gente) if (id !== excepto) enviar(p.ws, msg);
}

function pedirClave(sala) {
  const ahora = Date.now();
  if (!sala.presentador || ahora - sala.ultimoPedido < PEDIR_CLAVE_CADA) return;
  sala.ultimoPedido = ahora;
  enviar(sala.gente.get(sala.presentador)?.ws, { type: 'keyframe' });
}

function olvidarVideo(sala) {
  sala.config = null;
  sala.guardado = [];
  sala.guardadoBytes = 0;
  for (const p of sala.gente.values()) p.esperaClave = true;
}

// Pone al día a quien acaba de entrar: formato del video + lo último desde el último cuadro completo.
function ponerAlDia(sala, p) {
  if (!sala.config) return;
  enviar(p.ws, { type: 'video-config', config: sala.config });
  if (sala.guardado.length && sala.guardadoBytes <= PONER_AL_DIA_MAX) {
    for (const b of sala.guardado) p.ws.send(b);
    p.esperaClave = false;
  } else {
    p.esperaClave = true;
    pedirClave(sala);
  }
}

// Un cuadro del presentador: byte 0 = 1 si es cuadro completo (clave). El resto no se toca.
function repartir(sala, de, buf) {
  const clave = (buf[0] & 1) === 1;
  if (clave) {
    sala.guardado = [buf];
    sala.guardadoBytes = buf.length;
  } else if (sala.guardado.length && sala.guardadoBytes + buf.length <= GUARDAR_MAX) {
    sala.guardado.push(buf);
    sala.guardadoBytes += buf.length;
  } else {
    sala.guardado = [];
    sala.guardadoBytes = 0;
  }

  const ahora = Date.now();
  let espectadores = 0;
  let atascados = 0;
  for (const [id, p] of sala.gente) {
    if (id === de) continue;
    espectadores++;
    if (ahora - p.atascadoEn < 3000) atascados++;
    if (p.esperaClave && !clave) continue;
    if (p.ws.bufferedAmount > COLA_MAX) {
      // Su internet no da abasto: le salto cuadros (sin frenar a los demás).
      p.esperaClave = true;
      p.atascadoEn = ahora;
      pedirClave(sala);
      continue;
    }
    p.ws.send(buf);
    p.esperaClave = false;
  }

  // Si se atasca mucha gente a la vez, el problema es la salida del servidor:
  // le pido al presentador que baje la calidad para que todos lo vean fluido.
  if (atascados >= Math.max(2, Math.ceil(espectadores * 0.25)) && ahora - sala.ultimoAviso > AVISAR_CONGESTION_CADA) {
    sala.ultimoAviso = ahora;
    enviar(sala.gente.get(de)?.ws, { type: 'congestion' });
  }
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 * 1024 });

wss.on('connection', (ws) => {
  const id = crypto.randomUUID().slice(0, 8);
  let sala = null;
  let nombreSala = null;
  let yo = null;

  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw, esBinario) => {
    if (esBinario) {
      if (sala && sala.presentador === id && raw.length > 17) repartir(sala, id, raw);
      return;
    }
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'join' && !sala) {
      nombreSala = String(msg.room || '').slice(0, 64).toLowerCase();
      const nombre = String(msg.name || 'Invitado').slice(0, 40);
      if (!nombreSala) return;
      if (!salas.has(nombreSala)) {
        salas.set(nombreSala, { gente: new Map(), presentador: null, config: null, guardado: [], guardadoBytes: 0, ultimoPedido: 0, ultimoAviso: 0 });
      }
      sala = salas.get(nombreSala);
      enviar(ws, {
        type: 'welcome', id,
        peers: [...sala.gente].map(([pid, p]) => ({ id: pid, name: p.nombre })),
        presenter: sala.presentador,
      });
      yo = { ws, nombre, esperaClave: true, atascadoEn: 0 };
      sala.gente.set(id, yo);
      aTodos(sala, { type: 'peer-joined', id, name: nombre }, id);
      if (sala.presentador) ponerAlDia(sala, yo);
      return;
    }
    if (!sala) return;

    switch (msg.type) {
      case 'start-present':
        // Si otro estaba presentando, se le quita el turno.
        sala.presentador = id;
        olvidarVideo(sala);
        aTodos(sala, { type: 'presenter', id });
        break;
      case 'stop-present':
        if (sala.presentador === id) {
          sala.presentador = null;
          olvidarVideo(sala);
          aTodos(sala, { type: 'presenter', id: null });
        }
        break;
      case 'video-config':
        // Formato nuevo (o tamaño nuevo): todos esperan el próximo cuadro completo.
        if (sala.presentador !== id || !msg.config) break;
        olvidarVideo(sala);
        sala.config = msg.config;
        aTodos(sala, { type: 'video-config', config: msg.config }, id);
        break;
      case 'need-key':
        yo.esperaClave = true;
        pedirClave(sala);
        break;
    }
  });

  ws.on('close', () => {
    if (!sala) return;
    sala.gente.delete(id);
    aTodos(sala, { type: 'peer-left', id });
    if (sala.presentador === id) {
      sala.presentador = null;
      olvidarVideo(sala);
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
