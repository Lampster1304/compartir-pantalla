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

// Cada espectador confirma cada cuadro que le llega. Si uno lleva más de esto sin confirmarse
// (su internet no da abasto), se le saltan cuadros hasta que se ponga al día y se retoma con
// un cuadro completo: así nadie acumula segundos de retraso, esté donde esté el atasco
// (en el servidor, en Cloudflare o en su wifi).
const ATRASO_MAX = 1500;
// Red de seguridad (y regla para clientes viejos que no confirman): bytes en cola sin salir.
const COLA_MAX = 1_000_000;
// Lo transmitido desde el último cuadro completo se guarda para que quien entra vea al instante.
// Si pesa más que esto, mejor pedir un cuadro completo nuevo que mandarle todo de golpe.
const PONER_AL_DIA_MAX = 1_000_000;
const GUARDAR_MAX = 8_000_000;
// ms mínimos entre pedidos de cuadro completo al presentador (le llegan a todos: que nadie inunde la sala)
const PEDIR_CLAVE_CADA = 3000;
const AVISAR_CONGESTION_CADA = 2000;
// Tope de lo que el servidor manda en total (todas las copias juntas). Saturar la salida no da más
// calidad: llena la cola del módem y TODO lo que usa esa línea se atrasa segundos. Con este tope,
// al presentador se le pide como mucho SALIDA_MAX_MBPS ÷ espectadores (y nunca más de 4 Mbps).
const SALIDA_MAX_BPS = (Number(process.env.SALIDA_MAX_MBPS) || 20) * 1e6;

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
// persona: { ws, nombre, esperaClave, atascadoEn, usaAck, cola: [{ t, en }] (enviados sin confirmar) }
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

// Le dice al presentador cuánto puede mandar como máximo según cuántos están mirando.
function avisarLimite(sala) {
  if (!sala.presentador) return;
  const espectadores = Math.max(1, sala.gente.size - 1);
  enviar(sala.gente.get(sala.presentador)?.ws, { type: 'limite', bps: Math.round(SALIDA_MAX_BPS / espectadores) });
}

function olvidarVideo(sala) {
  sala.config = null;
  sala.guardado = [];
  sala.guardadoBytes = 0;
  for (const p of sala.gente.values()) { p.esperaClave = true; p.cola = []; }
}

function atrasado(p, ahora) {
  if (p.ws.bufferedAmount > COLA_MAX) return true;
  return p.usaAck && p.cola.length > 0 && ahora - p.cola[0].en > ATRASO_MAX;
}
function alDia(p) {
  return p.usaAck ? p.cola.length === 0 && p.ws.bufferedAmount < COLA_MAX / 2 : p.ws.bufferedAmount < COLA_MAX / 2;
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

// Se puso al día después de un atasco: le reenvío SOLO A ÉL lo último desde el último cuadro
// completo (barato para la sala). Si eso pesa mucho, pido un cuadro completo nuevo (le llega a todos).
function reanudar(sala, p) {
  if (sala.guardado.length && sala.guardadoBytes <= PONER_AL_DIA_MAX) {
    const ahora = Date.now();
    for (const b of sala.guardado) {
      p.ws.send(b);
      if (p.usaAck) p.cola.push({ t: b.readDoubleBE(9), en: ahora });
    }
    p.esperaClave = false;
  } else {
    pedirClave(sala);
  }
}

// Un cuadro del presentador. Cabecera: byte 0 = 1 si es cuadro completo (clave);
// bytes 9–16 = hora de envío del presentador, que sirve de identificador del cuadro.
function repartir(sala, de, buf) {
  const clave = (buf[0] & 1) === 1;
  const t = buf.readDoubleBE(9);
  // Le confirmo al presentador que llegó (así mide si su subida da abasto).
  enviar(sala.gente.get(de)?.ws, { type: 'ack-sub', t });
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
    if (p.esperaClave) {
      // Espera un cuadro completo, y solo se lo mando cuando ya recibió todo lo anterior.
      if (!clave || !alDia(p)) continue;
    } else if (atrasado(p, ahora)) {
      // Su internet no da abasto: le salto cuadros (sin frenar a los demás).
      p.esperaClave = true;
      p.atascadoEn = ahora;
      if (!p.usaAck) pedirClave(sala); // con confirmaciones, lo pide cuando se pone al día
      continue;
    }
    p.ws.send(buf);
    p.esperaClave = false;
    if (p.usaAck) {
      p.cola.push({ t, en: ahora });
      if (p.cola.length > 900) p.cola.shift();
    }
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
      yo = { ws, nombre, esperaClave: true, atascadoEn: 0, usaAck: false, cola: [] };
      sala.gente.set(id, yo);
      aTodos(sala, { type: 'peer-joined', id, name: nombre }, id);
      if (sala.presentador) { ponerAlDia(sala, yo); avisarLimite(sala); }
      return;
    }
    if (!sala) return;

    switch (msg.type) {
      case 'start-present':
        // Si otro estaba presentando, se le quita el turno.
        sala.presentador = id;
        olvidarVideo(sala);
        aTodos(sala, { type: 'presenter', id });
        avisarLimite(sala);
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
      case 'ack': {
        // El espectador confirma que le llegó el cuadro t (y todos los anteriores).
        const t = Number(msg.t);
        if (!Number.isFinite(t)) break;
        yo.usaAck = true;
        let i = 0;
        while (i < yo.cola.length && yo.cola[i].t <= t) i++;
        if (i) yo.cola.splice(0, i);
        if (yo.esperaClave && yo.cola.length === 0 && sala.config && sala.presentador && sala.presentador !== id) reanudar(sala, yo);
        break;
      }
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
    avisarLimite(sala);
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
