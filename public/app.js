// Cliente. El presentador codifica su pantalla UNA sola vez (WebCodecs) y la sube al
// servidor por el WebSocket; el servidor la reparte a todos. Los espectadores la
// decodifican y la dibujan en un <canvas>.
const $ = (s) => document.querySelector(s);

let ws = null;
let yo = null;            // mi id
let miNombre = '';
let sala = '';
let presentador = null;   // id de quien presenta (o null)
const gente = new Map();  // id -> nombre
let miPantalla = null;    // MediaStream si estoy presentando
let emisor = null;        // codifica y sube mi pantalla
let receptor = null;      // decodifica y dibuja la pantalla de otro
let saliendo = false;
// Para diagnosticar el retraso por tramos (ms): codificar (presentador), red (envío → llegada; solo
// exacto si ambos relojes coinciden), decodificar y esperar a dibujarse.
const stats = { cuadros: 0, decodificados: 0, ancho: 0, alto: 0, retrasoMs: 0, codificarMs: 0, redMs: 0, decodificarMs: 0, dibujarMs: 0 };

// Calidad de la transmisión. El emisor la baja sola si su internet (o el servidor) no da abasto.
const MAX_ANCHO = 1920, MAX_ALTO = 1080;
const BITRATE_INICIAL = 2_000_000, BITRATE_MIN = 300_000, BITRATE_MAX = 4_000_000;
// Cuadro completo de seguridad muy de vez en cuando: pesa ~200 KB y con 20 personas son ~4 MB de golpe
// (un parón de ~1 s para todos). Quien lo necesita (entra tarde, se recupera) lo pide al momento.
const CLAVE_CADA = 10 * 60_000;
const COLA_SUBIDA_MAX = 600_000;    // bytes sin salir antes de saltar cuadros
const ATRASO_SUBIDA_MAX = 700;      // ms que puede tardar el servidor en confirmar un cuadro
// H264 solo si la tarjeta de video lo codifica (Mac, casi todas las Windows): rápido y respeta el bitrate.
// Si no, VP8 por software, que rinde más que el H264 por software. Lo demás, último recurso.
const H264 = 'avc1.64002A';
const CODECS_RESPALDO = ['vp8', 'avc1.4D402A', 'avc1.42E02A', 'vp09.00.40.08'];
const CABECERA = 17;                // 1 byte tipo + 8 marca de tiempo + 8 hora de envío

// ---------- Entrada ----------
const params = new URLSearchParams(location.search);
$('#in-sala').value = params.get('sala') || 'teco2021';
try { $('#in-nombre').value = localStorage.getItem('nombre') || ''; } catch {}
if (params.get('sala')) $('#in-nombre').focus();

$('#form-entrar').addEventListener('submit', (e) => {
  e.preventDefault();
  miNombre = $('#in-nombre').value.trim() || 'Invitado';
  sala = $('#in-sala').value.trim().toLowerCase();
  try { localStorage.setItem('nombre', miNombre); } catch {}
  history.replaceState(null, '', '?sala=' + encodeURIComponent(sala));
  $('#lobby').hidden = true;
  $('#sala').hidden = false;
  $('#txt-sala').textContent = sala;
  conectar();
});

// ---------- Conexión con el servidor ----------
function conectar() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => ws.send(JSON.stringify({ type: 'join', room: sala, name: miNombre }));
  ws.onmessage = (e) => {
    if (typeof e.data === 'string') return alMensaje(JSON.parse(e.data));
    if (!local) alCuadro(e.data); // con el canal local abierto, lo que llegue por aquí es viejo
  };
  ws.onclose = () => {
    $('#estado-punto').className = 'punto mal';
    cerrarCanalLocal();
    if (saliendo) return;
    detenerReceptor();
    aviso('Se perdió la conexión, reconectando…');
    setTimeout(conectar, 2000);
  };
}

function alCuadro(buf) {
  // Confirmo que me llegó: así el servidor sabe si mi conexión da abasto y no me acumula retraso.
  mandar({ type: 'ack', t: new DataView(buf).getFloat64(9) });
  receptor?.cuadro(buf);
}

function enLinea() {
  return ws && ws.readyState === WebSocket.OPEN;
}
// Todo va por el canal de la red local si está abierto (así llega en orden por un solo camino).
function mandar(msg) {
  if (local) { try { return local.send(JSON.stringify(msg)); } catch {} }
  if (enLinea()) ws.send(JSON.stringify(msg));
}
function mandarCuadro(buf) {
  if (local) { try { return mandarTrozos(local, buf); } catch {} }
  ws.send(buf);
}
function colaDeSubida() {
  return local ? local.bufferedAmount : ws.bufferedAmount;
}

// ---------- Canal directo por la red de la oficina (WebRTC) ----------
// La página entra por internet (https), pero los cuadros viajan del navegador al servidor por la red
// local, sin ir a Miami y volver. Si no se puede abrir (fuera de la oficina), sigue todo por el WebSocket.
let local = null;   // RTCDataChannel abierto
let rtc = null;     // { pc, dc }
const TROZO = 64 * 1024;

async function abrirCanalLocal() {
  cerrarCanalLocal();
  if (!('RTCPeerConnection' in window)) return;
  const pc = new RTCPeerConnection({ iceServers: [] });
  const dc = pc.createDataChannel('cuadros');
  dc.binaryType = 'arraybuffer';
  const este = { pc, dc };
  rtc = este;
  let partes = null, falta = 0;
  dc.onopen = () => {
    if (rtc !== este) return;
    local = dc;
    emisor?.pedirClave(); // al cambiar de camino, arranco desde un cuadro completo
    pintar();
  };
  dc.onclose = () => {
    if (rtc !== este) return;
    local = null;
    emisor?.pedirClave();
    pintar();
  };
  dc.onmessage = (e) => {
    if (typeof e.data === 'string') return alMensaje(JSON.parse(e.data));
    const b = new Uint8Array(e.data);
    if (b[0] === 1) {
      const total = new DataView(e.data).getUint32(1);
      partes = { buf: new Uint8Array(total), en: 0 };
      partes.buf.set(b.subarray(5), 0);
      partes.en = b.length - 5;
    } else if (partes) {
      partes.buf.set(b.subarray(1), partes.en);
      partes.en += b.length - 1;
    }
    if (partes && partes.en >= partes.buf.length) {
      const listo = partes.buf.buffer;
      partes = null;
      alCuadro(listo);
    }
  };
  pc.onicecandidate = (e) => {
    if (e.candidate && rtc === este) enviarWs({ type: 'rtc-cand', candidate: e.candidate.candidate, mid: e.candidate.sdpMid });
  };
  pc.onconnectionstatechange = () => {
    if (rtc === este && (pc.connectionState === 'failed' || pc.connectionState === 'closed')) cerrarCanalLocal();
  };
  await pc.setLocalDescription(await pc.createOffer());
  enviarWs({ type: 'rtc-offer', sdp: pc.localDescription.sdp });
}

function cerrarCanalLocal() {
  const r = rtc;
  rtc = null;
  local = null;
  if (r) { try { r.dc.close(); r.pc.close(); } catch {} }
}

function enviarWs(msg) {
  if (enLinea()) ws.send(JSON.stringify(msg));
}

function mandarTrozos(dc, buf) {
  const datos = new Uint8Array(buf);
  for (let i = 0; i < datos.length; i += TROZO) {
    const parte = datos.subarray(i, i + TROZO);
    const cab = i === 0 ? 5 : 1;
    const m = new Uint8Array(cab + parte.length);
    m[0] = i === 0 ? 1 : 0;
    if (i === 0) new DataView(m.buffer).setUint32(1, datos.length);
    m.set(parte, cab);
    dc.send(m);
  }
}

function alMensaje(m) {
  switch (m.type) {
    case 'welcome':
      yo = m.id;
      gente.clear();
      gente.set(yo, miNombre);
      for (const p of m.peers) gente.set(p.id, p.name);
      presentador = m.presenter;
      $('#estado-punto').className = 'punto ok';
      if (m.rtc) abrirCanalLocal().catch(() => {});
      if (miPantalla) mandar({ type: 'start-present' }); // se cayó la conexión mientras presentaba: retomo
      else if (presentador) receptor = new Receptor();
      pintar();
      break;

    case 'peer-joined':
      gente.set(m.id, m.name);
      aviso(`${m.name} entró`);
      pintar();
      break;

    case 'peer-left':
      if (gente.has(m.id)) aviso(`${gente.get(m.id)} salió`);
      gente.delete(m.id);
      pintar();
      break;

    case 'presenter':
      presentador = m.id;
      detenerReceptor();
      if (m.id === yo) {
        emisor?.empezar();
      } else {
        if (miPantalla) {
          // Alguien más tomó el turno.
          soltarPantalla();
          aviso(`${gente.get(m.id) || 'Otra persona'} empezó a presentar`);
        }
        if (m.id) receptor = new Receptor();
      }
      pintar();
      break;

    case 'video-config':
      if (presentador !== yo) (receptor ||= new Receptor()).configurar(m.config);
      break;

    case 'keyframe':
      emisor?.pedirClave();
      break;

    case 'congestion':
      emisor?.atasco();
      break;

    case 'rtc-answer':
      rtc?.pc.setRemoteDescription({ type: 'answer', sdp: m.sdp }).catch(() => {});
      break;

    case 'rtc-cand':
      rtc?.pc.addIceCandidate({ candidate: m.candidate, sdpMid: m.mid }).catch(() => {});
      break;

    case 'camino':
      break;

    case 'ack-sub':
      emisor?.confirmado(m.t);
      break;

    case 'limite':
      emisor?.ponerLimite(m.bps);
      break;
  }
}

// ---------- Presentar: capturar, codificar una vez, subir ----------
class Emisor {
  constructor(stream) {
    this.track = stream.getVideoTracks()[0];
    this.objetivo = BITRATE_INICIAL; // lo que queremos que salga de verdad
    this.bitrate = BITRATE_INICIAL;  // lo que se le pide al codificador (algunos se pasan del doble)
    this.bytes = 0;                  // lo que salió desde el último ajuste
    this.encoder = null;
    this.codec = null;
    this.ancho = 0;
    this.alto = 0;
    this.activo = true;
    this.forzarClave = true;
    this.ultimaClave = 0;
    this.ultimoConfig = null;   // texto del último formato enviado, para no repetirlo
    this.configMsg = null;
    this.huboAtasco = false;
    this.ultimoAtasco = 0;
    this.fpsMax = 30;             // baja solo si el codificador no logra comprimir lo suficiente
    this.ultimoCuadro = 0;
    this.credito = 1;
    this.errores = 0;
    this.entradas = new Map();      // marca del cuadro -> cuándo entró al codificador
    this.limite = BITRATE_MAX;     // lo que el servidor deja mandar según cuántos miran
    this.sinConfirmar = [];        // horas de envío de los cuadros que el servidor aún no confirma
    this.servidorConfirma = false;
    this.ultimoAjuste = performance.now();
    this.arrancar();
  }

  async arrancar() {
    const { width = MAX_ANCHO, height = MAX_ALTO } = this.track.getSettings();
    const [w, h] = medida(width, height);
    this.codec = await elegirCodec(w, h);
    if (!this.codec) {
      aviso('Este navegador no puede codificar video. Usa Chrome, Edge o Safari actualizados.');
      return dejarDePresentar();
    }
    if (!this.activo) return;
    if ('MediaStreamTrackProcessor' in window) {
      // Chrome/Edge: cuadros directos de la captura.
      this.lector = new MediaStreamTrackProcessor({ track: this.track }).readable.getReader();
      while (this.activo) {
        const { value: cuadro, done } = await this.lector.read().catch(() => ({ done: true }));
        if (done) break;
        this.codificar(cuadro);
      }
    } else {
      // Safari/Firefox: copio cada cuadro de la vista previa (<video>).
      const paso = () => {
        if (!this.activo) return;
        if (video.videoWidth) this.codificar(new VideoFrame(video, { timestamp: performance.now() * 1000 }));
        if (video.requestVideoFrameCallback) video.requestVideoFrameCallback(paso);
        else setTimeout(paso, 1000 / 30);
      };
      paso();
    }
  }

  // Al tener el turno (o al reconectar): reenvío el formato y mando un cuadro completo.
  empezar() {
    if (this.configMsg) mandar(this.configMsg);
    this.forzarClave = true;
    this.sinConfirmar = [];
  }
  ponerLimite(bps) {
    this.limite = Math.max(BITRATE_MIN, Math.min(BITRATE_MAX, Number(bps) || BITRATE_MAX));
    if (this.objetivo > this.limite) this.objetivo = this.limite;
  }
  confirmado(t) {
    this.servidorConfirma = true;
    let i = 0;
    while (i < this.sinConfirmar.length && this.sinConfirmar[i] <= t) i++;
    if (i) this.sinConfirmar.splice(0, i);
  }
  // Mi subida va atrasada: hay un cuadro enviado hace rato que el servidor no ha confirmado
  // (puede estar atorado en mi wifi o en Cloudflare, donde bufferedAmount no lo ve).
  subidaAtrasada() {
    return this.servidorConfirma && this.sinConfirmar.length > 0 && Date.now() - this.sinConfirmar[0] > ATRASO_SUBIDA_MAX;
  }
  pedirClave() {
    this.forzarClave = true;
  }
  atasco() {
    this.huboAtasco = true;
    this.ultimoAtasco = performance.now();
  }

  codificar(cuadro) {
    try {
      if (!this.activo || !enLinea() || presentador !== yo) return;
      // Mi subida no da abasto: salto este cuadro en vez de acumular retraso.
      if (colaDeSubida() > COLA_SUBIDA_MAX || this.subidaAtrasada()) return this.atasco();
      // Mi máquina no da abasto: igual.
      if (this.encoder && this.encoder.encodeQueueSize > 2) return;
      // Tope de fps por créditos: con una fuente de 30 fps permite 17, 23… y no solo 30, 15, 10.
      const ahora = performance.now();
      this.credito = Math.min(1.5, this.credito + ((ahora - this.ultimoCuadro) * this.fpsMax) / 1000);
      this.ultimoCuadro = ahora;
      if (this.credito < 0.95) return;
      this.credito -= 1;

      const [w, h] = medida(cuadro.displayWidth, cuadro.displayHeight);
      if (!this.encoder || this.encoder.state === 'closed' || w !== this.ancho || h !== this.alto) this.configurar(w, h);
      this.ajustarCalidad();

      const clave = this.forzarClave || ahora - this.ultimaClave > CLAVE_CADA;
      if (clave) { this.forzarClave = false; this.ultimaClave = ahora; }
      // Si el cuadro es más grande que el tope, el codificador lo escala a ancho×alto.
      this.entradas.set(cuadro.timestamp, performance.now());
      if (this.entradas.size > 60) this.entradas.delete(this.entradas.keys().next().value);
      this.encoder.encode(cuadro, { keyFrame: clave });
    } finally {
      cuadro.close();
    }
  }

  configurar(w, h) {
    this.ancho = w;
    this.alto = h;
    if (!this.encoder || this.encoder.state === 'closed') {
      this.encoder = new VideoEncoder({
        output: (chunk, meta) => this.salida(chunk, meta),
        error: (e) => {
          console.warn('codificador:', e);
          this.forzarClave = true;
          // Si falla una y otra vez (p. ej. la tarjeta de video), sigo con VP8 por software.
          if (++this.errores >= 3 && this.codec !== 'vp8') { this.codec = 'vp8'; this.errores = 0; }
        },
      });
    }
    this.encoder.configure(this.configuracion());
    this.forzarClave = true;
  }

  configuracion() {
    const c = {
      codec: this.codec, width: this.ancho, height: this.alto,
      bitrate: this.bitrate, framerate: 30,
      latencyMode: 'realtime', bitrateMode: 'variable', contentHint: 'detail',
    };
    if (this.codec.startsWith('avc1')) c.avc = { format: 'avc' };
    return c;
  }

  // Cada 2 s:
  // 1) el objetivo baja si hubo atasco (mi subida o el servidor) y sube de a poco tras 10 s sin atascos;
  // 2) comparo lo que salió de verdad con el objetivo y corrijo lo que le pido al codificador.
  ajustarCalidad() {
    const ahora = performance.now();
    const seg = (ahora - this.ultimoAjuste) / 1000;
    if (seg < 2) return;
    const real = (this.bytes * 8) / seg;
    this.bytes = 0;
    this.ultimoAjuste = ahora;

    if (this.huboAtasco) this.objetivo = Math.max(BITRATE_MIN, this.objetivo * 0.7);
    else if (ahora - this.ultimoAtasco > 10_000) this.objetivo = Math.min(this.limite, this.objetivo * 1.15);
    this.objetivo = Math.min(this.objetivo, this.limite);
    this.huboAtasco = false;

    const piso = this.objetivo * 0.35;
    let pedido = this.bitrate;
    if (real > this.objetivo * 1.15) pedido *= this.objetivo / real;
    else if (real < this.objetivo * 0.85) pedido *= 1.25;
    pedido = Math.round(Math.min(this.objetivo, Math.max(piso, pedido)));
    // Aun pidiéndole poco se pasa (codificador por software con la pantalla entera en movimiento):
    // bajo los cuadros por segundo lo justo. Cuando la pantalla se calma, los recupero.
    if (real > this.objetivo * 1.15 && pedido <= piso * 1.01) {
      this.fpsMax = Math.max(5, Math.floor((this.fpsMax * this.objetivo) / real));
    } else if (real < this.objetivo * 0.85 && this.fpsMax < 30) {
      this.fpsMax = Math.min(30, this.fpsMax + 5);
    }
    if (Math.abs(pedido - this.bitrate) > this.bitrate * 0.05) {
      this.bitrate = pedido;
      this.encoder.configure(this.configuracion());
    }
  }

  salida(chunk, meta) {
    const entro = this.entradas.get(chunk.timestamp);
    if (entro) { stats.codificarMs = performance.now() - entro; this.entradas.delete(chunk.timestamp); }
    const dc = meta?.decoderConfig;
    if (dc) {
      const config = {
        codec: dc.codec,
        codedWidth: dc.codedWidth || this.ancho,
        codedHeight: dc.codedHeight || this.alto,
        description: dc.description ? aBase64(dc.description) : null,
      };
      const texto = JSON.stringify(config);
      if (texto !== this.ultimoConfig) {
        this.ultimoConfig = texto;
        this.configMsg = { type: 'video-config', config };
        mandar(this.configMsg);
        // Con formato nuevo los espectadores necesitan un cuadro completo para arrancar.
        if (chunk.type !== 'key') this.forzarClave = true;
      }
    }
    if (!enLinea()) return;
    const buf = new ArrayBuffer(CABECERA + chunk.byteLength);
    const dv = new DataView(buf);
    dv.setUint8(0, chunk.type === 'key' ? 1 : 0);
    dv.setFloat64(1, chunk.timestamp);
    const t = Date.now();
    dv.setFloat64(9, t);
    chunk.copyTo(new Uint8Array(buf, CABECERA));
    this.bytes += buf.byteLength;
    mandarCuadro(buf);
    this.sinConfirmar.push(t);
    if (this.sinConfirmar.length > 300) this.sinConfirmar.shift();
  }

  detener() {
    this.activo = false;
    this.lector?.cancel().catch(() => {});
    if (this.encoder && this.encoder.state !== 'closed') this.encoder.close();
  }
}

// Ajusta al tope de 1080p sin deformar (medidas pares, que H264 lo exige).
function medida(w, h) {
  const k = Math.min(1, MAX_ANCHO / w, MAX_ALTO / h);
  return [Math.max(2, Math.round((w * k) / 2) * 2), Math.max(2, Math.round((h * k) / 2) * 2)];
}

async function elegirCodec(w, h) {
  const prueba = async (codec, hardwareAcceleration = 'no-preference') => {
    const c = { codec, width: w, height: h, bitrate: BITRATE_INICIAL, framerate: 30, latencyMode: 'realtime', hardwareAcceleration };
    if (codec.startsWith('avc1')) c.avc = { format: 'avc' };
    try { return (await VideoEncoder.isConfigSupported(c)).supported; } catch { return false; }
  };
  if (await prueba(H264, 'prefer-hardware')) return H264;
  for (const codec of CODECS_RESPALDO) if (await prueba(codec)) return codec;
  return null;
}

// ---------- Ver: recibir, decodificar, dibujar ----------
class Receptor {
  constructor() {
    this.decoder = null;
    this.esperaClave = true;
    this.ultimoPedido = 0;
    this.enviados = new Map();  // marca de tiempo -> hora de envío (para medir el retraso)
    this.vivo = true;
    this.error = null;
    // desynchronized: dibuja sin esperar al compositor del navegador (menos retraso).
    this.ctx = lienzo.getContext('2d', { alpha: false, desynchronized: true });
  }

  configurar(cfg) {
    this.cerrarDecoder();
    if (!('VideoDecoder' in window)) {
      this.error = 'Tu navegador no puede mostrar esta presentación. Prueba con Chrome, Edge o Safari actualizados.';
      return pintar();
    }
    this.config = { codec: cfg.codec, codedWidth: cfg.codedWidth, codedHeight: cfg.codedHeight, optimizeForLatency: true };
    if (cfg.description) this.config.description = deBase64(cfg.description);
    this.decoder = new VideoDecoder({
      output: (f) => this.alDecodificar(f),
      error: (e) => {
        console.warn('decodificador:', e);
        if (e.name === 'NotSupportedError') {
          this.error = 'Tu navegador no puede mostrar esta presentación. Prueba con Chrome, Edge o Safari actualizados.';
          return pintar();
        }
        this.configurar(cfg); // se rompió: empiezo de nuevo desde un cuadro completo
      },
    });
    this.decoder.configure(this.config);
    this.esperaClave = true;
  }

  cuadro(buf) {
    if (!this.decoder || this.decoder.state !== 'configured') return;
    const dv = new DataView(buf);
    const clave = (dv.getUint8(0) & 1) === 1;
    if (this.esperaClave && !clave) return this.pedirClave();
    // La máquina no da abasto decodificando: suelto lo atrasado y retomo desde un cuadro completo.
    if (!clave && this.decoder.decodeQueueSize > 150) {
      this.esperaClave = true;
      return this.pedirClave();
    }
    this.esperaClave = false;
    const marca = dv.getFloat64(1);
    const enviado = dv.getFloat64(9);
    stats.redMs = Date.now() - enviado;
    this.enviados.set(marca, { enviado, llegada: performance.now() });
    if (this.enviados.size > 120) this.enviados.delete(this.enviados.keys().next().value);
    this.decoder.decode(new EncodedVideoChunk({ type: clave ? 'key' : 'delta', timestamp: marca, data: new Uint8Array(buf, CABECERA) }));
  }

  pedirClave() {
    const ahora = performance.now();
    if (ahora - this.ultimoPedido < 2000) return;
    this.ultimoPedido = ahora;
    mandar({ type: 'need-key' });
  }

  alDecodificar(f) {
    stats.decodificados++;
    const e = this.enviados.get(f.timestamp);
    if (e) { stats.retrasoMs = Date.now() - e.enviado; stats.decodificarMs = performance.now() - e.llegada; }
    this.decodificadoEn = performance.now();
    this.dibujar(f); // en cuanto está listo, sin esperar al siguiente refresco de pantalla
  }

  dibujar(f) {
    if (!this.vivo) return f.close();
    if (lienzo.width !== f.displayWidth || lienzo.height !== f.displayHeight) {
      lienzo.width = f.displayWidth;
      lienzo.height = f.displayHeight;
    }
    this.ctx.drawImage(f, 0, 0);
    stats.dibujarMs = performance.now() - this.decodificadoEn;
    f.close();
    stats.cuadros++;
    stats.ancho = lienzo.width;
    stats.alto = lienzo.height;
    if (!lienzo.classList.contains('visible')) {
      lienzo.classList.add('visible');
      pintar();
    }
  }

  cerrarDecoder() {
    if (this.decoder && this.decoder.state !== 'closed') this.decoder.close();
    this.decoder = null;
  }

  detener() {
    this.vivo = false;
    this.cerrarDecoder();
    lienzo.classList.remove('visible');
  }
}

function detenerReceptor() {
  receptor?.detener();
  receptor = null;
  pintar();
}

// ---------- Botones ----------
$('#btn-presentar').addEventListener('click', async () => {
  if (miPantalla) return dejarDePresentar();
  if (!navigator.mediaDevices?.getDisplayMedia) {
    return aviso('Este navegador no permite compartir pantalla (en celular casi ninguno lo permite).');
  }
  if (!('VideoEncoder' in window)) {
    return aviso('Este navegador no puede presentar. Usa Chrome, Edge o Safari actualizados.');
  }
  try {
    miPantalla = await navigator.mediaDevices.getDisplayMedia({
      // Tope 1080p: una pantalla Retina entera (2880×1800) es casi 3× más trabajo y no se nota.
      video: { width: { max: MAX_ANCHO }, height: { max: MAX_ALTO }, frameRate: { ideal: 30, max: 30 } },
      audio: false,
    });
  } catch {
    return; // canceló el diálogo
  }
  const v = miPantalla.getVideoTracks()[0];
  v.contentHint = 'detail';
  v.onended = dejarDePresentar; // botón "Dejar de compartir" del navegador
  detenerReceptor();
  mostrarPropia(miPantalla);
  emisor = new Emisor(miPantalla);
  mandar({ type: 'start-present' });
});

function dejarDePresentar() {
  if (!miPantalla) return;
  soltarPantalla();
  mandar({ type: 'stop-present' });
}
function soltarPantalla() {
  emisor?.detener();
  emisor = null;
  miPantalla.getTracks().forEach((t) => { t.onended = null; t.stop(); });
  miPantalla = null;
  video.srcObject = null;
  video.classList.remove('visible');
  pintar();
}

// ---------- Escenario ----------
const video = $('#video');   // mi propia pantalla mientras presento
const lienzo = $('#lienzo'); // la pantalla de otro

function mostrarPropia(stream) {
  video.srcObject = stream;
  video.muted = true;
  video.classList.add('visible');
  video.play().catch(() => {});
  pintar();
}

$('#btn-completa').addEventListener('click', () => {
  const el = $('#escenario');
  if (document.fullscreenElement) document.exitFullscreen();
  else (el.requestFullscreen || el.webkitRequestFullscreen)?.call(el);
});

$('#btn-copiar').addEventListener('click', async () => {
  const url = `${location.origin}/?sala=${encodeURIComponent(sala)}`;
  try { await navigator.clipboard.writeText(url); aviso('Enlace copiado'); }
  catch { prompt('Copia este enlace:', url); }
});

$('#btn-salir').addEventListener('click', () => {
  saliendo = true;
  if (miPantalla) dejarDePresentar();
  ws?.close();
  location.href = '/';
});

// ---------- Interfaz ----------
const COLORES = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#06b6d4', '#ef4444', '#84cc16'];
function color(id) {
  let h = 0;
  for (const c of id) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return COLORES[h % COLORES.length];
}

function pintar() {
  const n = gente.size;
  $('#txt-cuantos').textContent = (n ? `· ${n} ${n === 1 ? 'persona' : 'personas'}` : '') + (local ? ' · red local' : ' · por internet');

  const ul = $('#lista-gente');
  ul.innerHTML = '';
  const orden = [...gente].sort(([a], [b]) => (a === presentador ? -1 : b === presentador ? 1 : a === yo ? -1 : b === yo ? 1 : 0));
  for (const [id, nombre] of orden) {
    const li = document.createElement('li');
    if (id === yo) li.className = 'yo';
    const av = document.createElement('span');
    av.className = 'avatar';
    av.style.background = color(id);
    av.textContent = nombre.trim().charAt(0).toUpperCase() || '?';
    const nm = document.createElement('span');
    nm.className = 'nombre';
    nm.textContent = nombre + (id === yo ? ' (tú)' : '');
    li.append(av, nm);
    if (id === presentador) {
      const chip = document.createElement('span');
      chip.className = 'chip';
      chip.textContent = 'Presenta';
      li.append(chip);
    }
    ul.append(li);
  }

  const presento = !!miPantalla;
  const hayVideo = video.classList.contains('visible') || lienzo.classList.contains('visible');
  const btn = $('#btn-presentar');
  btn.classList.toggle('primario', !presento);
  btn.classList.toggle('activo', presento);
  btn.querySelector('span').textContent = presento ? 'Dejar de presentar' : (presentador ? 'Presentar (tomar turno)' : 'Presentar pantalla');

  $('#vacio').hidden = hayVideo;
  $('#btn-completa').hidden = !hayVideo || presento;
  const et = $('#etiqueta');
  et.hidden = !hayVideo;
  et.textContent = presento ? 'Estás presentando · así te ven' : `Pantalla de ${gente.get(presentador) || '…'}`;

  if (!hayVideo) {
    $('#vacio-titulo').textContent = presentador ? `${gente.get(presentador)} está presentando` : 'Nadie está presentando';
    $('#vacio-texto').textContent = receptor?.error || (presentador ? 'Conectando…' : 'Cuando alguien comparta su pantalla, aparecerá aquí.');
  }
  document.title = presentador && presentador !== yo ? `${gente.get(presentador)} presenta · ${sala}` : `${sala} · Pantalla Compartida`;
}

function aviso(texto) {
  const d = document.createElement('div');
  d.className = 'aviso';
  d.textContent = texto;
  $('#avisos').append(d);
  setTimeout(() => d.remove(), 3000);
}

function aBase64(buf) {
  const bytes = buf instanceof ArrayBuffer ? new Uint8Array(buf) : new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}
function deBase64(texto) {
  return Uint8Array.from(atob(texto), (c) => c.charCodeAt(0));
}
