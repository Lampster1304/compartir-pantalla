// Prueba de estrés del reparto, sin navegadores (casi no gasta CPU).
// Un presentador falso manda cuadros del tamaño y ritmo de una pantalla real, y N espectadores
// livianos los reciben y confirman igual que la app. Mide fps, Mbps y retraso de cada uno.
//
// uso: node pruebas/estres.js <url> [opciones]
//   --espectadores 20   cuántos espectadores
//   --lentos 0          cuántos de ellos con internet lento (ver --lento-mbps)
//   --lento-mbps 1      bajada de los lentos
//   --mbps 2            bitrate del presentador
//   --fps 30            cuadros por segundo
//   --clave-kb 200      tamaño de un cuadro completo
//   --clave-cada 60     segundos entre cuadros completos de seguridad (como la app)
//   --segundos 20       tiempo de medición (después de 5 s de arranque)
//   --host nombre       cabecera Host (para ir directo a la IP del servidor, sin Cloudflare)
//   --adaptar           el presentador baja la calidad ante avisos de congestión, como la app
const path = require('path');
const WebSocket = require(path.join(__dirname, '..', 'node_modules', 'ws'));

const URL_BASE = process.argv[2];
if (!URL_BASE) { console.log('uso: node pruebas/estres.js <url> [opciones]'); process.exit(1); }
const opt = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? +process.argv[i + 1] : d; };
const optTexto = (n) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : ''; };
const HOST = optTexto('host');
const ADAPTAR = process.argv.includes('--adaptar');
const N = opt('espectadores', 20);
const LENTOS = Math.min(N, opt('lentos', 0));
const LENTO_BPS = opt('lento-mbps', 1) * 1e6 / 8;
const MBPS = opt('mbps', 2);
const FPS = opt('fps', 30);
const CLAVE = opt('clave-kb', 200) * 1024;
const CLAVE_CADA = opt('clave-cada', 60) * 1000;
const SEG = opt('segundos', 20);
const WS_URL = URL_BASE.replace(/^http/, 'ws').replace(/\/$/, '') + '/ws';
const SALA = 'estres-' + Math.random().toString(36).slice(2, 7);
const CAB = 17;

const sockets = [];
function cerrar() { for (const s of sockets) { try { s.terminate(); } catch {} } }
process.on('SIGINT', () => { cerrar(); process.exit(130); });
setTimeout(() => { console.log('Tiempo máximo: cierro todo.'); cerrar(); process.exit(2); }, (SEG + 40) * 1000);

const conectar = (nombre) => new Promise((ok, mal) => {
  const ws = new WebSocket(WS_URL, HOST ? { headers: { Host: HOST } } : {});
  sockets.push(ws);
  ws.on('open', () => { ws.send(JSON.stringify({ type: 'join', room: SALA, name: nombre })); ok(ws); });
  ws.on('error', mal);
});

// ---------- presentador falso ----------
const pres = { enviados: 0, bytes: 0, claves: 0, pedidas: 0, congestion: 0, sinConfirmar: new Map(), subida: [] };
let midiendo = false;
async function presentador() {
  const ws = await conectar('Presentador');
  let yo = null, enviando = false, pedirClave = true, seq = 0, ultimaClave = 0;
  ws.on('message', (data, bin) => {
    if (bin) return;
    const m = JSON.parse(data);
    if (m.type === 'welcome') { yo = m.id; ws.send(JSON.stringify({ type: 'start-present' })); }
    if (m.type === 'presenter' && m.id === yo && !enviando) {
      enviando = true;
      ws.send(JSON.stringify({ type: 'video-config', config: { codec: 'prueba', codedWidth: 1728, codedHeight: 1080, description: null } }));
      let mbps = MBPS, ultimoAviso = 0, ultimoAjuste = Date.now();
      pres.limitar = (bps) => { pres.limite = bps / 1e6; mbps = Math.min(mbps, pres.limite); };
      pres.ajustar = () => { mbps = Math.max(0.5, mbps * 0.7); ultimoAviso = Date.now(); };
      setInterval(() => {
        if (ADAPTAR && Date.now() - ultimoAjuste > 2000) {
          ultimoAjuste = Date.now();
          if (Date.now() - ultimoAviso > 10000) mbps = Math.min(MBPS, pres.limite ?? MBPS, mbps * 1.15);
        }
        pres.mbps = mbps;
        const delta = Math.max(200, Math.round((mbps * 1e6) / 8 / FPS));
        const ahora = Date.now();
        const clave = pedirClave || ahora - ultimaClave > CLAVE_CADA;
        if (clave) { pedirClave = false; ultimaClave = ahora; }
        const buf = Buffer.alloc(CAB + 4 + (clave ? CLAVE : delta));
        buf[0] = clave ? 1 : 0;
        buf.writeDoubleBE(seq, 1);
        buf.writeDoubleBE(ahora, 9);
        buf.writeUInt32BE(seq++, CAB);
        ws.send(buf);
        pres.sinConfirmar.set(ahora, ahora);
        if (midiendo) { pres.enviados++; pres.bytes += buf.length; if (clave) pres.claves++; }
      }, 1000 / FPS);
    }
    if (m.type === 'keyframe') { pedirClave = true; if (midiendo) pres.pedidas++; }
    if (m.type === 'limite' && ADAPTAR) pres.limitar?.(m.bps);
    if (m.type === 'congestion') { if (midiendo) pres.congestion++; if (ADAPTAR) pres.ajustar?.(); }
    if (m.type === 'ack-sub' && pres.sinConfirmar.has(m.t)) {
      if (midiendo) pres.subida.push(Date.now() - m.t);
      pres.sinConfirmar.delete(m.t);
    }
  });
}

// ---------- espectadores ----------
const esp = [];
async function espectador(i, lento) {
  const e = { lento, cuadros: 0, bytes: 0, retrasos: [], huecos: 0, ultimoSeq: -1, conFormato: false, maxT: 0, frescura: [] };
  esp.push(e);
  const ws = await conectar((lento ? 'Lento' : 'Espectador') + i);
  // Internet lento: leo del socket a un ritmo limitado (el resto se queda en la red, como en la vida real).
  let presupuesto = LENTO_BPS, ultimo = Date.now(), pausado = false;
  const recargar = () => {
    const ahora = Date.now();
    presupuesto = Math.min(LENTO_BPS, presupuesto + ((ahora - ultimo) / 1000) * LENTO_BPS);
    ultimo = ahora;
  };
  const reanudar = () => {
    recargar();
    if (presupuesto < 0) return setTimeout(reanudar, (-presupuesto / LENTO_BPS) * 1000 + 5);
    pausado = false;
    ws._socket.resume();
  };
  ws.on('message', (data, bin) => {
    if (!bin) { if (JSON.parse(data).type === 'video-config') e.conFormato = true; return; }
    const t = data.readDoubleBE(9);
    ws.send(JSON.stringify({ type: 'ack', t }));
    if (t > e.maxT) e.maxT = t;
    const seq = data.readUInt32BE(CAB);
    if (midiendo) {
      e.cuadros++; e.bytes += data.length; e.retrasos.push(Date.now() - t);
      if (e.ultimoSeq >= 0 && seq > e.ultimoSeq + 1) e.huecos += seq - e.ultimoSeq - 1;
    }
    e.ultimoSeq = seq;
    if (lento) {
      recargar();
      presupuesto -= data.length;
      if (presupuesto < 0 && !pausado) {
        pausado = true;
        ws._socket.pause();
        setTimeout(reanudar, (-presupuesto / LENTO_BPS) * 1000);
      }
    }
  });
}

const med = (a) => { const o = [...a].sort((x, y) => x - y); return o.length ? o[Math.floor(o.length / 2)] : NaN; };
const p95 = (a) => { const o = [...a].sort((x, y) => x - y); return o.length ? o[Math.floor(o.length * 0.95)] : NaN; };
const r = (a, d = 1) => (a.length ? `mín ${Math.min(...a).toFixed(d)} · mediana ${med(a).toFixed(d)} · máx ${Math.max(...a).toFixed(d)}` : '—');

(async () => {
  try {
    await presentador();
    await new Promise((ok) => setTimeout(ok, 500));
    await Promise.all(Array.from({ length: N }, (_, i) => espectador(i + 1, i < LENTOS)));
    await new Promise((ok) => setTimeout(ok, 5000));
    midiendo = true;
    const t0 = Date.now();
    // «Lo que se ve»: cada 100 ms, cuántos ms tiene el cuadro más nuevo que ya le llegó a cada uno.
    const muestreo = setInterval(() => { const ahora = Date.now(); for (const e of esp) if (e.maxT) e.frescura.push(ahora - e.maxT); }, 100);
    await new Promise((ok) => setTimeout(ok, SEG * 1000));
    clearInterval(muestreo);
    midiendo = false;
    const s = (Date.now() - t0) / 1000;

    console.log(`Sala ${SALA} en ${URL_BASE}: ${N} espectadores (${LENTOS} con ${LENTO_BPS * 8 / 1e6} Mbps), presentador a ${MBPS} Mbps / ${FPS} fps, ${s.toFixed(1)} s`);
    console.log(`Presentador: ${(pres.enviados / s).toFixed(1)} cuadros/s · ${((pres.bytes * 8) / s / 1e6).toFixed(2)} Mbps · ` +
      `subida confirmada en mediana ${med(pres.subida)} ms (p95 ${p95(pres.subida)}) · cuadros completos ${pres.claves} (pedidos ${pres.pedidas}) · avisos de congestión ${pres.congestion}` +
      (ADAPTAR ? ` · terminó en ${pres.mbps.toFixed(2)} Mbps (tope del servidor ${pres.limite?.toFixed(2) ?? '—'})` : ''));
    for (const [nombre, grupo] of [['Normales', esp.filter((e) => !e.lento)], ['Lentos', esp.filter((e) => e.lento)]]) {
      if (!grupo.length) continue;
      const fps = grupo.map((e) => e.cuadros / s);
      const mbps = grupo.map((e) => (e.bytes * 8) / s / 1e6);
      const ret = grupo.flatMap((e) => e.frescura);
      console.log(`${nombre} (${grupo.length}, con formato ${grupo.filter((e) => e.conFormato).length}): reciben ${r(fps)} cuadros/s · ${r(mbps, 2)} Mbps c/u ` +
        `(total ${mbps.reduce((a, b) => a + b, 0).toFixed(1)}) · antigüedad de lo que se ve: mediana ${med(ret)} ms, p95 ${p95(ret)} ms, máx ${ret.length ? Math.max(...ret) : '—'} ms · cuadros saltados ${grupo.reduce((a, e) => a + e.huecos, 0)}`);
    }
  } catch (e) {
    console.log('ERROR', e.message);
  } finally {
    cerrar();
    setTimeout(() => process.exit(0), 200);
  }
})();
