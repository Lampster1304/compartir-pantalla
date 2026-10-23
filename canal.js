// Canal directo por la red local (WebRTC DataChannel): los cuadros van del navegador a este
// servidor sin salir a internet. WebRTC cifra por su cuenta, así que no hace falta certificado
// (la página sigue cargando por https://call.cloudteco.com y la señalización va por el WebSocket).
// Si no se puede abrir (alguien fuera de la oficina, UDP bloqueado), todo sigue por el WebSocket.
let ndc = null;
if (!process.env.SIN_CANAL) { try { ndc = require('node-datachannel'); } catch {} } // SIN_CANAL=1: solo WebSocket

// Dentro de Docker el servidor solo ve su IP interna: a los navegadores hay que darles la de la
// red de la oficina (LAN_IP) y el puerto UDP publicado (RTC_PUERTO, el mismo dentro y fuera).
const IP_LOCAL = process.env.LAN_IP || '';
const PUERTO = Number(process.env.RTC_PUERTO) || 0;
const FRAG = 64 * 1024; // los canales aceptan mensajes de hasta 256 KB: los cuadros se parten

const disponible = () => !!ndc;

// Cuadro → trozos: byte 0 = 1 en el primero (seguido del tamaño total, 4 bytes) y 0 en los demás.
const trozosDe = new WeakMap();
function trozos(buf) {
  let t = trozosDe.get(buf);
  if (t) return t;
  t = [];
  for (let i = 0; i < buf.length; i += FRAG) {
    const parte = buf.subarray(i, i + FRAG);
    const cab = i === 0 ? 5 : 1;
    const m = Buffer.allocUnsafe(cab + parte.length);
    m[0] = i === 0 ? 1 : 0;
    if (i === 0) m.writeUInt32BE(buf.length, 1);
    parte.copy(m, cab);
    t.push(m);
  }
  trozosDe.set(buf, t); // el mismo cuadro va a 20 personas: se parte una sola vez
  return t;
}

function mandarCuadro(dc, buf) {
  for (const m of trozos(buf)) dc.sendMessageBinary(m);
}

function candidatoLimpio(c, vistos) {
  c = c.replace(/^a=/, '');
  const p = c.split(' ');
  if (p.length < 8 || (p[4] || '').includes(':')) return null; // sin IPv6
  if (IP_LOCAL && p[7] === 'host') p[4] = IP_LOCAL;
  const limpio = p.join(' ');
  if (vistos.has(limpio)) return null;
  vistos.add(limpio);
  return limpio;
}

// Abre el canal con un navegador a partir de su oferta. senal(msg) le manda la respuesta y los
// candidatos por el WebSocket; alAbrir(dc), alMensaje(datos, esBinario) y alCerrar() avisan.
function abrir(oferta, senal, { alAbrir, alMensaje, alCerrar }) {
  const pc = new ndc.PeerConnection('pantalla', {
    iceServers: [],
    ...(PUERTO ? { enableIceUdpMux: true, portRangeBegin: PUERTO, portRangeEnd: PUERTO } : {}),
  });
  const vistos = new Set();
  let cerrado = false;
  const cerrar = () => { if (cerrado) return; cerrado = true; alCerrar(); try { pc.close(); } catch {} };

  pc.onLocalDescription((sdp, type) => {
    const lineas = sdp.split(/\r?\n/).filter(Boolean).map((l) => {
      if (!l.startsWith('a=candidate:')) return l;
      const c = candidatoLimpio(l, vistos);
      return c ? 'a=' + c : null;
    }).filter(Boolean);
    senal({ type: 'rtc-answer', sdp: lineas.join('\r\n') + '\r\n', tipo: type });
  });
  pc.onLocalCandidate((cand, mid) => {
    const c = candidatoLimpio(cand, vistos);
    if (c) senal({ type: 'rtc-cand', candidate: c, mid });
  });
  pc.onStateChange((estado) => { if (estado === 'failed' || estado === 'closed' || estado === 'disconnected') cerrar(); });
  pc.onDataChannel((dc) => {
    let partes = null, falta = 0;
    dc.onMessage((m) => {
      if (typeof m === 'string') return alMensaje(m, false);
      const b = Buffer.isBuffer(m) ? m : Buffer.from(m);
      if (b[0] === 1) {
        partes = [b.subarray(5)];
        falta = b.readUInt32BE(1) - (b.length - 5);
      } else {
        if (!partes) return;
        partes.push(b.subarray(1));
        falta -= b.length - 1;
      }
      if (falta <= 0) {
        const todo = partes.length === 1 ? partes[0] : Buffer.concat(partes);
        partes = null;
        alMensaje(todo, true);
      }
    });
    dc.onClosed(cerrar);
    const listo = () => alAbrir(dc);
    if (dc.isOpen()) listo(); else dc.onOpen(listo);
  });
  pc.setRemoteDescription(oferta, 'offer');
  return {
    candidato: (c, mid) => { try { pc.addRemoteCandidate(c, mid || '0'); } catch {} },
    cerrar,
  };
}

module.exports = { disponible, abrir, mandarCuadro };
