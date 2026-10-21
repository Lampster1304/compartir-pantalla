// Cliente: el presentador abre una conexión WebRTC con cada espectador
// y le manda su pantalla. Los espectadores solo reciben.
const $ = (s) => document.querySelector(s);

let ws = null;
let yo = null;            // mi id
let miNombre = '';
let sala = '';
let presentador = null;   // id de quien presenta (o null)
const gente = new Map();  // id -> nombre
const conexiones = new Map(); // id -> RTCPeerConnection
const pendientes = new Map(); // id -> candidatos ICE que llegaron antes de tiempo
let miPantalla = null;    // MediaStream si estoy presentando
let iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
let saliendo = false;

// ---------- Entrada ----------
const params = new URLSearchParams(location.search);
$('#in-sala').value = params.get('sala') || 'teco2021';
try { $('#in-nombre').value = localStorage.getItem('nombre') || ''; } catch {}
if (params.get('sala')) $('#in-nombre').focus();

$('#form-entrar').addEventListener('submit', async (e) => {
  e.preventDefault();
  miNombre = $('#in-nombre').value.trim() || 'Invitado';
  sala = $('#in-sala').value.trim().toLowerCase();
  try { localStorage.setItem('nombre', miNombre); } catch {}
  history.replaceState(null, '', '?sala=' + encodeURIComponent(sala));
  try { iceServers = (await (await fetch('/config')).json()).iceServers; } catch {}
  $('#lobby').hidden = true;
  $('#sala').hidden = false;
  $('#txt-sala').textContent = sala;
  conectar();
});

// ---------- Señalización ----------
function conectar() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => ws.send(JSON.stringify({ type: 'join', room: sala, name: miNombre }));
  ws.onmessage = (e) => alMensaje(JSON.parse(e.data));
  ws.onclose = () => {
    $('#estado-punto').className = 'punto mal';
    if (saliendo) return;
    cerrarTodas();
    aviso('Se perdió la conexión, reconectando…');
    setTimeout(conectar, 2000);
  };
}

function mandar(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

async function alMensaje(m) {
  switch (m.type) {
    case 'welcome':
      yo = m.id;
      gente.clear();
      gente.set(yo, miNombre);
      for (const p of m.peers) gente.set(p.id, p.name);
      presentador = m.presenter;
      $('#estado-punto').className = 'punto ok';
      // Si se cayó la conexión mientras presentaba, retomo.
      if (miPantalla) mandar({ type: 'start-present' });
      pintar();
      break;

    case 'peer-joined':
      gente.set(m.id, m.name);
      aviso(`${m.name} entró`);
      if (presentador === yo) llamar(m.id);
      pintar();
      break;

    case 'peer-left':
      if (gente.has(m.id)) aviso(`${gente.get(m.id)} salió`);
      gente.delete(m.id);
      cerrar(m.id);
      pintar();
      break;

    case 'presenter':
      cerrarTodas();
      presentador = m.id;
      if (m.id === yo) {
        for (const id of gente.keys()) if (id !== yo) llamar(id);
      } else if (miPantalla) {
        // Alguien más tomó el turno.
        soltarPantalla();
        aviso(`${gente.get(m.id) || 'Otra persona'} empezó a presentar`);
      }
      pintar();
      break;

    case 'signal':
      await alSenal(m.from, m.data);
      break;
  }
}

// ---------- WebRTC ----------
function nuevaConexion(id) {
  cerrar(id);
  const pc = new RTCPeerConnection({ iceServers });
  conexiones.set(id, pc);
  pc.onicecandidate = (e) => {
    if (e.candidate) mandar({ type: 'signal', to: id, data: { candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    // Si se rompe y yo presento, vuelvo a intentar una vez.
    if (pc.connectionState === 'failed' && presentador === yo && conexiones.get(id) === pc) {
      setTimeout(() => { if (presentador === yo && gente.has(id)) llamar(id); }, 1500);
    }
  };
  return pc;
}

async function llamar(id) {
  if (!miPantalla) return;
  const pc = nuevaConexion(id);
  for (const track of miPantalla.getTracks()) {
    const sender = pc.addTrack(track, miPantalla);
    if (track.kind === 'video') {
      // Si falta red o máquina, baja un poco la nitidez antes que los cuadros.
      const p = sender.getParameters();
      p.degradationPreference = 'balanced';
      p.encodings = (p.encodings?.length ? p.encodings : [{}]).map((e) => ({ ...e, maxBitrate: 4_000_000, maxFramerate: 30 }));
      sender.setParameters(p).catch(() => {});
    }
  }
  const oferta = await pc.createOffer();
  await pc.setLocalDescription(oferta);
  mandar({ type: 'signal', to: id, data: { sdp: pc.localDescription } });
}

async function alSenal(de, data) {
  if (data.sdp?.type === 'offer') {
    // Solo acepto video de quien está presentando.
    if (de !== presentador) return;
    const pc = nuevaConexion(de);
    pc.ontrack = (e) => mostrarVideo(e.streams[0]);
    await pc.setRemoteDescription(data.sdp);
    await soltarPendientes(de, pc);
    const resp = await pc.createAnswer();
    await pc.setLocalDescription(resp);
    mandar({ type: 'signal', to: de, data: { sdp: pc.localDescription } });
  } else if (data.sdp?.type === 'answer') {
    const pc = conexiones.get(de);
    if (!pc) return;
    await pc.setRemoteDescription(data.sdp);
    await soltarPendientes(de, pc);
  } else if (data.candidate) {
    const pc = conexiones.get(de);
    if (pc && pc.remoteDescription) {
      await pc.addIceCandidate(data.candidate).catch(() => {});
    } else {
      if (!pendientes.has(de)) pendientes.set(de, []);
      pendientes.get(de).push(data.candidate);
    }
  }
}

async function soltarPendientes(id, pc) {
  for (const c of pendientes.get(id) || []) await pc.addIceCandidate(c).catch(() => {});
  pendientes.delete(id);
}

function cerrar(id) {
  const pc = conexiones.get(id);
  if (pc) { pc.ontrack = null; pc.onconnectionstatechange = null; pc.close(); }
  conexiones.delete(id);
}
function cerrarTodas() {
  for (const id of [...conexiones.keys()]) cerrar(id);
  pendientes.clear();
  if (!miPantalla) quitarVideo();
}

// ---------- Presentar ----------
$('#btn-presentar').addEventListener('click', async () => {
  if (miPantalla) return dejarDePresentar();
  if (!navigator.mediaDevices?.getDisplayMedia) {
    return aviso('Este navegador no permite compartir pantalla (en celular casi ninguno lo permite).');
  }
  try {
    miPantalla = await navigator.mediaDevices.getDisplayMedia({
      // Tope 1080p: una pantalla Retina entera (2880×1800) es casi 3× más trabajo y no se nota.
      video: { width: { max: 1920 }, height: { max: 1080 }, frameRate: { ideal: 30, max: 30 } },
      audio: true, // sonido de la pestaña/sistema si el navegador lo ofrece
    });
  } catch {
    return; // canceló el diálogo
  }
  const v = miPantalla.getVideoTracks()[0];
  v.contentHint = 'detail'; // texto nítido; si falta red, 'balanced' reparte entre nitidez y fps
  v.onended = dejarDePresentar; // botón "Dejar de compartir" del navegador
  mostrarVideo(miPantalla, true);
  mandar({ type: 'start-present' });
});

function dejarDePresentar() {
  if (!miPantalla) return;
  soltarPantalla();
  mandar({ type: 'stop-present' });
}
function soltarPantalla() {
  miPantalla.getTracks().forEach((t) => { t.onended = null; t.stop(); });
  miPantalla = null;
  quitarVideo();
}

// ---------- Video ----------
const video = $('#video');

function mostrarVideo(stream, propio = false) {
  video.srcObject = stream;
  video.muted = propio; // mi propia pantalla, sin eco
  video.classList.add('visible');
  $('#btn-sonido').hidden = true;
  video.play().catch(() => {
    // El navegador bloqueó el sonido automático: arranco en silencio.
    video.muted = true;
    video.play().catch(() => {});
    if (stream.getAudioTracks().length) $('#btn-sonido').hidden = false;
  });
  pintar();
}
function quitarVideo() {
  video.srcObject = null;
  video.classList.remove('visible');
  $('#btn-sonido').hidden = true;
  pintar();
}

$('#btn-sonido').addEventListener('click', () => {
  video.muted = false;
  video.play();
  $('#btn-sonido').hidden = true;
});

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
  $('#txt-cuantos').textContent = n ? `· ${n} ${n === 1 ? 'persona' : 'personas'}` : '';

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
  const hayVideo = video.classList.contains('visible');
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
    $('#vacio-texto').textContent = presentador ? 'Conectando…' : 'Cuando alguien comparta su pantalla, aparecerá aquí.';
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
