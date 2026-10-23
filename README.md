# Pantalla Compartida

Salas para presentar pantalla sin cámara ni micrófono. En producción: https://call.cloudteco.com

```bash
npm install
npm start          # http://localhost:3077  (PORT=xxxx para otro puerto)
```

- Entra con tu nombre y una sala (por defecto `teco2021`); comparte el enlace con «Copiar enlace».
- Una persona presenta a la vez; si otra pulsa «Presentar», toma el turno.

## Cómo viaja la pantalla

El presentador codifica su pantalla **una sola vez** en el navegador (WebCodecs: H264 por la tarjeta
de video si la hay, si no VP8) y la sube por el mismo WebSocket de la sala. El servidor **no recodifica**:
copia cada cuadro a todos los espectadores, que lo decodifican y lo dibujan en un `<canvas>`.
Así, al presentador le cuesta lo mismo 1 espectador que 20.

- Tope 1080p y 30 fps; objetivo de 4 Mbps. El presentador mide lo que realmente sale y corrige
  (algunos codificadores por software se pasan del doble); si aun así no alcanza, baja los fps.
- Si un espectador tiene mal internet, el servidor le salta cuadros hasta el próximo cuadro completo
  (sin frenar a los demás). Si se atasca mucha gente a la vez, le pide al presentador bajar la calidad.
- Quien entra tarde recibe lo último desde el último cuadro completo y ve la pantalla al instante.
- **Red local de la oficina:** la página entra por https://call.cloudteco.com, pero en cuanto se puede
  se abre un canal WebRTC (DataChannel, sin certificado) directo al servidor por la red local y todo
  lo de esa persona va por ahí: de ~80 ms (ida y vuelta a Miami por Cloudflare) a unos pocos ms.
  Variables: `LAN_IP` (IP del servidor en la oficina, 172.18.22.210 por cable) y `RTC_PUERTO`
  (47800, publicado como `47800:47800/udp`). Si el canal no abre (fuera de la oficina), todo sigue
  por el WebSocket; la barra dice «red local» o «por internet». `SIN_CANAL=1` lo apaga.
- Sin sonido (solo imagen).
- Tope de salida total: `SALIDA_MAX_MBPS` (por defecto 20). Al presentador se le pide como mucho
  ese tope ÷ espectadores (y nunca más de 4 Mbps). Hoy el servidor sale por el internet de la
  oficina (~20–40 Mbps compartidos): saturarlo atrasa segundos todo lo que usa esa línea. En un
  servidor de datacenter con ≥100 Mbps de subida se puede subir a 80 (20 personas × 4 Mbps).
- Si mucha gente se atrasa a la vez (la salida compartida no da), no se saltan cuadros: la cola se
  vacía sola y se le pide al presentador bajar la calidad.
- Cada espectador confirma cada cuadro que recibe; si uno lleva más de 1 s sin confirmarse, el servidor
  le salta cuadros a esa persona hasta que se ponga al día (el retraso no crece aunque el atasco esté
  en Cloudflare o en su wifi). El servidor también le confirma al presentador lo que sube.
- En producción Cloudflare guarda los .js/.css 4 h: `server.js` les pone `?v=` nuevo en cada arranque.

## Pruebas de carga

`node pruebas/estres.js <url> --espectadores 20 --mbps 2` simula un presentador y N espectadores
livianos (sin navegadores, casi sin CPU) y mide fps, Mbps y retraso de cada uno; `--lentos 3` agrega
espectadores con internet lento. Para medir el VPS de verdad sin gastar el internet propio:
GitHub → Actions → «Prueba de carga» → Run workflow (corre lo mismo en un servidor de GitHub).
