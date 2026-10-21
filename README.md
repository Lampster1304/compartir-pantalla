# Pantalla Compartida

Salas para presentar pantalla sin cámara ni micrófono. El video va directo entre navegadores (WebRTC); el servidor solo pone en contacto a la gente.

```bash
npm install
npm start          # http://localhost:3077  (PORT=xxxx para otro puerto)
```

- Entra con tu nombre y una sala; comparte el enlace con «Copiar enlace».
- Una persona presenta a la vez; si otra pulsa «Presentar», toma el turno.
- En producción hace falta **HTTPS** (el navegador no deja compartir pantalla sin él, salvo en localhost).
- Redes muy cerradas (algunas corporativas o datos móviles) necesitan un servidor TURN:
  `TURN_URL=turn:host:3478 TURN_USER=... TURN_PASS=...`
- Pensada para grupos pequeños (~10 espectadores): el presentador sube una copia del video por cada uno.
