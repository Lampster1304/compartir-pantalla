FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY server.js canal.js ./
COPY public ./public
ENV PORT=3077
EXPOSE 3077
# Canal directo por la red local (WebRTC): un solo puerto UDP, publicado igual fuera (RTC_PUERTO)
EXPOSE 47800/udp
CMD ["node", "server.js"]
