FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY server.js ./
COPY public ./public
ENV PORT=3077
EXPOSE 3077
CMD ["node", "server.js"]
