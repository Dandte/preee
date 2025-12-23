# Dockerfile para ADDI Scraper - EasyPanel
FROM node:20-bookworm-slim

# Instalar Chromium y dependencias
RUN apt-get update && apt-get install -y \
    chromium \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Configurar Puppeteer
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Copiar package files
COPY package*.json ./

# Instalar dependencias (sin package-lock.json)
RUN npm install --omit=dev --ignore-scripts

# Copiar código
COPY src ./src

# Puerto
EXPOSE 8000

CMD ["node", "src/index.js"]