FROM node:22-bookworm-slim

# Install Python, C++ toolchain, and CA certs for Mediasoup worker prebuild/fallback
RUN apt-get update && apt-get install -y \
    ca-certificates \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY src/ ./src/
COPY index.html ./
RUN npm run build

EXPOSE 3000 40000-40010/udp

CMD ["node", "dist/server.js"]
