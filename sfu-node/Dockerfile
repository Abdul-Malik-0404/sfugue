FROM node:20-bullseye-slim

# Install Python & C++ build toolchain for Mediasoup worker compilation
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
RUN npx tsc

EXPOSE 3000 40000-40010/udp

CMD ["node", "dist/server.js"]
