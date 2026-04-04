FROM node:20-slim
WORKDIR /app
RUN npm install -g npm@10.5.0
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
