FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --prefer-offline
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]