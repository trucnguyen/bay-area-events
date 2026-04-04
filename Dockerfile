FROM node:20-slim
WORKDIR /app
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
