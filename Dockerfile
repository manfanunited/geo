FROM node:20-alpine
WORKDIR /app
COPY package.json server.js index.html ./
ENV TUNNEL=none
EXPOSE 8080
CMD ["node", "server.js"]
