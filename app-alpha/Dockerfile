FROM registry.redhat.io/ubi9/nodejs-22-minimal:latest

WORKDIR /app

COPY --chown=1001:1001 package*.json ./
RUN npm install --production

COPY --chown=1001:1001 server.js ./

USER 1001

EXPOSE 3000

CMD ["node", "server.js"]
