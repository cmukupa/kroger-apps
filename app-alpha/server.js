import http from "node:http";
import { readFileSync } from "node:fs";
import { KafkaJS } from "@confluentinc/kafka-javascript";

const APP_NAME  = "app-alpha";
const PORT      = parseInt(process.env.PORT ?? "3000", 10);

// ---------- Kafka ----------

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} environment variable is required`);
  return v;
}

const { Kafka, logLevel } = KafkaJS;

function createProducer() {
  const kafka = new Kafka({
    kafkaJS: {
      clientId: APP_NAME,
      brokers: requireEnv("KAFKA_BROKERS").split(",").map((b) => b.trim()),
      ssl: true,
      sasl: {
        mechanism: "plain",
        username: requireEnv("KAFKA_API_KEY"),
        password: requireEnv("KAFKA_API_SECRET"),
      },
      logLevel: logLevel.ERROR,
    },
  });
  return kafka.producer({ kafkaJS: { acks: -1 } });
}

// ---------- App state ----------

let isOnline = true;
let offlineAt = null;

async function goOffline(producer) {
  if (!isOnline) return; // already offline
  isOnline = false;
  offlineAt = new Date().toISOString();

  await producer.connect();
  try {
    await producer.send({
      topic: requireEnv("KAFKA_APP_EVENTS_TOPIC"),
      messages: [{
        key: APP_NAME,
        value: JSON.stringify({
          event: "APP_OFFLINE",
          app: APP_NAME,
          timestamp: offlineAt,
        }),
      }],
    });
    console.log(`[${APP_NAME}] APP_OFFLINE event published to Kafka`);
  } finally {
    await producer.disconnect().catch(() => undefined);
  }
}

// ---------- Store lookup page ----------

const PAGES = {
  "/":       { type: "text/html; charset=utf-8",       body: readFileSync(new URL("./public/index.html", import.meta.url)) },
  "/app.js": { type: "text/javascript; charset=utf-8", body: readFileSync(new URL("./public/app.js", import.meta.url)) },
};

const STORES = [
  { id: "S001", name: "Cincinnati - Main St" },
  { id: "S002", name: "Columbus - Polaris" },
  { id: "S003", name: "Louisville - Shelbyville Rd" },
  { id: "S004", name: "Indianapolis - Keystone" },
];

const PAGE_HEADERS = {
  "Content-Security-Policy": "default-src 'self'; style-src 'self' 'unsafe-inline'",
  "X-Content-Type-Options": "nosniff",
};

// ---------- HTTP server ----------

const producer = createProducer();

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    const status = isOnline ? 200 : 503;
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: isOnline ? "online" : "offline",
      app: APP_NAME,
      timestamp: new Date().toISOString(),
    }));
    return;
  }

  if (req.method === "GET" && PAGES[req.url]) {
    res.writeHead(200, { "Content-Type": PAGES[req.url].type, ...PAGE_HEADERS });
    res.end(PAGES[req.url].body);
    return;
  }

  if (req.method === "GET" && req.url === "/api/stores") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ stores: STORES }));
    return;
  }

  if (req.method === "POST" && req.url === "/shutdown") {
    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Shutdown initiated, APP_OFFLINE event will be sent" }));
    // Fire and forget — do not await in the request handler
    goOffline(producer).catch((err) =>
      console.error(`[${APP_NAME}] Failed to publish offline event:`, err.message),
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[${APP_NAME}] listening on 0.0.0.0:${PORT}`);
});
