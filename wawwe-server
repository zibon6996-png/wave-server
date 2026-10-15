const { WebSocketServer } = require("ws");
const http = require("http");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("WAVE signal server is running\n");
});

const wss = new WebSocketServer({ server });
const rooms = new Map();

wss.on("connection", ws => {
  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "join") {
        ws.room = msg.room;
        ws.userId = msg.userId;
        if (!rooms.has(msg.room)) rooms.set(msg.room, new Set());
        rooms.get(msg.room).add(ws);
        broadcast(ws.room, { type: "peer-joined", userId: msg.userId, from: msg.userId });
        return;
      }
      const peers = rooms.get(ws.room) || new Set();
      for (const p of peers) {
        if (p === ws) continue;
        if (msg.to && p.userId !== msg.to) continue;
        if (p.readyState === 1) p.send(raw.toString());
      }
    } catch(e) {}
  });

  ws.on("close", () => {
    if (ws.room && rooms.has(ws.room)) {
      rooms.get(ws.room).delete(ws);
      broadcast(ws.room, { type: "peer-left", userId: ws.userId });
      if (rooms.get(ws.room).size === 0) rooms.delete(ws.room);
    }
  });
});

function broadcast(room, msg) {
  const peers = rooms.get(room) || new Set();
  const data = JSON.stringify(msg);
  for (const p of peers) {
    if (p.readyState === 1) p.send(data);
  }
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log("WAVE server on port " + PORT));
