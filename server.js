const express = require('express');
const amqp = require('amqplib');
const path = require('path');
const crypto = require('crypto');

const QUEUE = 'items';
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';
const PROCESS_MS = Number(process.env.PROCESS_MS || 5000); // thời gian xử lý mỗi item
const PORT = Number(process.env.PORT || 3000);

// Trạng thái item lưu trong memory: id -> {id, name, priority, version, status}
const items = new Map();
let channel;

function publish(item) {
  channel.sendToQueue(
    QUEUE,
    Buffer.from(JSON.stringify({ id: item.id, version: item.version })),
    { persistent: true, priority: item.priority }
  );
}

async function connectRabbit() {
  for (;;) {
    try {
      const conn = await amqp.connect(RABBIT_URL);
      channel = await conn.createChannel();
      await channel.assertQueue(QUEUE, {
        durable: true,
        arguments: { 'x-max-priority': 10 },
      });
      await channel.prefetch(1); // xử lý lần lượt từng item
      console.log('Connected to RabbitMQ');
      return;
    } catch (e) {
      console.log('RabbitMQ chưa sẵn sàng, thử lại sau 3s...');
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

function startConsumer() {
  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    const { id, version } = JSON.parse(msg.content.toString());
    const item = items.get(id);

    // Message cũ (đã đổi priority và publish lại) -> bỏ qua
    if (!item || item.version !== version) {
      channel.ack(msg);
      return;
    }

    item.status = 'processing';
    setTimeout(() => {
      item.status = 'done';
      channel.ack(msg);
    }, PROCESS_MS);
  });
}

const app = express();
app.use(express.json());

app.get('/', (_, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/items', (_, res) => res.json([...items.values()]));

app.post('/api/items', (req, res) => {
  const name = String(req.body.name || '').trim();
  const priority = Math.min(10, Math.max(0, Number(req.body.priority) || 0));
  if (!name) return res.status(400).json({ error: 'name required' });
  const item = { id: crypto.randomUUID(), name, priority, version: 1, status: 'queued' };
  items.set(item.id, item);
  publish(item);
  res.status(201).json(item);
});

// Đổi priority: RabbitMQ không sửa được priority của message đã publish,
// nên tăng version rồi publish lại; message cũ sẽ bị consumer bỏ qua.
app.patch('/api/items/:id', (req, res) => {
  const item = items.get(req.params.id);
  if (!item) return res.status(404).json({ error: 'not found' });
  if (item.status !== 'queued')
    return res.status(409).json({ error: 'item không còn ở trạng thái queued' });
  item.priority = Math.min(10, Math.max(0, Number(req.body.priority) || 0));
  item.version += 1;
  publish(item);
  res.json(item);
});

(async () => {
  await connectRabbit();
  startConsumer();
  app.listen(PORT, () => console.log(`UI: http://localhost:${PORT}`));
})();
