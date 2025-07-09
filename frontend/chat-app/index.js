const http = require('http');
const express = require('express');
const { Kafka } = require('kafkajs');
const { Server } = require('socket.io');

const app = express();
const port = 3000;
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*', // Adjust for production
    methods: ['GET', 'POST']
  }
});

// === Kafka Setup ===
const kafka = new Kafka({
  clientId: 'express-socket-kafka-app',
  brokers: ['localhost:9092'] // Change if needed
});

const consumer = kafka.consumer({ groupId: 'web-group' });
const producer = kafka.producer();
const topics = ['predicted_sales', 'apriori-rules-output', 'current_status', 'chatbot-output'];

// === Run Kafka Consumer ===
const runKafkaConsumer = async () => {
  await consumer.connect();
  for (const t of topics) {
    await consumer.subscribe({ topic: t, fromBeginning: true });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const kafkaMsg = message.value.toString();
      console.log(`🟢 Kafka [${topic}]: ${kafkaMsg}`);

      // Emit to all clients
      io.emit('kafka-message', { topic, message: kafkaMsg });
      io.emit(`kafka-${topic}`, kafkaMsg); // optional topic-specific event
    },
  });
};

// === Run Kafka Producer ===
const runKafkaProducer = async () => {
  await producer.connect();
};

// === Socket.IO Handling ===
io.on('connection', (socket) => {
  console.log('🔌 A client connected');

  socket.on('message', async (msg) => {
    console.log(`📩 Message from client: ${msg}`);

    try {
      await producer.send({
        topic: 'chatbot-input',
        messages: [{ value: msg }],
      });
      console.log(`✅ Sent to Kafka topic 'chatbot-input'`);
    } catch (err) {
      console.error('❌ Kafka Producer Error:', err);
    }
  });

  socket.on('disconnect', () => {
    console.log('❌ Client disconnected');
  });
});

// === Static Frontend Serving (Optional) ===
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html');
});

// === Start Server & Kafka ===
server.listen(port, () => {
  console.log(`🚀 Server is running at http://localhost:${port}`);
});

runKafkaConsumer().catch(console.error);
runKafkaProducer().catch(console.error);
