const http = require('http');
const express = require('express');
const { Kafka } = require('kafkajs');
const { Server } = require('socket.io');

const app = express();
const port = 3000;
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*', // or specific origin like 'http://localhost:3001'
    methods: ['GET', 'POST']
  }
});


// === Kafka Configuration ===
const kafka = new Kafka({
  clientId: 'express-socket-kafka-app',
  brokers: ['localhost:9092'] // Change if needed
});

const consumer = kafka.consumer({ groupId: 'web-group' });
const topic = 'predicted_sales'; // Same topic your producer is sending messages to

// === Socket.IO Connection ===
io.on('connection', (socket) => {
  console.log('🔌 A client connected');

  socket.on('message', (msg) => {
    console.log(`📩 Message from client: ${msg}`);
  });

  socket.on('disconnect', () => {
    console.log('❌ Client disconnected');
  });
});

// === Kafka Consumer Logic ===
const runKafkaConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const kafkaMsg = message.value.toString();
      console.log(`🟢 Kafka: ${kafkaMsg}`);

      // Broadcast to all connected clients
      io.emit('kafka-message', kafkaMsg);
    },
  });
};

runKafkaConsumer().catch(console.error);

// === Express Routing ===
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html');
});

server.listen(port, () => {
  console.log(`🚀 Server is running at http://localhost:${port}`);
});
