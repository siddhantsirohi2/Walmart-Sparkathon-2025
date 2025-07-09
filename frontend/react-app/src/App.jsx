import React, { useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';

function App() {
  const [messages, setMessages] = useState([]);
  const [inputText, setInputText] = useState('');
  const socketRef = useRef(null);

  useEffect(() => {
    if (!socketRef.current) {
      socketRef.current = io('http://localhost:3000');

      socketRef.current.on('kafka-message', (msg) => {
        try {
          const parsed = typeof msg === 'string' ? JSON.parse(msg) : msg;
          console.log("Kafka Message Received:", parsed);
          setMessages(prev => [...prev, parsed]);
        } catch (err) {
          console.error("Failed to parse message", err);
        }
      });
    }

    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
        socketRef.current = null;
      }
    };
  }, []);

  const sendMessage = () => {
    if (socketRef.current && inputText.trim()) {
      socketRef.current.emit('message', inputText.trim());
      setInputText('');
    }
  };

  return (
    <div>
      <h2>Incoming Kafka Messages</h2>
      {messages.map((msg, idx) => (
        <pre key={idx}>
          {JSON.stringify(msg, null, 2)}
        </pre>
      ))}

      <div>
        <input
          type="text"
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          placeholder="Type a message"
        />
        <button onClick={sendMessage}>Send</button>
      </div>
    </div>
  );
}

export default App;
