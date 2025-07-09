import React, { useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';

function App() {
  const [messages, setMessages] = useState([]);
  const socketRef = useRef(null);

  useEffect(() => {
    // Connect only once
    if (!socketRef.current) {
      socketRef.current = io('http://localhost:3000');

      socketRef.current.on('kafka-message', (msg) => {
        setMessages([msg]);
      });
    }

    // Cleanup on unmount
    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
        socketRef.current = null;
      }
    };
  }, []);

  return (
    <div style={{ padding: '20px' }}>
      <h2>Kafka Messages (Live)</h2>
      <ul>
        {messages.map((msg, index) => (
          <li key={index} style={{ marginBottom: '5px' }}>
            {msg}
          </li>
        ))}
      </ul>
    </div>
  );
}

export default App;
