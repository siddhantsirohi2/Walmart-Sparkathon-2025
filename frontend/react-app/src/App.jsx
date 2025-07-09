import React, { useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

function App() {
  const [dataMap, setDataMap] = useState({});
  const [storeOptions, setStoreOptions] = useState([]);
  const [productOptions, setProductOptions] = useState([]);
  const [selectedStore, setSelectedStore] = useState('');
  const [selectedProduct, setSelectedProduct] = useState('');
  const socketRef = useRef(null);

  useEffect(() => {
    if (!socketRef.current) {
      socketRef.current = io('http://localhost:3000');

      socketRef.current.on('kafka-message', (msg) => {
        try {
          const parsed = typeof msg === 'string' ? JSON.parse(msg) : msg;
          const { product_id, store_id, prediction } = parsed;

          const key = `${store_id}-${product_id}`;
          setDataMap(prev => {
            const prevData = prev[key] || [];
            const newData = [...prevData, { timestamp: new Date().toLocaleTimeString(), prediction }];
            return { ...prev, [key]: newData.slice(-20) }; // keep last 20
          });

          setStoreOptions(prev => prev.includes(store_id) ? prev : [...prev, store_id]);
          setProductOptions(prev => prev.includes(product_id) ? prev : [...prev, product_id]);
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

  const currentKey = `${selectedStore}-${selectedProduct}`;
  const chartData = dataMap[currentKey] || [];

  return (
    <div style={{ padding: '20px' }}>
      <h2>📊 Live Predictions from Kafka</h2>

      <div style={{ marginBottom: '10px' }}>
        <label>Store ID: </label>
        <select value={selectedStore} onChange={(e) => setSelectedStore(e.target.value)}>
          <option value="">-- Select Store --</option>
          {storeOptions.map((store, idx) => (
            <option key={idx} value={store}>{store}</option>
          ))}
        </select>

        <label style={{ marginLeft: '20px' }}>Product ID: </label>
        <select value={selectedProduct} onChange={(e) => setSelectedProduct(e.target.value)}>
          <option value="">-- Select Product --</option>
          {productOptions.map((product, idx) => (
            <option key={idx} value={product}>{product}</option>
          ))}
        </select>
      </div>

      {chartData.length > 0 ? (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="timestamp" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="prediction" stroke="#8884d8" activeDot={{ r: 6 }} />
          </LineChart>
        </ResponsiveContainer>
      ) : (
        <p>Select a store and product to see predictions.</p>
      )}
    </div>
  );
}

export default App;
