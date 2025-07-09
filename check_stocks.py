import json
import mysql.connector
from kafka import KafkaConsumer
from kafka import KafkaProducer
# Kafka consumer setup
consumer = KafkaConsumer(
    'predicted_sales',
    bootstrap_servers='localhost:9092',
    group_id='stock-checker-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Connect to your MySQL database
try:
    conn = mysql.connector.connect(
        host='localhost',       # or your DB host
        user='root',   # replace with your MySQL username
        password='rootpassword', # replace with your MySQL password
        database='mydb'       # name of your MySQL database
    )
    cursor = conn.cursor()
except Exception as e:
    print(f"❌ Failed to connect to MySQL: {e}")
    exit(1)

print("📡 Listening for predicted sales from Kafka...")

for message in consumer:
    try:
        data = message.value
        store_id = data['store_id']
        product_id = data['product_id']
        prediction = data['prediction']

        print(data)
        # Query actual stock
        cursor.execute(
            "SELECT stock FROM stocks WHERE store_id = %s AND product_id = %s",
            (store_id, product_id)
        )
        result = cursor.fetchone()

        if result is None:
            print(f"⚠️ No stock data for Store {store_id}, Product {product_id}")
            continue

        current_stock = result[0]

        # Determine stock status
        if current_stock < 0.5 * prediction:
            status = "UNDERSTOCKED"
        elif current_stock > 2 * prediction:
            status = "OVERSTOCKED"
        else:
            status = "OK"

        print(f"📦 Store {store_id}, Product {product_id}: Stock = {current_stock}, "
              f"Prediction = {prediction:.2f} → {status}")
        
        producer.send('current_status', {
        'store_id': store_id,
        'product_id': product_id,
        'status': status
        })

    except Exception as e:
        print(f"❌ Error processing message: {e}")
