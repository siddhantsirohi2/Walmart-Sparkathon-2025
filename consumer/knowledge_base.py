# consumer/kb_builder.py
from kafka import KafkaConsumer
import json
import os
import threading

# Define the path for the knowledge base file
# It's often good practice to define this relative to the script or use an absolute path
# If you run the consumer inside a Docker container, this path needs to be a mounted volume
# so that the data persists even if the container is removed.
# For simplicity, keeping it relative for now assuming you run it directly on host.
KNOWLEDGE_BASE_FILE = 'apriori_knowledge_base.json'

def load_knowledge_base():
    if os.path.exists(KNOWLEDGE_BASE_FILE):
        with open(KNOWLEDGE_BASE_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: {KNOWLEDGE_BASE_FILE} is empty or corrupted. Starting with empty KB.")
                return []
    return []

def save_knowledge_base(kb_data):
    os.makedirs(os.path.dirname(KNOWLEDGE_BASE_FILE) or '.', exist_ok=True) # Ensure directory exists
    with open(KNOWLEDGE_BASE_FILE, 'w') as f:
        json.dump(kb_data, f, indent=4)
    print(f"Knowledge base saved to {KNOWLEDGE_BASE_FILE}")

def get_kafka_consumer():
    # Use 'localhost:9092' when running consumer from host machine
    # Use 'kafka:9092' only when running inside Docker network
    return KafkaConsumer(
        'apriori-rules-output',
        bootstrap_servers=['localhost:9092'], #
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='knowledge-base-builder-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

if __name__ == '__main__':
    consumer = get_kafka_consumer()
    print("Knowledge Base Builder started. Listening for Apriori rules...")

    knowledge_base = load_knowledge_base()
    updated_rules_count = 0

    try:
        for message in consumer:
            rule = message.value
            print(f"Received rule from Kafka: {rule}")
            
            # Simple deduplication: Check if the rule (as a dictionary) already exists
            if rule not in knowledge_base:
                knowledge_base.append(rule)
                updated_rules_count += 1
                save_knowledge_base(knowledge_base)
                print(f"Rule added to knowledge base. Total rules: {len(knowledge_base)}")
            else:
                print("Rule already exists in knowledge base. Skipping.")

    except KeyboardInterrupt:
        print("\nStopping Knowledge Base Builder.")
        if updated_rules_count > 0:
            save_knowledge_base(knowledge_base)
            print(f"Saved {updated_rules_count} new/updated rules to {KNOWLEDGE_BASE_FILE}")
    finally:
        consumer.close()
        print("Kafka consumer closed.")