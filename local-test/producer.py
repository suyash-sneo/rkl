import json
import random
import string
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def generate_random_string(length=10):
    """Generate a random string of fixed length"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_json():
    """Generate a random JSON object"""
    return {
        "id": generate_random_string(8),
        "name": generate_random_string(15),
        "value": random.randint(1, 1000),
        "timestamp": time.time(),
        "data": {
            "field1": generate_random_string(20),
            "field2": random.uniform(0, 100),
            "field3": random.choice([True, False])
        }
    }

def wait_for_kafka(bootstrap_servers, max_retries=30):
    """Wait for Kafka to be ready"""
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            print("Kafka is ready!")
            return True
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... (attempt {i+1}/{max_retries})")
            time.sleep(2)
    return False

def main():
    bootstrap_servers = ['kafka:29092']
    topic = 'random-data'
    num_messages = 10000
    
    print("Waiting for Kafka to be available...")
    if not wait_for_kafka(bootstrap_servers):
        print("Failed to connect to Kafka after maximum retries")
        return
    
    print("Creating Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print(f"Starting to produce {num_messages} messages to topic '{topic}'...")
    
    for i in range(num_messages):
        key = generate_random_string(12)
        value = generate_random_json()
        
        producer.send(topic, key=key, value=value)
        
        if (i + 1) % 1000 == 0:
            print(f"Produced {i + 1} messages...")
    
    # Wait for all messages to be delivered
    producer.flush()
    print(f"Successfully produced {num_messages} messages!")
    
    producer.close()
    print("Producer finished and closed.")

if __name__ == "__main__":
    main()
