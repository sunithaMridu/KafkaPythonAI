import json
from kafka import KafkaProducer
import os

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📝 Interactive Kafka Producer (Ctrl+C to stop)")
print("Enter prompts (JSON format):")

try:
    while True:
        user_input = input("> ")
        if user_input.strip():
            message = {"prompt": user_input.strip()}
            producer.send('input-topic', message)
            producer.flush()
            print(f"✅ Sent: {user_input[:40]}...")
except KeyboardInterrupt:
    print("\n👋 Stopped")

producer.close()
