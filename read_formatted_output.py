from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'output-topic',
    bootstrap_servers=['localhost:9092'],
    group_id=None,                     # read everything, no group offsets
    auto_offset_reset='earliest',      # start from beginning
    enable_auto_commit=False,
    value_deserializer=lambda x: x.decode('utf-8', errors='ignore')
)

print("📥 Reading messages from 'output-topic' (Ctrl+C to stop)...")

try:
    for msg in consumer:
        raw = msg.value.strip()
        # Try JSON first
        try:
            obj = json.loads(raw)
            pretty = json.dumps(obj, indent=2, ensure_ascii=False)
            print("\n================ MESSAGE ================")
            print(pretty)
        except json.JSONDecodeError:
            # Fallback: plain text
            print("\n================ MESSAGE ================")
            print(raw)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("\n🛑 Stopped.")
