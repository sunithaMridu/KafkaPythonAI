import os
import json
import signal
from kafka import KafkaConsumer, KafkaProducer
from openai import OpenAI

def safe_parse(value):
    if isinstance(value, bytes): value = value.decode('utf-8', errors='ignore')
    value = value.strip()
    if value.startswith('{'): return json.loads(value)
    return {"prompt": value}

# Kill any old consumer groups
os.system("kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group openrouter-processor-v1 --delete 2>/dev/null")

shutdown = False
signal.signal(signal.SIGINT, lambda s,f: globals().update(shutdown=True))

# OpenRouter client
client = OpenAI(
    api_key=os.getenv('OPENROUTER_API_KEY'),
    base_url="https://openrouter.ai/api/v1"
)

consumer = KafkaConsumer(
    'input-topic',
    bootstrap_servers=['localhost:9092'],
    group_id=None,   
    value_deserializer=lambda x: x,
    enable_auto_commit=True,
    consumer_timeout_ms=2000,
    fetch_max_wait_ms=500,
    auto_offset_reset='earliest',
    heartbeat_interval_ms=2000,
    session_timeout_ms=10000
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 OpenRouter FIXED Consumer")
print("Waiting 3s for Kafka coordination...")
import time; time.sleep(3)

message_count = 0
poll_count = 0

print("🔄 Starting message loop...")
for message in consumer:
    poll_count += 1
    if poll_count % 10 == 0:
        print(f"⏳ Polled {poll_count} times, processed {message_count}")
    
    if shutdown: break
    
    message_count += 1
    print(f"\n{'='*50}")
    print(f"📥 [{message_count}] RAW: {repr(message.value[:80])}")
    
    try:
        data = safe_parse(message.value)
        prompt = data.get('prompt', '').strip()
        
        if not prompt:
            print(f"⚠️ No prompt")
            consumer.commit()
            continue
        
        print(f"   Prompt: {prompt[:50]}...")
        
        # OpenRouter call
        response = client.chat.completions.create(
            model="anthropic/claude-3.5-sonnet",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        
        reply = response.choices[0].message.content.strip()
        
        producer.send('output-topic', {
            "prompt": prompt,
            "response": reply,
            "model": "claude-3.5-sonnet"
        })
        producer.flush()
        
        print(f"✅ Reply: {reply[:60]}...")
        consumer.commit()  # Manual commit after success
        
    except Exception as e:
        print(f"❌ Error: {e}")
        continue

print(f"\n📊 FINAL: Polled {poll_count}, Processed {message_count}")
consumer.close()
producer.close()
