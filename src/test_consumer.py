from kafka import KafkaConsumer
import json

# Connect and subscribe to the topic
consumer = KafkaConsumer(
    'raw_video_events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # read from the very beginning of the topic
    group_id='test-consumer-group'
)

print("Waiting for messages... (Ctrl+C to stop)")

for message in consumer:
    print(f"Offset: {message.offset}")
    print(f"Partition: {message.partition}")
    print(f"Value: {json.dumps(message.value, indent=2)}")
    print("---")
