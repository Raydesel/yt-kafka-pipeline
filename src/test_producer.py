from kafka import KafkaProducer
import json

# Connect to your local Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send a test message
message = {
    "video_id": "abc123",
    "title": "Test Video from Python",
    "channel_title": "My Channel",
    "views": 999999,
    "likes": 12000,
    "region": "us",
    "trending_date": "2026-05-05",
    "fetched_at": "2026-05-05T03:00:00Z"
}

producer.send('raw_video_events', value=message)
producer.flush()  # make sure message is actually sent before script exits

print("Message sent successfully")
