"""
YouTube API Producer
────────────────────
Polls the YouTube Trending API every 5 minutes and publishes
raw video events to the Kafka topic: raw_video_events

Each video becomes one Kafka message, keyed by region so that
all videos from the same region always land on the same partition.
"""

import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER      = "localhost:9092"
TOPIC             = "raw_video_events"
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY")
POLL_INTERVAL_SEC = 30  # 5 minutes
REGIONS           = ["US", "GB", "CA", "IN"]
MAX_RESULTS       = 10   # keep it small to save API quota

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ── Producer Setup ───────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),   # ← keys are strings
        acks="all",         # wait for all replicas to confirm (data safety)
        retries=3,          # retry on transient failures
        linger_ms=100,      # wait 100ms to batch messages together
    )


# ── YouTube API ──────────────────────────────────────────────────────────────
def fetch_trending_videos(region: str) -> list:
    """Fetch trending videos for a given region from YouTube Data API v3."""
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "regionCode": region,
        "maxResults": MAX_RESULTS,
        "key": YOUTUBE_API_KEY,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"API error for region {region}: {e}")
        return []


def parse_video(item: dict, region: str, fetched_at: str) -> dict:
    """Flatten the YouTube API response into a clean flat dict."""
    snippet    = item.get("snippet", {})
    statistics = item.get("statistics", {})

    return {
        # Identity
        "video_id":      item.get("id"),
        "region":        region.lower(),
        "fetched_at":    fetched_at,

        # Content
        "title":         snippet.get("title"),
        "channel_id":    snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "category_id":   snippet.get("categoryId"),
        "publish_time":  snippet.get("publishedAt"),
        "tags":          snippet.get("tags", []),
        "description":   snippet.get("description", "")[:200],  # truncate

        # Statistics — default to 0 if missing (API sometimes omits these)
        "views":         int(statistics.get("viewCount",    0)),
        "likes":         int(statistics.get("likeCount",    0)),
        "comment_count": int(statistics.get("commentCount", 0)),

        # Derived
        "like_ratio": round(
            int(statistics.get("likeCount", 0)) /
            max(int(statistics.get("viewCount", 1)), 1) * 100, 4
        ),
    }


# ── Main Poll Loop ───────────────────────────────────────────────────────────
def poll_and_publish(producer: KafkaProducer):
    """Fetch trending videos for all regions and publish to Kafka."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    total_sent = 0

    for region in REGIONS:
        logger.info(f"Fetching trending videos for region: {region}")
        videos = fetch_trending_videos(region)

        if not videos:
            logger.warning(f"No videos returned for {region}")
            continue

        for item in videos:
            message = parse_video(item, region, fetched_at)

            try:
                producer.send(
                    topic=TOPIC,
                    key=region,           # ← key by region for partition ordering
                    value=message,
                )
                total_sent += 1
            except KafkaError as e:
                logger.error(f"Failed to send message: {e}")

        logger.info(f"  Sent {len(videos)} videos for {region}")

    producer.flush()
    logger.info(f"Poll complete. Total messages sent: {total_sent}")


def main():
    logger.info(f"Starting YouTube producer. Poll interval: {POLL_INTERVAL_SEC}s")
    logger.info(f"Regions: {REGIONS}")
    logger.info(f"Topic: {TOPIC}")

    producer = create_producer()

    try:
        while True:
            logger.info("─── Starting poll ───")
            poll_and_publish(producer)
            logger.info(f"Sleeping {POLL_INTERVAL_SEC}s until next poll...")
            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
