"""
Stream Processor — Trending Analytics Consumer
───────────────────────────────────────────────
Reads raw video events from Kafka topic: raw_video_events
Maintains real-time aggregations in memory:
  - Top videos per region (by views)
  - Category distribution per region
  - View velocity (views gained between polls)

Key concepts demonstrated:
  - Consumer groups
  - Offset management
  - Stateful stream processing (in-memory state)
  - Windowed aggregations
"""

import json
import os
import logging
from collections import defaultdict
from datetime import datetime, timezone
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC        = "raw_video_events"
GROUP_ID     = "trending-processor"
STATE_FILE = "state.json"

# ── In-Memory State ──────────────────────────────────────────────────────────
# This is what makes it "stateful" stream processing —
# we keep running aggregations in memory as messages arrive

# Latest snapshot per video per region
# Structure: {region: {video_id: {title, views, likes, ...}}}
latest_videos: dict = defaultdict(dict)

# Previous views snapshot for velocity calculation
# Structure: {region: {video_id: views}}
previous_views: dict = defaultdict(dict)

# Category counts per region
# Structure: {region: {category_id: count}}
category_counts: dict = defaultdict(lambda: defaultdict(int))

# Total messages processed
messages_processed = 0


# ── Processing Functions ─────────────────────────────────────────────────────
def process_message(message: dict):
    """Update in-memory state with a new video event."""
    global messages_processed

    region   = message.get("region", "unknown")
    video_id = message.get("video_id")
    views    = message.get("views", 0)

    if not video_id:
        return

    # Calculate view velocity — how many views gained since last poll
    prev_views = previous_views[region].get(video_id, 0)
    view_velocity = views - prev_views if prev_views > 0 else 0

    # Update latest snapshot
    latest_videos[region][video_id] = {
        "video_id":      video_id,
        "title":         message.get("title", "Unknown"),
        "channel_title": message.get("channel_title", "Unknown"),
        "views":         views,
        "likes":         message.get("likes", 0),
        "like_ratio":    message.get("like_ratio", 0),
        "category_id":   message.get("category_id", "0"),
        "fetched_at":    message.get("fetched_at"),
        "view_velocity": view_velocity,   # ← new: views gained since last poll
    }

    # Update previous views for next velocity calculation
    previous_views[region][video_id] = views

    # Update category distribution
    category_id = message.get("category_id", "unknown")
    category_counts[region][category_id] += 1

    messages_processed += 1


def get_top_videos(region: str, n: int = 5) -> list:
    """Return top N videos for a region sorted by current views."""
    videos = list(latest_videos[region].values())
    return sorted(videos, key=lambda v: v["views"], reverse=True)[:n]


def get_fastest_growing(region: str, n: int = 3) -> list:
    """Return top N videos by view velocity (fastest growing right now)."""
    videos = list(latest_videos[region].values())
    return sorted(videos, key=lambda v: v["view_velocity"], reverse=True)[:n]


def get_category_distribution(region: str) -> dict:
    """Return category counts for a region sorted by frequency."""
    cats = dict(category_counts[region])
    return dict(sorted(cats.items(), key=lambda x: x[1], reverse=True))


def print_dashboard():
    """Print a simple real-time dashboard to the terminal."""
    regions = list(latest_videos.keys())

    if not regions:
        return

    print("\n" + "═" * 60)
    print(f"  REAL-TIME TRENDING DASHBOARD")
    print(f"  Messages processed: {messages_processed}")
    print(f"  Updated: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
    print("═" * 60)

    for region in sorted(regions):
        print(f"\n  📍 Region: {region.upper()}")
        print(f"  {'─' * 50}")

        # Top videos by views
        print(f"  🔥 Top Videos by Views:")
        for i, video in enumerate(get_top_videos(region, 5), 1):
            velocity = video["view_velocity"]
            velocity_str = f"+{velocity:,}" if velocity > 0 else "first seen"
            print(f"    {i}. {video['title'][:40]:<40}")
            print(f"       Views: {video['views']:>12,}  |  Velocity: {velocity_str}")

        # Fastest growing
        growing = get_fastest_growing(region, 3)
        growing = [v for v in growing if v["view_velocity"] > 0]
        if growing:
            print(f"\n  🚀 Fastest Growing:")
            for video in growing:
                print(f"    → {video['title'][:40]:<40} +{video['view_velocity']:,} views")

        # Category distribution
        cats = get_category_distribution(region)
        if cats:
            top_cats = list(cats.items())[:3]
            cats_str = "  |  ".join([f"cat {k}: {v}" for k, v in top_cats])
            print(f"\n  📊 Top Categories: {cats_str}")

    print("═" * 60)

def save_state():
    """Write current in-memory state to a JSON file for the dashboard to read."""
    state = {}

    for region in latest_videos.keys():
        state[region] = {
            "top_videos": get_top_videos(region, 10),
            "fastest_growing": get_fastest_growing(region, 5),
            "category_distribution": get_category_distribution(region),
            "total_videos_tracked": len(latest_videos[region]),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)

# ── Main Consumer Loop ───────────────────────────────────────────────────────
def main():
    logger.info(f"Starting stream processor")
    logger.info(f"Topic: {TOPIC} | Group: {GROUP_ID}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id=GROUP_ID,
        auto_offset_reset="latest",      # ← only process NEW messages from now
        enable_auto_commit=True,         # ← automatically commit offsets
        auto_commit_interval_ms=5000,    # ← commit every 5 seconds
    )

    logger.info("Consumer ready. Waiting for messages...")
    logger.info("Start the producer in another terminal to see data flow")

    batch_count = 0

    try:
        for message in consumer:
            process_message(message.value)
            batch_count += 1

            # Print dashboard every 10 messages
            if batch_count % 10 == 0:
                print_dashboard()
                save_state()
                batch_count = 0

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total messages processed: {messages_processed}")


if __name__ == "__main__":
    main()
