"""
Streamlit Real-Time Trending Dashboard
───────────────────────────────────────
Reads state written by stream_processor.py and renders
an auto-refreshing visual dashboard.

Run with:
    streamlit run src/dashboard.py
"""

import json
import os
import time
from datetime import datetime
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

STATE_FILE = "state.json"

# YouTube category ID → name mapping
CATEGORY_NAMES = {
    "1":  "Film & Animation",
    "2":  "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="YouTube Trending — Live",
    page_icon="📺",
    layout="wide",
)

# ── Load State ────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


# ── Dashboard ─────────────────────────────────────────────────────────────────
def render_dashboard(state: dict):

    st.title("📺 YouTube Trending — Real-Time Dashboard")
    st.caption("Powered by Kafka + YouTube Data API v3. Auto-refreshes every 10 seconds.")

    if not state:
        st.warning("No data yet. Start the stream processor and producer first.")
        return

    # ── Global metrics row ────────────────────────────────────────────────────
    regions = list(state.keys())
    total_videos = sum(state[r]["total_videos_tracked"] for r in regions)
    last_updated = state[regions[0]]["last_updated"] if regions else "N/A"

    col1, col2, col3 = st.columns(3)
    col1.metric("Regions Tracked", len(regions))
    col2.metric("Total Videos Tracked", total_videos)
    col3.metric("Last Updated", last_updated[11:19] + " UTC")

    st.divider()

    # ── One tab per region ────────────────────────────────────────────────────
    tabs = st.tabs([r.upper() for r in sorted(regions)])

    for tab, region in zip(tabs, sorted(regions)):
        data = state[region]

        with tab:
            col_left, col_right = st.columns([3, 2])

            # ── Top Videos Table ──────────────────────────────────────────────
            with col_left:
                st.subheader("🔥 Top Trending Videos")

                top_videos = data.get("top_videos", [])
                if top_videos:
                    df = pd.DataFrame(top_videos)
                    df = df[["title", "channel_title", "views",
                             "likes", "like_ratio", "view_velocity"]]
                    df.columns = ["Title", "Channel", "Views",
                                  "Likes", "Like %", "Velocity"]
                    df["Title"] = df["Title"].str[:45]
                    df["Views"] = df["Views"].apply(lambda x: f"{x:,}")
                    df["Likes"] = df["Likes"].apply(lambda x: f"{x:,}")
                    df["Velocity"] = df["Velocity"].apply(
                        lambda x: f"+{x:,}" if x > 0 else "new"
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)

            # ── Fastest Growing ───────────────────────────────────────────────
            with col_right:
                st.subheader("🚀 Fastest Growing")

                growing = [v for v in data.get("fastest_growing", [])
                          if v["view_velocity"] > 0]

                if growing:
                    for video in growing:
                        st.metric(
                            label=video["title"][:40],
                            value=f"{video['views']:,} views",
                            delta=f"+{video['view_velocity']:,} since last poll",
                        )
                else:
                    st.info("Waiting for second poll to calculate velocity...")

            st.divider()

            col_chart1, col_chart2 = st.columns(2)

            # ── Category Bar Chart ────────────────────────────────────────────
            with col_chart1:
                st.subheader("📊 Category Distribution")

                cats = data.get("category_distribution", {})
                if cats:
                    cat_df = pd.DataFrame([
                        {
                            "Category": CATEGORY_NAMES.get(k, f"Category {k}"),
                            "Videos": v
                        }
                        for k, v in list(cats.items())[:8]
                    ])
                    fig = px.bar(
                        cat_df,
                        x="Videos",
                        y="Category",
                        orientation="h",
                        color="Videos",
                        color_continuous_scale="Reds",
                    )
                    fig.update_layout(
                        height=300,
                        margin=dict(l=0, r=0, t=0, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"category_{region}")

            # ── Views Bar Chart ───────────────────────────────────────────────
            with col_chart2:
                st.subheader("👁️ Views Comparison")

                if top_videos:
                    views_df = pd.DataFrame([
                        {
                            "Title": v["title"][:25] + "...",
                            "Views": v["views"],
                            "Velocity": v["view_velocity"],
                        }
                        for v in top_videos[:8]
                    ])
                    fig2 = px.bar(
                        views_df,
                        x="Views",
                        y="Title",
                        orientation="h",
                        color="Velocity",
                        color_continuous_scale="Blues",
                    )
                    fig2.update_layout(
                        height=300,
                        margin=dict(l=0, r=0, t=0, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig2, use_container_width=True, key=f"views_{region}")


# ── Auto-refresh loop ─────────────────────────────────────────────────────────
def main():
    state = load_state()
    render_dashboard(state)

    # Auto-refresh every 10 seconds
    time.sleep(10)
    st.rerun()


if __name__ == "__main__":
    main()
