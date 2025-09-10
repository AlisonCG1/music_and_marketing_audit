import streamlit as st
import plotly.express as px
from data import load_all_data
import pandas as pd

st.set_page_config(page_title="My Dashboard", layout="wide")

bronze_dfs, silver_dfs, gold_dfs = load_all_data()

st.set_page_config(page_title="Music & Marketing", layout="wide")
st.title(" Music & Marketing Data ExplorerExplorer✨🎹🎶")
#st.markdown(',style>div.block-container{padding-top:1rem;}</style>', unsafe_allow_html=True)


tab1, tab2, tab3 = st.tabs(["Overall Data", "Insights", "Aggregated Insight"])


with tab1:
    st.header("Preview Data")

    for name, df in bronze_dfs.items():
        with st.expander(f"📂 {name}"):
            st.dataframe(df.head(20), use_container_width=True)
            st.caption(f"Total rows: {len(df)}")


with tab2:
    st.header("🥈 Silver Tables - Insights")
    df = bronze_dfs["alldata_silver"]  # alldata_silver is in bronze

    # --- Genre filter ---
    available_genres = df["youtube_genre"].dropna().unique()
    selected_genres = st.multiselect(
        "🎶 Select Genres",
        options=sorted(available_genres),
        default=[],
        help="Filter visualizations by one or more genres"
    )

    # Apply genre filter
    if selected_genres:
        df = df[df["youtube_genre"].isin(selected_genres)]

    # --- Preview filtered data ---
    with st.expander("🔎 Preview Filtered alldata_silver"):
        st.dataframe(
            df.head(20).style.background_gradient(cmap="Blues"),
            use_container_width=True
        )
        st.caption(f"Total rows after filtering: {len(df)}")

    # --- Plots ---
    # 1. Top Artists by Virality Potential
    if "artist_name" in df.columns and "virality_potential" in df.columns:
        top_virality = (
            df.groupby(["artist_name", "virality_potential"])
              .size()
              .reset_index(name="count")
              .sort_values("count", ascending=False)
              .head(20)
        )
        fig = px.bar(
            top_virality,
            x="artist_name", y="count", color="virality_potential",
            title="🔥 Top Artists by Virality Potential",
            barmode="stack"
        )
        st.plotly_chart(fig, use_container_width=True)

    # 2. Engagement Trend Over Time
    if "release_date" in df.columns and "youtube_engagement_rate" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        trend_over_time = df.groupby("release_date")["youtube_engagement_rate"].mean().reset_index()
        fig = px.line(
            trend_over_time,
            x="release_date", y="youtube_engagement_rate",
            title="📈 Avg YouTube Engagement Rate Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)

    # 3. Genre Breakdown
    if "youtube_genre" in df.columns:
        genre_counts = df["youtube_genre"].value_counts().head(10).reset_index()
        genre_counts.columns = ["youtube_genre", "count"]
        fig = px.pie(
            genre_counts,
            names="youtube_genre",
            values="count",
            title="🎧 Top 10 Genres in Silver Layer"
        )
        st.plotly_chart(fig, use_container_width=True)

    # 4. Trend Score vs Engagement by Genre
    if "trend_score" in df.columns and "youtube_engagement_rate" in df.columns:
        fig = px.scatter(
            df,
            x="trend_score", y="youtube_engagement_rate",
            color="youtube_genre",
            hover_data=["artist_name"],
            title="⚡ Trend Score vs Engagement Rate by Genre"
        )
        st.plotly_chart(fig, use_container_width=True)


with tab3:
    st.header("🥇 Gold Tables - Final Insights")

    for name, df in gold_dfs.items():
        with st.expander(f"📂 {name}"):
            st.dataframe(df.head(20), use_container_width=True)
            st.caption(f"Total rows: {len(df)}")

    # Artist performance
    if "artist_performance_gold" in gold_dfs:
        df = gold_dfs["artist_performance_gold"]
        if "artist_name" in df.columns and "avg_trend_score" in df.columns:
            top_artists = df.sort_values("avg_trend_score", ascending=False).head(15)
            fig = px.bar(
                top_artists,
                x="artist_name", y="avg_trend_score",
                title="🏆 Top Artists by Avg Trend Score",
                text_auto=True
            )
            st.plotly_chart(fig, use_container_width=True)

    if "genre_trends_gold" in gold_dfs:
        df = gold_dfs["genre_trends_gold"]
        if "youtube_genre" in df.columns and "avg_trend_score" in df.columns:
            fig = px.bar(
                df.sort_values("avg_trend_score", ascending=False).head(10),
                x="youtube_genre", y="avg_trend_score",
                title="🎼 Top Genres by Avg Trend Score",
                text_auto=True
            )
            st.plotly_chart(fig, use_container_width=True)