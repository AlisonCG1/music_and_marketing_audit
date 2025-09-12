import streamlit as st
import plotly.express as px
from data import load_all_data
import pandas as pd

st.set_page_config(page_title="Music & Marketing Data Explorer", layout="wide")

bronze_dfs, silver_dfs, gold_dfs = load_all_data()

st.title("Music & Marketing Data Explorer ✨🎹🎶")

st.markdown(
    """
    <style>
        div.block-container { padding-top: 1rem; }
    </style>
    """,
    unsafe_allow_html=True
)

tab1, tab2, tab3 = st.tabs(["Overall Data", "Insights", "Aggregated Insight"])

with tab1:
    st.header("🥉 Bronze Layer - Overall Data")

    bronze_table_names = [
        "youtube_clean_bronze",
        "spotify_tracks_bronze",
        "spotify_search_bronze",
    ]

    selected_table = st.selectbox(
        "📂 Select a Bronze Table to Explore",
        options=bronze_table_names
    )

    if selected_table in silver_dfs:
        df = silver_dfs[selected_table]

        if "ingested_at" in df.columns:
            df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")
            df["ingested_at_formatted"] = df["ingested_at"].dt.strftime("%Y-%m-%d %H:%M")

        with st.expander(f"🔎 Preview {selected_table}"):
            st.dataframe(df.head(20), width="stretch")
            st.caption(f"Total rows: {len(df)}")

        st.subheader(f"Insights from {selected_table}")

        if selected_table == "youtube_clean_bronze":
            figs = []


            if "genre" in df.columns:
                genre_counts = df["genre"].value_counts().head(10).reset_index()
                genre_counts.columns = ["genre", "count"]
                figs.append(px.pie(
                    genre_counts, names="genre", values="count",
                    title="🎶 Top 10 YouTube Genres",
                    color_discrete_sequence=px.colors.qualitative.Pastel
                ))

            if "channel_title" in df.columns:
                top_channels = df["channel_title"].value_counts().head(10).reset_index()
                top_channels.columns = ["channel_title", "count"]
                figs.append(px.bar(
                    top_channels, x="channel_title", y="count",
                    title="📺 Top 10 YouTube Channels",
                    text_auto=True,
                    color="channel_title",
                    color_discrete_sequence=px.colors.qualitative.Bold
                ))


            metrics = ["likes", "comment_count", "views"]
            metrics_existing = [m for m in metrics if m in df.columns]
            if metrics_existing:
                df_metrics = df[metrics_existing].sum().reset_index()
                df_metrics.columns = ["metric", "total"]
                figs.append(px.bar(
                    df_metrics, x="metric", y="total",
                    title="👍 Total Likes, Comments, and Views",
                    text_auto=True,
                    color="metric",
                    color_discrete_sequence=px.colors.qualitative.Vivid
                ))

    
            if "published_at" in df.columns:
                df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
                publish_counts = df["published_at"].dt.year.value_counts().sort_index().reset_index()
                publish_counts.columns = ["year", "count"]
                figs.append(px.line(
                    publish_counts, x="year", y="count",
                    title="📅 Videos Published Over Time",
                    markers=True,
                    color_discrete_sequence=[px.colors.sequential.Inferno[-1]]
                ))

           
            for i in range(0, len(figs), 2):
                cols = st.columns(2)
                cols[0].plotly_chart(figs[i], use_container_width=True)
                if i + 1 < len(figs):
                    cols[1].plotly_chart(figs[i + 1], use_container_width=True)


        elif selected_table == "spotify_tracks_bronze":
            if "artist_name" in df.columns:
                top_artists = df["artist_name"].value_counts().head(10).reset_index()
                top_artists.columns = ["artist_name", "count"]
                fig = px.bar(
                    top_artists, x="artist_name", y="count",
                    title="🎤 Top 10 Spotify Artists",
                    text_auto=True,
                    color="artist_name",
                    color_discrete_sequence=px.colors.qualitative.Bold
                )
                st.plotly_chart(fig, use_container_width=True)

            if "duration_seconds" in df.columns:
                fig = px.histogram(
                    df, x="duration_seconds", nbins=50,
                    title="⏱️ Track Duration Distribution",
                    color_discrete_sequence=px.colors.sequential.Plasma
                )
                st.plotly_chart(fig, use_container_width=True)

            if "release_date" in df.columns:
                df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
                release_counts = df["release_date"].dt.year.value_counts().sort_index().reset_index()
                release_counts.columns = ["year", "count"]
                fig = px.line(
                    release_counts, x="year", y="count",
                    title="📅 Tracks Released Over Time",
                    markers=True,
                    color_discrete_sequence=[px.colors.sequential.Viridis[-1]]
                )
                st.plotly_chart(fig, use_container_width=True)


        elif selected_table == "spotify_search_bronze":
            if "artist_name" in df.columns:
                top_artists = df["artist_name"].value_counts().head(10).reset_index()
                top_artists.columns = ["artist_name", "count"]
                fig = px.bar(
                    top_artists, x="artist_name", y="count",
                    title="🔍 Most Searched Artists",
                    text_auto=True,
                    color="artist_name",
                    color_discrete_sequence=px.colors.qualitative.Prism
                )
                st.plotly_chart(fig, use_container_width=True)

            if "total_tracks" in df.columns:
                fig = px.histogram(
                    df, x="total_tracks", nbins=20,
                    title="🎵 Number of Tracks per Search Query",
                    color_discrete_sequence=px.colors.sequential.Magenta
                )
                st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header(" Insights")
    df = bronze_dfs["alldata_silver"]

    available_genres = df["youtube_genre"].dropna().unique()
    selected_genres = st.multiselect(
        "🎶 Select Genres",
        options=sorted(available_genres),
        default=[],
        help="Filter visualizations by one or more genres"
    )

    if selected_genres:
        df = df[df["youtube_genre"].isin(selected_genres)]

    with st.expander(" Preview Filtered alldata_silver"):
        st.dataframe(df.head(20).style.background_gradient(cmap="Blues"), width="stretch")
        st.caption(f"Total rows after filtering: {len(df)}")

    # Silver visualizations
    col1, col2 = st.columns(2)
    if "youtube_genre" in df.columns and "trend_score" in df.columns:
        genre_trends = df.groupby("youtube_genre", as_index=False)["trend_score"].mean()
        fig1 = px.bar(
            genre_trends, x="youtube_genre", y="trend_score",
            title=" Avg Trend Score by Genre",
            color="trend_score",
            color_continuous_scale=px.colors.sequential.Inferno
        )
        col1.plotly_chart(fig1, use_container_width=True)

    if "artist_name" in df.columns and "youtube_engagement_rate" in df.columns:
        artist_perf = df.groupby("artist_name", as_index=False)["youtube_engagement_rate"].mean()\
            .sort_values("youtube_engagement_rate", ascending=False).head(15)
        fig2 = px.bar(
            artist_perf, x="artist_name", y="youtube_engagement_rate",
            title=" Top Artists by Engagement Rate",
            color="youtube_engagement_rate",
            color_continuous_scale=px.colors.sequential.Plasma
        )
        col2.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        df["release_year"] = df["release_date"].dt.year
        yearly_trend = df.groupby("release_year", as_index=False)["trend_score"].mean()
        fig3 = px.line(
            yearly_trend, x="release_year", y="trend_score",
            title="📈 Trend Score by Release Year",
            markers=True,
            color_discrete_sequence=px.colors.sequential.Magma
        )
        col3.plotly_chart(fig3, use_container_width=True)

    if "virality_potential" in df.columns and "youtube_genre" in df.columns:
        fig4 = px.histogram(
            df, x="virality_potential", color="youtube_genre",
            title="🔥 Virality Potential by Genre",
            barmode="group",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        col4.plotly_chart(fig4, use_container_width=True)

    col5, _, col6 = st.columns([2, 0.5, 2])
    if "duration_seconds" in df.columns and "youtube_engagement_rate" in df.columns:
        fig5 = px.scatter(
            df, x="duration_seconds", y="youtube_engagement_rate",
            color="youtube_genre",
            hover_data=["track_title", "artist_name"],
            title="🎵 Duration vs Engagement Rate",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        col5.plotly_chart(fig5, use_container_width=True)

# ---------------- TAB 3: Gold Insights ----------------
with tab3:
    st.header("🥇 Gold Layer - Insights")

    if "artist_performance_gold" in gold_dfs:
        df_artist = gold_dfs["artist_performance_gold"]
        with st.expander("🔎 Preview artist_performance_gold"):
            st.dataframe(df_artist.head(20), width="stretch")
            st.caption(f"Total rows: {len(df_artist)}")

        st.subheader("📊 Artist Performance Insights")
        fig1 = px.bar(
            df_artist.sort_values("avg_trend_score", ascending=False).head(15),
            x="artist_name", y="avg_trend_score",
            title="🏆 Top Artists by Avg Trend Score",
            text_auto=True,
            color="avg_trend_score",
            color_continuous_scale=px.colors.sequential.Plasma
        )

        fig2 = px.scatter(
            df_artist, x="track_count", y="avg_engagement",
            color="artist_name", hover_data=["artist_name"],
            title="🎵 Track Count vs Avg Engagement",
            color_discrete_sequence=px.colors.qualitative.Bold
        )

        col1, col2 = st.columns(2)
        col1.plotly_chart(fig1, use_container_width=True)
        col2.plotly_chart(fig2, use_container_width=True)

    if "genre_trends_gold" in gold_dfs:
        df_genre = gold_dfs["genre_trends_gold"]
        with st.expander("🔎 Preview genre_trends_gold"):
            st.dataframe(df_genre.head(20), width="stretch")
            st.caption(f"Total rows: {len(df_genre)}")

        st.subheader("🎧 Genre Trends Insights")
        fig3 = px.bar(
            df_genre, x="genre", y="avg_engagement",
            color="genre",
            text="avg_engagement",
            title="Avg Engagement by Genre",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig3.update_layout(showlegend=False)

        fig4 = px.bar(
            df_genre, x="genre", y="avg_trend_score",
            color="genre",
            text="avg_trend_score",
            title="Avg Trend Score by Genre",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig4.update_layout(showlegend=False)

        col3, col4 = st.columns(2)
        col3.plotly_chart(fig3, use_container_width=True)
        col4.plotly_chart(fig4, use_container_width=True)

        fig5 = px.scatter(
            df_genre, x="avg_engagement", y="avg_trend_score",
            color="genre", size="avg_engagement",
            hover_data=["genre", "month"],
            title="Genre: Engagement vs Trend Score",
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        st.plotly_chart(fig5, use_container_width=True)
