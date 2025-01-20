import time
from datetime import UTC, datetime, timedelta

import boto3
import pandas as pd
import streamlit as st
from domain import AMIModel, GameModel, SessionModel, SessionPingModel, VideoModel

# Set up AWS clients
s3_client = boto3.client("s3")

st.set_page_config(
    page_title="Android Genymotion Dashboard",
    page_icon="📱",
    layout="wide",
)

# Page title and reload button
col1, col2 = st.columns([9, 1])

with col1:
    st.title("Android Genymotion Dashboard")
with col2:
    if st.button("Reload Data"):
        # Clear the session state data
        for key in ["sessions", "session_pings", "games", "videos", "amis", "video_df"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()


def load_data() -> None:
    if "sessions" not in st.session_state:
        session_model = SessionModel()
        st.session_state.sessions = session_model.get_all_sessions_with_updated_info()
    if "session_pings" not in st.session_state:
        session_ping_model = SessionPingModel()
        st.session_state.session_pings = session_ping_model.get_all_items()
    if "games" not in st.session_state:
        game_model = GameModel()
        st.session_state.games = game_model.get_all_items()
    if "videos" not in st.session_state:
        video_model = VideoModel()
        st.session_state.videos = video_model.get_all_items()
    if "amis" not in st.session_state:
        ami_model = AMIModel()
        st.session_state.amis = ami_model.list_all_amis()


def display_additional_statistics() -> None:
    sessions = st.session_state.sessions
    videos = st.session_state.videos

    total_sessions = len(sessions)
    total_videos = len(videos)

    # Calculate delta per last 24 hours
    now = datetime.now(tz=UTC)
    this_week = now - timedelta(days=7)

    sessions_this_week = [s for s in sessions if datetime.fromisoformat(s.start_time) > this_week]
    total_sessions_this_week = len(sessions_this_week)

    videos_this_week = [v for v in videos if datetime.fromisoformat(v.timestamp) > this_week]
    total_videos_this_week = len(videos_this_week)

    # Total unique user IPs
    user_ips = set(s.user_ip for s in sessions if s.user_ip)
    total_user_ips = len(user_ips)

    user_ips_this_week = set(s.user_ip for s in sessions_this_week if s.user_ip)
    total_user_ips_this_week = len(user_ips_this_week)

    # Total running instances
    running_instances = [s for s in sessions if s.instance and s.instance.instance_state == "running"]
    total_running_instances = len(running_instances)

    st.subheader("Database Statistics")

    # Using st.metric for display
    col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
    col1.metric(
        label="Total Sessions",
        value=total_sessions,
        delta=f"{total_sessions_this_week} new this week",
    )
    col2.metric(
        label="Unique User IPs",
        value=total_user_ips,
        delta=f"{total_user_ips_this_week} new this week",
    )
    col3.metric(
        label="Total Videos",
        value=total_videos,
        delta=f"{total_videos_this_week} new this week",
    )
    col4.metric(
        label="Running Instances",
        value=total_running_instances,
    )

    # Additional statistics
    total_video_size = sum(v.size or 0 for v in videos)
    total_video_size_this_week = sum(v.size or 0 for v in videos_this_week)

    # Average video size
    if videos:
        average_video_size = total_video_size / len(videos)
    else:
        average_video_size = 0

    # Session durations
    session_durations = []
    for s in sessions:
        if s.end_time:
            start = datetime.fromisoformat(s.start_time)
            end = datetime.fromisoformat(s.end_time)
            duration = (end - start).total_seconds()
            if duration > 60 * 60 * 12:
                # Ignore sessions longer than 24 hours, that is a bug
                continue
            session_durations.append(duration)
    if session_durations:
        average_session_duration = sum(session_durations) / len(session_durations)
    else:
        average_session_duration = 0

    average_session_duration_this_week = (
        sum(
            (datetime.fromisoformat(s.end_time) - datetime.fromisoformat(s.start_time)).total_seconds()
            for s in sessions_this_week
            if s.end_time
        )
        / len(sessions_this_week)
        if sessions_this_week
        else 0
    )

    st.subheader("Additional Statistics")
    col5.metric(
        label="Total Video Size (GB)",
        value=round(total_video_size / (1024**3), 1),
        delta=f"{round(total_video_size_this_week / (1024**3), 1)}GB recorded this week",
        help=f"Total video size during this week: {round(total_video_size_this_week / (1024**3), 2)} GB",
    )
    col6.metric(
        label="Average Video Size (MB)",
        value=round(average_video_size / (1024**2), 1),
    )
    col7.metric(
        label="Average Session Duration (minutes)",
        value=int(round(average_session_duration / 60)),
        delta=f"{int(round(average_session_duration_this_week / 60))} min during this week",
    )

    # Get AWS billing info
    try:
        ce = boto3.client("ce")
        # Get the total cost for the last 30 days, excluding the current day
        start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
        )
        total_cost = sum(float(day["Total"]["UnblendedCost"]["Amount"]) for day in response["ResultsByTime"])
        col8.metric(label="Total AWS Cost (last 30 days)", value=f"${total_cost:.2f}")
    except Exception as e:
        col8.error(f"Error retrieving AWS billing info: {e}")


def display_running_sessions() -> None:
    sessions = st.session_state.sessions
    session_pings = st.session_state.session_pings
    amis = {ami.SK: ami for ami in st.session_state.amis}

    # Create a mapping from session id to session ping
    session_ping_dict = {ping.SK: ping for ping in session_pings}

    running_sessions = []
    for session in sessions:
        if session.instance and session.instance.instance_state == "running":
            session_ping = session_ping_dict.get(session.SK)
            ami_info = amis.get(session.ami_id)
            session_info = {
                "Session ID": session.SK,
                "Instance ID": session.instance.instance_id,
                "Instance Type": session.instance.instance_type,
                "Instance State": session.instance.instance_state,
                "Instance IP": session.instance.instance_ip,
                "Access URL": f"https://genymotion:{session.instance.instance_id}@{session.domain_name}/",
                "User IP": session.user_ip,
                "Browser Info": session.browser_info,
                "Start Time": session.start_time,
                "Last Accessed": session_ping.last_accessed_on if session_ping else None,
                "Representing Year": str(ami_info.representing_year) if ami_info else None,
                "Android Version": ami_info.android_version if ami_info else None,
            }
            running_sessions.append(session_info)

    if running_sessions:
        df_sessions = pd.DataFrame(running_sessions)
        df_sessions.sort_values(by="Start Time", ascending=False, inplace=True)
        st.subheader("Running Instances")
        st.write(f"Total Running Instances: {len(running_sessions)}")
        st.dataframe(df_sessions, use_container_width=True)
    else:
        st.subheader("No Running Instances")


def display_video_statistics() -> None:
    games = st.session_state.games
    videos = st.session_state.videos
    amis = {ami.SK: ami for ami in st.session_state.amis}

    game_data = []
    for game in games:
        game_videos = [v for v in videos if v.game_id == game.SK]
        total_size = sum(video.size or 0 for video in game_videos)
        ami_info = amis.get(game.GSI1SK)  # game.GSI1SK is the ami_id
        game_info = {
            "Game Name": game.name,
            "Game Version": game.game_version,
            "Representing Year": str(ami_info.representing_year) if ami_info else None,
            "Android Version": ami_info.android_version if ami_info else None,
            "Number of Videos": len(game_videos),
            "Total Size (MB)": round(total_size / (1024 * 1024), 2),
        }
        game_data.append(game_info)

    if game_data:
        df_games = pd.DataFrame(game_data)
        df_games.sort_values(by=["Game Name", "Game Version"], inplace=True, ignore_index=True)
        st.subheader("Video Statistics per Game")
        st.dataframe(df_games, use_container_width=True, height=300, hide_index=True)
    else:
        st.subheader("No Game Data Available")


def display_ami_statistics() -> None:
    amis = st.session_state.amis
    games = st.session_state.games
    videos = st.session_state.videos

    ami_data = []
    for ami in amis:
        games_for_ami = [g for g in games if g.GSI1SK == ami.SK]
        total_videos = 0
        total_size = 0
        for game in games_for_ami:
            game_videos = [v for v in videos if v.game_id == game.SK]
            total_videos += len(game_videos)
            total_size += sum(video.size or 0 for video in game_videos)
        ami_info = {
            "Representing Year": str(ami.representing_year),
            "Android Version": ami.android_version,
            "Number of Videos": total_videos,
            "Total Size (MB)": round(total_size / (1024 * 1024), 2),
        }
        ami_data.append(ami_info)

    if ami_data:
        df_amis = pd.DataFrame(ami_data)
        df_amis.sort_values(by=["Representing Year"], inplace=True)
        st.subheader("Video Statistics per AMI")
        st.dataframe(df_amis, use_container_width=True, height=300, hide_index=True)
    else:
        st.subheader("No AMI Data Available")


def display_video_downloads() -> None:
    videos = st.session_state.videos
    games = {game.SK: game for game in st.session_state.games}
    amis = {ami.SK: ami for ami in st.session_state.amis}
    s3_bucket_name = "android-project"  # Replace with your bucket name

    st.subheader("Available Videos")

    # Initialize or retrieve the dataframe from session state
    if "video_df" not in st.session_state:
        video_data = []
        for video in videos:
            game = games.get(video.game_id)
            ami = amis.get(game.GSI1SK) if game else None
            video_info = {
                "Select": False,
                "Video ID": video.SK,
                "Session ID": video.session_id,
                "Game Name": game.name if game else None,
                "Game Version": game.game_version if game else None,
                "Representing Year": str(ami.representing_year) if ami else None,
                "Android Version": ami.android_version if ami else None,
                "Timestamp": video.timestamp,
                "Size (MB)": round((video.size or 0) / (1024 * 1024), 2),
                "Download Link": "",
            }
            video_data.append(video_info)

        st.session_state.video_df = pd.DataFrame(video_data)
    else:
        # Data already loaded in session_state
        pass

    # Use data editor to display the dataframe
    edited_df = st.data_editor(
        st.session_state.video_df,
        num_rows="fixed",
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select videos to generate download links",
                default=False,
            ),
            "Download Link": st.column_config.LinkColumn(
                "Download Link",
                help="Click the button to generate download links for selected videos",
                width="small",
            ),
            "Session ID": st.column_config.TextColumn(width="small"),
        },
        height=900,
        hide_index=True,
        use_container_width=True,
        key="video_data_editor",
    )

    # Button to generate presigned URLs
    if st.button("Generate Download Links"):
        # Update the session state with the latest 'Select' values
        st.session_state.video_df["Select"] = edited_df["Select"]

        for index, row in st.session_state.video_df.iterrows():
            if row["Select"] and not row["Download Link"]:
                s3_key = f"recordings/{row['Video ID']}.mp4"
                try:
                    presigned_url = s3_client.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": s3_bucket_name, "Key": s3_key},
                        ExpiresIn=3600,  # URL valid for 1 hour
                    )
                    st.session_state.video_df.at[index, "Download Link"] = presigned_url
                except Exception as e:
                    st.error(f"Error generating presigned URL for video {row['Video ID']}: {e}")
        # Re-display the updated data editor
        st.rerun()


def main() -> None:
    load_data()
    display_additional_statistics()
    st.write("Last updated at ", time.strftime("%Y-%m-%d %H:%M:%S"))

    # Display Running Instances
    display_running_sessions()

    # Display Video Statistics per Game and per AMI side by side
    col1, col2 = st.columns(2)
    with col1:
        display_video_statistics()
    with col2:
        display_ami_statistics()

    # Display Video Downloads
    display_video_downloads()


if __name__ == "__main__":
    main()
