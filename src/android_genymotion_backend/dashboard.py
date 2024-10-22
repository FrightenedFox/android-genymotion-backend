import time

import boto3
import pandas as pd
import streamlit as st
# from streamlit_autorefresh import st_autorefresh

from domain import SessionModel, GameModel, VideoModel, AMIModel

# Set up AWS clients
s3_client = boto3.client("s3")

st.set_page_config(
    page_title="Android Genymotion Dashboard",
    page_icon="📱",
    layout="wide",
)

st.title("Android Genymotion Real-Time Dashboard")

# # Autorefresh every 30 seconds
# count = st_autorefresh(interval=30 * 1000, limit=None, key="dashboardrefresh")


def display_additional_statistics():
    session_model = SessionModel()
    game_model = GameModel()
    video_model = VideoModel()
    ami_model = AMIModel()

    total_sessions = len(session_model.get_all_items())
    total_games = len(game_model.get_all_items())
    total_videos = len(video_model.get_all_items())
    total_amis = len(ami_model.list_all_amis())

    st.subheader("Database Statistics")

    # Using st.metric for a cool display
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="Total Sessions", value=total_sessions)
    col2.metric(label="Total Games", value=total_games)
    col3.metric(label="Total Videos", value=total_videos)
    col4.metric(label="Total AMIs", value=total_amis)


def display_running_sessions():
    session_model = SessionModel()
    running_sessions = session_model.get_all_sessions_with_updated_info(only_active=True)

    session_data = []
    for session in running_sessions:
        if session.instance and session.instance.instance_state == "running":
            session_info = {
                "Session ID": session.SK,
                "Instance ID": session.instance.instance_id,
                "Instance Type": session.instance.instance_type,
                "Instance State": session.instance.instance_state,
                "Instance IP": session.instance.instance_ip,
                "Secure Address": session_model.domain_name(session.SK),
                "Access URL": (
                    f"https://genymotion:{session.instance.instance_id}@{session_model.domain_name(session.SK)}/"
                ),
            }
            session_data.append(session_info)

    if session_data:
        df_sessions = pd.DataFrame(session_data)
        st.subheader("Running Instances")
        st.write(f"Total Running Instances: {len(session_data)}")
        st.dataframe(df_sessions, use_container_width=True)
    else:
        st.subheader("No Running Instances")


def display_video_statistics():
    game_model = GameModel()
    video_model = VideoModel()
    games = game_model.get_all_items()

    game_data = []
    for game in games:
        game_videos = video_model.get_videos_by_game_id(game.SK)
        total_size = sum(video.size or 0 for video in game_videos)
        game_info = {
            "Game ID": game.SK,
            "Name": game.name,
            "Number of Videos": len(game_videos),
            "Total Size (MB)": round(total_size / (1024 * 1024), 2),
        }
        game_data.append(game_info)

    if game_data:
        df_games = pd.DataFrame(game_data)
        st.subheader("Video Statistics per Game")
        st.dataframe(df_games, use_container_width=True)
    else:
        st.subheader("No Game Data Available")


def display_ami_statistics():
    ami_model = AMIModel()
    game_model = GameModel()
    video_model = VideoModel()
    amis = ami_model.list_all_amis()

    ami_data = []
    for ami in amis:
        games_for_ami = game_model.get_games_by_ami_id(ami.SK)
        total_videos = 0
        total_size = 0
        for game in games_for_ami:
            game_videos = video_model.get_videos_by_game_id(game.SK)
            total_videos += len(game_videos)
            total_size += sum(video.size or 0 for video in game_videos)
        ami_info = {
            "AMI ID": ami.SK,
            "Representing Year": str(ami.representing_year),
            "Number of Videos": total_videos,
            "Total Size (MB)": round(total_size / (1024 * 1024), 2),
        }
        ami_data.append(ami_info)

    if ami_data:
        df_amis = pd.DataFrame(ami_data)
        st.subheader("Video Statistics per AMI")
        st.dataframe(df_amis, use_container_width=True)
    else:
        st.subheader("No AMI Data Available")


def display_video_downloads():
    video_model = VideoModel()
    videos = video_model.get_all_items()
    s3_bucket_name = "android-project"  # Replace with your bucket name

    st.subheader("Available Videos")

    # Initialize or retrieve the dataframe from session state
    if "video_df" not in st.session_state:
        video_data = []
        for video in videos:
            video_info = {
                "Select": False,
                "Video ID": video.SK,
                "Session ID": video.session_id,
                "Game ID": video.game_id,
                "Timestamp": video.timestamp,
                "Size (MB)": round((video.size or 0) / (1024 * 1024), 2),
                "Download Link": "",
            }
            video_data.append(video_info)

        st.session_state.video_df = pd.DataFrame(video_data)
    else:
        # If videos have been updated (new videos added), we may need to refresh the dataframe
        pass  # For now, assume the list of videos doesn't change during the session

    # Use data editor to display the dataframe
    edited_df = st.data_editor(
        st.session_state.video_df,
        num_rows="fixed",
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select", help="Select videos to generate download links", default=False
            ),
            "Download Link": st.column_config.LinkColumn(
                "Download Link", help="Click the button to generate download links for selected videos", width="large"
            ),
        },
        height=900,
        hide_index=True,
        use_container_width=True,
        key="video_data_editor",  # Assign a key to maintain state
    )

    # Update the session state with the latest 'Select' values
    st.session_state.video_df["Select"] = edited_df["Select"]

    # Button to generate presigned URLs
    if st.button("Generate Download Links"):
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


def main():
    display_additional_statistics()
    st.write("Last updated at ", time.strftime("%Y-%m-%d %H:%M:%S"))

    # Separate the rest of the page into two columns
    col_left, col_right = st.columns(2)

    with col_left:
        display_running_sessions()
        display_video_statistics()
        display_ami_statistics()

    with col_right:
        display_video_downloads()


if __name__ == "__main__":
    main()
