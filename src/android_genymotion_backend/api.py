import random
from typing import List

from fastapi import FastAPI, HTTPException
from mangum import Mangum

from android_genymotion_backend.schemas import SessionPing
from application_manager import ApplicationManager
from domain import GameModel, SessionModel, VideoModel, AMIModel, VcpuLimitExceededException
from schemas import (
    CreateGameRequest,
    CreateSessionRequest,
    CreateVideoRequest,
    Game,
    Session,
    Video,
    AMI,
    CreateAMIRequest,
)

app = FastAPI()

# Initialize domain models
session_model = SessionModel()
game_model = GameModel()
video_model = VideoModel()
ami_model = AMIModel()
app_manager = ApplicationManager()


@app.get("/sessions", response_model=List[Session])
def get_all_sessions(only_active: bool = False) -> List[Session]:
    """
    Retrieve all sessions, updating their instance states.

    Args:
        only_active (bool): If True, only return active sessions where the instance is running.
    """
    try:
        sessions = session_model.get_all_sessions_with_updated_info(only_active=only_active)
        return sessions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/random", response_model=Session)
def create_session(request: CreateSessionRequest) -> Session:
    try:
        amis_list = ami_model.get_all_items()
        if not amis_list:
            raise HTTPException(status_code=404, detail="No AMIs found")
        ami_id = random.choice(amis_list).SK

        session = session_model.create_session(
            ami_id=ami_id,
            user_ip=request.user_ip,
            browser_info=request.browser_info,
        )
        return session
    except VcpuLimitExceededException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/list-all-inactive", response_model=List[Session])
def list_all_inactive_sessions() -> List[Session]:
    """
    Retrieve all sessions that are not active (i.e., the instance is not running).
    """
    try:
        sessions = session_model.get_inactive_sessions()
        return sessions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/end-all-running")
def end_all_active_sessions() -> dict:
    """
    End all sessions that have an active instance running.
    """
    try:
        session_model.end_all_running_sessions()
        return {"message": "All active sessions have been queued for termination."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{year}", response_model=Session)
def create_session(year: int, request: CreateSessionRequest) -> Session:
    """
    Create a new session.
    """
    try:
        # Select AMI for the given year
        amis_list = ami_model.get_all_items()
        if not amis_list:
            raise HTTPException(status_code=404, detail="No AMIs found")
        ami_id = None
        for ami in amis_list:
            if ami.representing_year == year:
                ami_id = ami.SK
                break
        if not ami_id:
            raise HTTPException(status_code=404, detail=f"No AMI found for year {year}")

        session = session_model.create_session(
            ami_id=ami_id,
            user_ip=request.user_ip,
            browser_info=request.browser_info,
        )
        return session
    except VcpuLimitExceededException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}", response_model=Session)
def get_session(session_id: str) -> Session:
    """
    Retrieve a session by its ID.
    """
    try:
        # Update the instance state before returning the session
        item = session_model.get_session_by_id(session_id)
        if item:
            session_model.session_ping_model.update_last_accessed(session_id)
            return item
        else:
            raise HTTPException(status_code=404, detail="Session not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/ping", response_model=SessionPing)
def get_session(session_id: str) -> SessionPing:
    """
    Get the session ping by its ID.
    """
    try:
        item = session_model.session_ping_model.get_item_by_id(session_id)
        if item:
            session_model.session_ping_model.update_last_accessed(session_id, instance_active=item.instance_active)
            return item
        else:
            raise HTTPException(status_code=404, detail="Session not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/end")
def end_session(session_id: str) -> dict:
    """
    End a session by updating its end time and terminating the instance.
    """
    try:
        session_model.end_session(session_id)
        return {"message": f"Session {session_id} ended."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/kiosk")
def set_kiosk(session_id: str, enabled: bool):
    """
    Enable or disable kiosk mode in the specified session.
    """
    try:
        app_manager.set_kiosk_mode(session_id, enabled)
        return {"message": f"Kiosk mode {'enabled' if enabled else 'disabled'} in session {session_id}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/internet")
def set_internet(session_id: str, enabled: bool):
    """
    Enable or disable internet access in the specified session.
    """
    try:
        app_manager.set_internet_access(session_id, enabled)
        return {"message": f"Internet access {'enabled' if enabled else 'disabled'} in session {session_id}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/cleanup")
def cleanup_session(session_id: str):
    """
    Clean up the specified session.
    """
    try:
        app_manager.cleanup_session(session_id)
        return {"message": f"Session {session_id} cleaned up."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/upload-recordings")
def upload_recordings(session_id: str):
    """
    Upload all recordings for the session.
    """
    try:
        app_manager.upload_all_recordings_to_s3(session_id)
        return {"message": f"Recordings uploaded for session {session_id}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/games/stop")
def stop_game(session_id: str):
    """
    Stop the game in the specified session.
    """
    try:
        app_manager.stop_game_in_session(session_id)
        return {"message": f"Game stopped in session {session_id}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/games/{game_id}/start")
def start_game(session_id: str, game_id: str):
    """
    Start a game in the specified session.
    """
    try:
        app_manager.start_game_in_session(session_id, game_id)
        return {"message": f"Game {game_id} started in session {session_id}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# AMI endpoints
@app.post("/amis", response_model=AMI)
def create_ami(request: CreateAMIRequest) -> AMI:
    """
    Create a new AMI entry.
    """
    try:
        ami = ami_model.create_ami(
            ami_id=request.ami_id,
            representing_year=request.representing_year,
            instance_type=request.instance_type,
            disk_size=request.disk_size,
            android_version=request.android_version,
            screen_width=request.screen_width,
            screen_height=request.screen_height,
        )
        return ami
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/amis", response_model=List[AMI])
def get_all_amis() -> List[AMI]:
    """
    Retrieve all AMIs.
    """
    try:
        amis = ami_model.list_all_amis()
        return amis
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/amis/recommended")
def get_recommended_ami():
    """
    Retrieves the AMI with the lowest number of videos recorded for its assigned games.
    """
    try:
        ami = ami_model.get_recommended_ami()
        if not ami:
            raise HTTPException(status_code=404, detail="No AMI found.")
        return ami
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/amis/{ami_id}", response_model=AMI)
def get_ami(ami_id: str) -> AMI:
    """
    Retrieve an AMI by its ID.
    """
    try:
        ami = ami_model.get_ami_by_id(ami_id)
        if not ami:
            raise HTTPException(status_code=404, detail="AMI not found")
        return ami
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Game endpoints
@app.get("/games", response_model=List[Game])
def get_all_games() -> List[Game]:
    """
    Retrieve all games.
    """
    items = game_model.get_all_items()
    return items


@app.get("/games/{game_id}", response_model=Game)
def get_game(game_id: str) -> Game:
    """
    Retrieve a game by its ID.
    """
    item = game_model.get_item_by_id(game_id)
    if not item:
        raise HTTPException(status_code=404, detail="Game not found")
    return item


@app.post("/games", response_model=Game)
def create_game(request: CreateGameRequest) -> Game:
    """
    Create a new game entry.
    """
    game = game_model.create_game(
        name=request.name,
        version=request.game_version,
        apk_s3_path=request.apk_s3_path,
        ami_id=request.ami_id,
        android_package_name=request.android_package_name,
        wifi_enabled=request.wifi_enabled,
        screen_orientation=request.screen_orientation,
    )
    return game


@app.get("/games/ami/{ami_id}", response_model=List[Game])
def get_games_by_ami_id(ami_id: str) -> List[Game]:
    """
    Retrieve all games associated with a specific AMI ID.
    """
    try:
        games = game_model.get_games_by_ami_id(ami_id)
        return games
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/games/ami/{ami_id}/recommended")
def get_recommended_game_for_ami(ami_id: str):
    """
    Retrieves the game assigned to the specified AMI with the lowest number of videos.
    """
    try:
        game = game_model.get_recommended_game_for_ami(ami_id)
        if not game:
            raise HTTPException(status_code=404, detail="No recommended game found for the specified AMI.")
        return game
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Video endpoints
@app.get("/videos", response_model=List[Video])
def get_all_videos() -> List[Video]:
    """
    Retrieve all videos.
    """
    items = video_model.get_all_items()
    return items


@app.get("/videos/{video_id}", response_model=Video)
def get_video(video_id: str) -> Video:
    """
    Retrieve a video by its ID.
    """
    item = video_model.get_item_by_id(video_id)
    if not item:
        raise HTTPException(status_code=404, detail="Video not found")
    return item


@app.post("/videos", response_model=Video)
def create_video(request: CreateVideoRequest) -> Video:
    """
    Create a new video entry.
    """
    video = video_model.create_video(
        video_id=request.video_id,
        session_id=request.session_id,
        game_id=request.game_id,
        duration=request.duration,
        size=request.size,
    )
    return video


# Endpoint to get all videos for a given session
@app.get("/videos/session/{session_id}", response_model=List[Video])
def get_videos_by_session(session_id: str) -> List[Video]:
    """
    Retrieve all videos associated with a specific session ID.
    """
    items = video_model.get_videos_by_session_id(session_id)
    return items


# Endpoint to get all videos for a given game
@app.get("/videos/game/{game_id}", response_model=List[Video])
def get_videos_by_game(game_id: str) -> List[Video]:
    """
    Retrieve all videos associated with a specific game ID.
    """
    items = video_model.get_videos_by_game_id(game_id)
    return items


handler = Mangum(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
