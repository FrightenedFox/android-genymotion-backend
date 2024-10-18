from typing import List

from domain import GameModel, SessionModel, VideoModel
from fastapi import BackgroundTasks, FastAPI, HTTPException
from mangum import Mangum
from schemas import CreateGameRequest, CreateSessionRequest, CreateVideoRequest, Game, Session, Video

app = FastAPI()

# Initialize domain models
session_model = SessionModel()
game_model = GameModel()
video_model = VideoModel()


# Session endpoints
@app.post("/sessions", response_model=Session)
def create_session(request: CreateSessionRequest) -> Session:
    """
    Create a new session.
    """
    try:
        session = session_model.create_session(
            user_ip=request.user_ip,
            browser_info=request.browser_info,
        )
        return session
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}", response_model=Session)
def get_session(session_id: str) -> Session:
    """
    Retrieve a session by its ID.
    """
    try:
        # Update the instance state before returning the session
        session_model.update_instance_info(session_id)
        item = session_model.get_item_by_id(session_id)
        session_model.update_last_accessed(session_id)
        if not item:
            raise HTTPException(status_code=404, detail="Session not found")
        return item
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions", response_model=List[Session])
def get_all_sessions() -> List[Session]:
    """
    Retrieve all sessions, updating their instance states.
    """
    try:
        sessions = session_model.get_all_sessions_with_updated_info()
        return sessions
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


@app.post("/sessions/end-all-active")
def end_all_active_sessions(background_tasks: BackgroundTasks) -> dict:
    """
    End all sessions that have an active instance running.
    """
    try:
        session_model.end_all_active_sessions(background_tasks)
        return {"message": "All active sessions have been queued for termination."}
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
        version=request.version,
        apk_s3_path=request.apk_s3_path,
    )
    return game


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
        session_id=request.session_id,
        game_id=request.game_id,
        s3_path=request.s3_path,
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
