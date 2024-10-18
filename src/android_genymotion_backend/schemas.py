# schemas.py

from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

# New Pydantic model for instance info
class InstanceInfo(BaseModel):
    instance_id: str
    instance_type: str
    instance_state: Optional[str]
    instance_ip: Optional[str] = None
    instance_aws_address: Optional[str] = None
    ssl_configured: bool = False
    secure_address: Optional[str] = None

# Schema for Session entity
class Session(BaseModel):
    PK: str = "SESSION"  # Partition key
    SK: str  # Sort key (KSUID)
    instance: Optional[InstanceInfo]
    user_ip: Optional[str] = None
    browser_info: Optional[str] = None
    start_time: str
    end_time: Optional[str] = None
    last_accessed_on: Optional[str] = None


# Schema for Game entity
class Game(BaseModel):
    PK: str = "GAME"  # Partition key
    SK: str  # Sort key (UUID)
    name: str
    version: str
    apk_s3_path: str
    # TODO: game orientation, min version

# Schema for Video entity
class Video(BaseModel):
    PK: str = "VIDEO"  # Partition key
    SK: str  # Sort key (KSUID)
    session_id: str
    game_id: str
    s3_path: str
    duration: Optional[int] = None  # Duration in seconds
    size: Optional[int] = None  # Size in bytes
    timestamp: str  # Store datetime as ISO-formatted string

# Request schemas for API endpoints
class CreateSessionRequest(BaseModel):
    user_ip: Optional[str] = None
    browser_info: Optional[str] = None

class CreateGameRequest(BaseModel):
    name: str
    version: str
    apk_s3_path: str

class CreateVideoRequest(BaseModel):
    session_id: str
    game_id: str
    s3_path: str
    duration: Optional[int] = None
    size: Optional[int] = None
