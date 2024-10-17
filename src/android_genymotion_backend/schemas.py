from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


# Schema for Session entity
class Session(BaseModel):
    PK: str = "SESSION"  # Partition key
    SK: str  # Sort key (KSUID)
    instance_id: str
    user_ip: Optional[str] = None
    browser_info: Optional[str] = None
    start_time: datetime
    end_time: Optional[datetime] = None


# Schema for Game entity
class Game(BaseModel):
    PK: str = "GAME"  # Partition key
    SK: str  # Sort key (UUID)
    name: str
    version: str
    apk_s3_path: str


# Schema for Video entity
class Video(BaseModel):
    PK: str = "VIDEO"  # Partition key
    SK: str  # Sort key (KSUID)
    session_id: str
    game_id: str
    s3_path: str
    duration: Optional[int] = None  # Duration in seconds
    size: Optional[int] = None  # Size in bytes
    timestamp: datetime


# Request schemas for API endpoints
class LaunchInstanceRequest(BaseModel):
    ami_id: str
    instance_type: str = "t2.micro"
    key_name: Optional[str] = None
    security_group_ids: Optional[List[str]] = None
    subnet_id: Optional[str] = None
    min_count: int = 1
    max_count: int = 1


# Schemas for API requests
class CreateSessionRequest(BaseModel):
    instance_id: str
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
