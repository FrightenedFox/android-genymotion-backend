from typing import Literal, Optional

from pydantic import BaseModel


class InstanceInfo(BaseModel):
    instance_id: str
    instance_type: str


class CompleteInstanceInfo(InstanceInfo):
    instance_state: str
    instance_ip: Optional[str]
    instance_aws_address: Optional[str]


class Session(BaseModel):
    PK: str = "SESSION"  # Partition key
    SK: str  # Session ID (KSUID)
    instance: Optional[InstanceInfo | CompleteInstanceInfo]
    ami_id: str = "ami-0f608f5544f94803b"
    ssl_configured: bool = False
    user_ip: Optional[str] = None
    browser_info: Optional[str] = None
    start_time: str
    end_time: Optional[str] = None


class SessionPing(BaseModel):
    PK: str = "SESSION#PING"  # Partition key
    SK: str  # Session ID (KSUID)
    instance_active: bool
    last_accessed_on: str
    scheduled_for_deletion: bool = False


class SessionWithPing(Session, SessionPing):
    PK: str = "SESSION"
    instance_active: Optional[bool] = None
    last_accessed_on: Optional[str] = None
    scheduled_for_deletion: Optional[bool] = None


# Schema for AMI entity
class AMI(BaseModel):
    PK: str = "AMI"  # Partition key
    SK: str  # Sort key (UUID)
    representing_year: int
    instance_type: str
    disk_size: int
    android_version: str
    screen_width: int
    screen_height: int


# Schema for Game entity
class Game(BaseModel):
    PK: str = "GAME"  # Partition key
    SK: str  # Sort key (UUID)
    name: str
    game_version: str
    android_package_name: str
    apk_s3_path: Optional[str] = None
    wifi_enabled: bool = True
    screen_orientation: Literal["horizontal", "vertical"] = "vertical"
    GSI1PK: Optional[str]
    GSI1SK: Optional[str]


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
    GSI1PK: Optional[str]
    GSI1SK: Optional[str]
    GSI2PK: Optional[str]
    GSI2SK: Optional[str]


# Request schemas for API endpoints
class CreateSessionRequest(BaseModel):
    user_ip: Optional[str] = None
    browser_info: Optional[str] = None


class CreateGameRequest(BaseModel):
    name: str
    game_version: str
    android_package_name: str
    apk_s3_path: Optional[str] = None
    ami_id: Optional[str] = None
    wifi_enabled: bool = True
    screen_orientation: Literal["horizontal", "vertical"] = "vertical"


class CreateVideoRequest(BaseModel):
    video_id: str
    session_id: str
    game_id: str
    duration: Optional[int] = None
    size: Optional[int] = None


class CreateAMIRequest(BaseModel):
    ami_id: str
    representing_year: int
    instance_type: str
    disk_size: int
    android_version: str
    screen_width: int
    screen_height: int
