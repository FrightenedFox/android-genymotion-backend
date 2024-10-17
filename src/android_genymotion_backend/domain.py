import uuid
from ksuid import ksuid

import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from typing import List, Optional, TypeVar, Generic, Dict, Any

from schemas import Session, Game, Video
from fastapi.encoders import jsonable_encoder

dynamodb = boto3.resource('dynamodb')

# Define a TypeVar for the item type
T = TypeVar("T")


# Base class for DynamoDB interactions
class DynamoDBModel(Generic[T]):
    table_name: str = "android-project-db"
    partition_key_name: str = "PK"
    partition_key_value: str
    sort_key_name: str = "SK"
    gsi1_name: str = "GSI1"  # Name of the first Global Secondary Index
    gsi1pk_name: str = "GSI1PK"
    gsi1pk_value: str
    gsi1sk_name: str = "GSI1SK"
    gsi2_name: str = "GSI2"  # Name of the second Global Secondary Index
    gsi2pk_name: str = "GSI2PK"
    gsi2pk_value: str
    gsi2sk_name: str = "GSI2SK"

    def __init__(self) -> None:
        self.table = dynamodb.Table(self.table_name)

    def get_all_items(self) -> List[T]:
        response = self.table.query(KeyConditionExpression=Key(self.partition_key_name).eq(self.partition_key_value))
        items = response.get("Items", [])
        return [self._deserialize(item) for item in items]

    def get_item_by_id(self, item_id: str) -> Optional[T]:
        response = self.table.get_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: item_id,
            }
        )
        item = response.get("Item")
        if item:
            return self._deserialize(item)
        return None

    def create_item(self, item_data: T, extra_attributes: Dict[str, Any] = None) -> T:
        serialized_item = self._serialize(item_data)
        if extra_attributes:
            serialized_item.update(extra_attributes)
        self.table.put_item(Item=serialized_item)
        return item_data

    def _serialize(self, item: T) -> Dict[str, Any]:
        return jsonable_encoder(item)

    def _deserialize(self, data: Dict[str, Any]) -> T:
        raise NotImplementedError("Subclasses must implement _deserialize method.")


# Session domain class
class SessionModel(DynamoDBModel[Session]):
    partition_key_value: str = "SESSION"

    def _deserialize(self, data: Dict[str, Any]) -> Session:
        return Session(**data)

    def create_session(self, instance_id: str, user_ip: Optional[str], browser_info: Optional[str]) -> Session:
        session_id = ksuid().__str__()
        session = Session(
            PK=self.partition_key_value,
            SK=session_id,
            instance_id=instance_id,
            user_ip=user_ip,
            browser_info=browser_info,
            start_time=datetime.now().isoformat(),
        )
        self.create_item(session)
        return session

    def end_session(self, session_id: str) -> None:
        self.table.update_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: session_id,
            },
            UpdateExpression="SET end_time = :end_time",
            ExpressionAttributeValues={":end_time": datetime.now().isoformat()},
        )


# Game domain class
class GameModel(DynamoDBModel[Game]):
    partition_key_value: str = "GAME"

    def _deserialize(self, data: Dict[str, Any]) -> Game:
        return Game(**data)

    def create_game(self, name: str, version: str, apk_s3_path: str) -> Game:
        game_id = str(uuid.uuid4())
        game = Game(
            PK=self.partition_key_value,
            SK=game_id,
            name=name,
            version=version,
            apk_s3_path=apk_s3_path,
        )
        self.create_item(game)
        return game


# Video domain class
class VideoModel(DynamoDBModel[Video]):
    partition_key_value: str = "VIDEO"
    gsi1pk_value = "SESSION"
    gsi2pk_value = "GAME"

    def _deserialize(self, data: Dict[str, Any]) -> Video:
        return Video(**data)

    def create_video(
        self,
        session_id: str,
        game_id: str,
        s3_path: str,
        duration: Optional[int],
        size: Optional[int],
    ) -> Video:
        video_id = ksuid().__str__()
        video = Video(
            PK=self.partition_key_value,
            SK=video_id,
            session_id=session_id,
            game_id=game_id,
            s3_path=s3_path,
            duration=duration,
            size=size,
            timestamp=datetime.now(),
        )
        extra_attributes = {
            self.gsi1pk_name: self.gsi1pk_value,
            self.gsi1sk_name: session_id,
            self.gsi2pk_name: self.gsi2pk_value,
            self.gsi2sk_name: game_id,
        }
        self.create_item(video, extra_attributes=extra_attributes)
        return video

    def get_videos_by_session_id(self, session_id: str) -> List[Video]:
        response = self.table.query(
            IndexName=self.gsi1_name,
            KeyConditionExpression=Key(self.gsi1pk_name).eq(self.gsi1pk_value) & Key(self.gsi1sk_name).eq(session_id),
        )
        items = response.get("Items", [])
        return [self._deserialize(item) for item in items]

    def get_videos_by_game_id(self, game_id: str) -> List[Video]:
        response = self.table.query(
            IndexName=self.gsi2_name,
            KeyConditionExpression=Key(self.gsi2pk_name).eq(self.gsi2pk_value) & Key(self.gsi2sk_name).eq(game_id),
        )
        items = response.get("Items", [])
        return [self._deserialize(item) for item in items]
