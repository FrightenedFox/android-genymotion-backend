import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
from typing import List, Optional, TypeVar, Generic, Dict, Any

from schemas import Session, Game, Video
from fastapi.encoders import jsonable_encoder
from ksuid import ksuid

dynamodb = boto3.resource('dynamodb')

# Define a TypeVar for the item type
T = TypeVar('T')

# Base class for DynamoDB interactions
class DynamoDBModel(Generic[T]):
    table_name: str
    partition_key_name: str
    partition_key_value: str
    sort_key_name: str

    def __init__(self) -> None:
        self.table = dynamodb.Table(self.table_name)

    def get_all_items(self) -> List[T]:
        response = self.table.query(
            KeyConditionExpression=Key(self.partition_key_name).eq(self.partition_key_value)
        )
        items = response.get('Items', [])
        return [self._deserialize(item) for item in items]

    def get_item_by_id(self, item_id: str) -> Optional[T]:
        response = self.table.get_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: item_id
            }
        )
        item = response.get('Item')
        if item:
            return self._deserialize(item)
        return None

    def create_item(self, item_data: T) -> T:
        serialized_item = self._serialize(item_data)
        self.table.put_item(Item=serialized_item)
        return item_data

    def _serialize(self, item: T) -> Dict[str, Any]:
        return jsonable_encoder(item)

    def _deserialize(self, data: Dict[str, Any]) -> T:
        raise NotImplementedError("Subclasses must implement _deserialize method.")

# Session domain class
class SessionModel(DynamoDBModel[Session]):
    table_name: str = 'YourTableName'  # Replace with your DynamoDB table name
    partition_key_name: str = 'entity_type'
    partition_key_value: str = 'Session'
    sort_key_name: str = 'session_id'

    def _deserialize(self, data: Dict[str, Any]) -> Session:
        return Session(**data)

    def create_session(
        self,
        instance_id: str,
        user_ip: Optional[str],
        browser_info: Optional[str]
    ) -> Session:
        session_id = ksuid().__str__()
        session = Session(
            entity_type=self.partition_key_value,
            session_id=session_id,
            instance_id=instance_id,
            user_ip=user_ip,
            browser_info=browser_info,
            start_time=datetime.now(),
        )
        self.create_item(session)
        return session

    def end_session(self, session_id: str) -> None:
        self.table.update_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: session_id
            },
            UpdateExpression='SET end_time = :end_time',
            ExpressionAttributeValues={
                ':end_time': datetime.now().isoformat()
            }
        )

# Game domain class
class GameModel(DynamoDBModel[Game]):
    table_name: str = 'YourTableName'  # Replace with your DynamoDB table name
    partition_key_name: str = 'entity_type'
    partition_key_value: str = 'Game'
    sort_key_name: str = 'game_id'

    def _deserialize(self, data: Dict[str, Any]) -> Game:
        return Game(**data)

    def create_game(
        self,
        name: str,
        version: str,
        apk_s3_path: str
    ) -> Game:
        game_id = ksuid().__str__()
        game = Game(
            entity_type=self.partition_key_value,
            game_id=game_id,
            name=name,
            version=version,
            apk_s3_path=apk_s3_path,
        )
        self.create_item(game)
        return game

# Video domain class
class VideoModel(DynamoDBModel[Video]):
    table_name: str = 'YourTableName'  # Replace with your DynamoDB table name
    partition_key_name: str = 'entity_type'
    partition_key_value: str = 'Video'
    sort_key_name: str = 'video_id'

    def _deserialize(self, data: Dict[str, Any]) -> Video:
        return Video(**data)

    def create_video(
        self,
        session_id: str,
        game_id: str,
        s3_path: str,
        duration: Optional[int],
        size: Optional[int]
    ) -> Video:
        video_id = ksuid().__str__()
        video = Video(
            entity_type=self.partition_key_value,
            video_id=video_id,
            session_id=session_id,
            game_id=game_id,
            s3_path=s3_path,
            duration=duration,
            size=size,
            timestamp=datetime.now(),
        )
        self.create_item(video)
        return video

    def get_videos_by_session_id(self, session_id: str) -> List[Video]:
        response = self.table.query(
            IndexName='session_id-index',
            KeyConditionExpression=Key('session_id').eq(session_id)
        )
        items = response.get('Items', [])
        return [self._deserialize(item) for item in items]

    def get_videos_by_game_id(self, game_id: str) -> List[Video]:
        response = self.table.query(
            IndexName='game_id-index',
            KeyConditionExpression=Key('game_id').eq(game_id)
        )
        items = response.get('Items', [])
        return [self._deserialize(item) for item in items]
