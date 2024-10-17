import uuid
from ksuid import ksuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Key
from datetime import datetime
from typing import List, Optional, TypeVar, Generic, Dict, Any

from schemas import Session, Game, Video, InstanceInfo
from fastapi.encoders import jsonable_encoder

import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# typical console logger handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

dynamodb = boto3.resource("dynamodb")

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
        try:
            response = self.table.query(
                KeyConditionExpression=Key(self.partition_key_name).eq(self.partition_key_value)
            )
            items = response.get("Items", [])
            logger.info(f"Retrieved {len(items)} items from {self.partition_key_value}")
            return [self._deserialize(item) for item in items]
        except Exception as e:
            logger.error(f"Error retrieving items: {e}")
            raise

    def get_item_by_id(self, item_id: str) -> Optional[T]:
        try:
            response = self.table.get_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: item_id,
                }
            )
            item = response.get("Item")
            if item:
                logger.info(f"Item {item_id} retrieved from {self.partition_key_value}")
                return self._deserialize(item)
            else:
                logger.warning(f"Item {item_id} not found in {self.partition_key_value}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving item {item_id}: {e}")
            raise

    def create_item(self, item_data: T, extra_attributes: Dict[str, Any] = None) -> T:
        try:
            serialized_item = self._serialize(item_data)
            if extra_attributes:
                serialized_item.update(extra_attributes)
            self.table.put_item(Item=serialized_item)
            logger.info(f"Created item in {self.partition_key_value}: {serialized_item}")
            return item_data
        except Exception as e:
            logger.error(f"Error creating item: {e}")
            raise

    def _serialize(self, item: T) -> Dict[str, Any]:
        return jsonable_encoder(item)

    def _deserialize(self, data: Dict[str, Any]) -> T:
        raise NotImplementedError("Subclasses must implement _deserialize method.")


class InstanceModel:
    def __init__(self):
        self.ec2 = boto3.client("ec2")

    def create_instance(self) -> InstanceInfo:
        try:
            response = self.ec2.run_instances(
                ImageId="ami-0f608f5544f94803b",
                InstanceType="c6g.xlarge",
                KeyName="android-vms-project-europe",
                SecurityGroupIds=["sg-082c79721016868d3"],
                SubnetId="subnet-0a2abcedb92aba9e1",
                MinCount=1,
                MaxCount=1,
            )
            instance = response["Instances"][0]
            instance_id = instance["InstanceId"]
            instance_type = instance["InstanceType"]
            instance_state = instance["State"]["Name"]
            instance_info = InstanceInfo(
                instance_id=instance_id, instance_type=instance_type, instance_state=instance_state
            )
            logger.info(f"Created EC2 instance {instance_id} with state {instance_state}")
            return instance_info
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error creating EC2 instance: {e}")
            raise

    def terminate_instance(self, instance_id: str) -> None:
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            logger.info(f"Terminated EC2 instance {instance_id}")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error terminating EC2 instance {instance_id}: {e}")
            raise

    def get_instance_state(self, instance_id: str) -> str | None:
        try:
            response = self.ec2.describe_instances(InstanceIds=[instance_id])
            reservations = response["Reservations"]
            if not reservations:
                logger.warning(f"Instance {instance_id} not found; assuming 'terminated'")
                return "terminated"
            instance = reservations[0]["Instances"][0]
            state = instance["State"]["Name"]
            logger.info(f"Instance {instance_id} is in state {state}")
            return state
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error getting state for EC2 instance {instance_id}: {e}")
            return None


# Session domain class
class SessionModel(DynamoDBModel[Session]):
    partition_key_value: str = "SESSION"

    def _deserialize(self, data: Dict[str, Any]) -> Session:
        return Session(**data)

    def create_session(self, user_ip: Optional[str], browser_info: Optional[str]) -> Session:
        try:
            instance_model = InstanceModel()
            instance_info = instance_model.create_instance()
            session_id = ksuid().__str__()
            session = Session(
                PK=self.partition_key_value,
                SK=session_id,
                instance=instance_info,
                user_ip=user_ip,
                browser_info=browser_info,
                start_time=datetime.now().isoformat(),
            )
            self.create_item(session)
            logger.info(f"Session {session_id} created with instance {instance_info.instance_id}")
            return session
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            raise

    def end_session(self, session_id: str) -> None:
        try:
            session = self.get_item_by_id(session_id)
            if session and session.instance:
                instance_id = session.instance.instance_id
                instance_model = InstanceModel()
                instance_model.terminate_instance(instance_id)
            self.table.update_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: session_id,
                },
                UpdateExpression="SET end_time = :end_time",
                ExpressionAttributeValues={":end_time": datetime.now().isoformat()},
            )
            logger.info(f"Session {session_id} ended and instance terminated")
        except Exception as e:
            logger.error(f"Error ending session {session_id}: {e}")
            raise

    def update_instance_state(self, session_id: str) -> None:
        try:
            session = self.get_item_by_id(session_id)
            if session and session.instance:
                instance_id = session.instance.instance_id
                instance_model = InstanceModel()
                session.instance.instance_state = instance_model.get_instance_state(instance_id)
                self.table.update_item(
                    Key={
                        self.partition_key_name: self.partition_key_value,
                        self.sort_key_name: session_id,
                    },
                    UpdateExpression="SET instance = :instance",
                    ExpressionAttributeValues={":instance": session.instance.model_dump()},
                )
                logger.info(f"Updated instance state for session {session_id} to {session.instance.instance_state}")
        except Exception as e:
            logger.error(f"Error updating instance state for session {session_id}: {e}")
            raise


# Game domain class
class GameModel(DynamoDBModel[Game]):
    partition_key_value: str = "GAME"

    def _deserialize(self, data: Dict[str, Any]) -> Game:
        return Game(**data)

    def create_game(self, name: str, version: str, apk_s3_path: str) -> Game:
        try:
            game_id = str(uuid.uuid4())
            game = Game(
                PK=self.partition_key_value,
                SK=game_id,
                name=name,
                version=version,
                apk_s3_path=apk_s3_path,
            )
            self.create_item(game)
            logger.info(f"Game {game_id} created: {name} v{version}")
            return game
        except Exception as e:
            logger.error(f"Error creating game: {e}")
            raise


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
        try:
            video_id = ksuid().__str__()
            video = Video(
                PK=self.partition_key_value,
                SK=video_id,
                session_id=session_id,
                game_id=game_id,
                s3_path=s3_path,
                duration=duration,
                size=size,
                timestamp=datetime.now().isoformat(),
            )
            extra_attributes = {
                self.gsi1pk_name: self.gsi1pk_value,
                self.gsi1sk_name: session_id,
                self.gsi2pk_name: self.gsi2pk_value,
                self.gsi2sk_name: game_id,
            }
            self.create_item(video, extra_attributes=extra_attributes)
            logger.info(f"Video {video_id} created for session {session_id} and game {game_id}")
            return video
        except Exception as e:
            logger.error(f"Error creating video: {e}")
            raise

    def get_videos_by_session_id(self, session_id: str) -> List[Video]:
        try:
            response = self.table.query(
                IndexName=self.gsi1_name,
                KeyConditionExpression=Key(self.gsi1pk_name).eq(self.gsi1pk_value)
                & Key(self.gsi1sk_name).eq(session_id),
            )
            items = response.get("Items", [])
            logger.info(f"Retrieved {len(items)} videos for session {session_id}")
            return [self._deserialize(item) for item in items]
        except Exception as e:
            logger.error(f"Error retrieving videos for session {session_id}: {e}")
            raise

    def get_videos_by_game_id(self, game_id: str) -> List[Video]:
        try:
            response = self.table.query(
                IndexName=self.gsi2_name,
                KeyConditionExpression=Key(self.gsi2pk_name).eq(self.gsi2pk_value) & Key(self.gsi2sk_name).eq(game_id),
            )
            items = response.get("Items", [])
            logger.info(f"Retrieved {len(items)} videos for game {game_id}")
            return [self._deserialize(item) for item in items]
        except Exception as e:
            logger.error(f"Error retrieving videos for game {game_id}: {e}")
            raise
