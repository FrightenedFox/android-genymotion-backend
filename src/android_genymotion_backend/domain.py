import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar, Literal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import BackgroundTasks
from fastapi.encoders import jsonable_encoder
from ksuid import ksuid

from schemas import Game, InstanceInfo, Session, Video, AMI
from utils import custom_requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Typical console logger handler
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

    def query_by_gsi(self, gsi_name: str, gsi_pk: str, gsi_sk: str) -> List[T]:
        """
        Generic method to query items by Global Secondary Index (GSI).

        :param gsi_name: The name of the GSI.
        :param gsi_pk: The partition key value for the GSI.
        :param gsi_sk: The sort key value for the GSI.
        :return: A list of deserialized items.
        """
        try:
            response = self.table.query(
                IndexName=gsi_name,
                KeyConditionExpression=Key(self.gsi1pk_name).eq(gsi_pk) & Key(self.gsi1sk_name).eq(gsi_sk),
            )
            items = response.get("Items", [])
            logger.info(f"Retrieved {len(items)} items for {gsi_name} with PK: {gsi_pk} and SK: {gsi_sk}")
            return [self._deserialize(item) for item in items]
        except Exception as e:
            logger.error(f"Error querying {gsi_name} with PK: {gsi_pk} and SK: {gsi_sk}: {e}")
            raise

    def _serialize(self, item: T) -> Dict[str, Any]:
        return jsonable_encoder(item)

    def _deserialize(self, data: Dict[str, Any]) -> T:
        raise NotImplementedError("Subclasses must implement _deserialize method.")


class VcpuLimitExceededException(Exception):
    def __init__(
        self,
        message="You have reached your EC2 vCPU limit. Unable to create more instances at this time. Please try again later or contact support.",
    ):
        self.message = message
        super().__init__(self.message)


class InstanceModel:
    def __init__(self) -> None:
        self.ec2 = boto3.client("ec2")

    def create_instance(self, ami_id: str) -> InstanceInfo:
        try:
            ami_model = AMIModel()
            ami_info = ami_model.get_ami_by_id(ami_id)
            if not ami_info:
                raise ValueError(f"AMI {ami_id} not found")
            response = self.ec2.run_instances(
                ImageId=ami_id,
                InstanceType=ami_info.instance_type,
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
            if "VcpuLimitExceeded" in str(e):
                logger.error("VcpuLimitExceeded error while creating EC2 instance")
                raise VcpuLimitExceededException()
            logger.error(f"Error creating EC2 instance: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating EC2 instance: {e}")
            raise

    def terminate_instance(self, instance_id: str) -> None:
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            logger.info(f"Terminated EC2 instance {instance_id}")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error terminating EC2 instance {instance_id}: {e}")
            raise

    def get_instance_info(self, instance_id: str) -> Optional[InstanceInfo]:
        try:
            response = self.ec2.describe_instances(InstanceIds=[instance_id])
            reservations = response["Reservations"]
            if not reservations:
                logger.warning(f"Instance {instance_id} not found; setting instance info to None")
                return None

            instance = reservations[0]["Instances"][0]
            state = instance["State"]["Name"]
            ip_address = instance.get("PublicIpAddress")
            aws_address = instance.get("PublicDnsName")

            return InstanceInfo(
                instance_id=instance_id,
                instance_type=instance["InstanceType"],
                instance_state=state,
                instance_ip=ip_address,
                instance_aws_address=aws_address,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error getting info for EC2 instance {instance_id}: {e}")
            return None

    def get_instances_info(self, instance_ids: List[str]) -> Dict[str, Optional[InstanceInfo]]:
        """
        Returns a mapping from instance IDs to their full instance information.
        If an instance is not found, its information is set to None.
        """
        instances_info = {}
        try:
            # Split instance_ids into chunks to avoid exceeding API limits
            max_ids_per_request = 1000  # AWS limit per request
            for i in range(0, len(instance_ids), max_ids_per_request):
                chunk = instance_ids[i : i + max_ids_per_request]
                response = self.ec2.describe_instances(InstanceIds=chunk)
                found_instance_ids = set()
                for reservation in response["Reservations"]:
                    for instance in reservation["Instances"]:
                        instance_id = instance["InstanceId"]
                        state = instance["State"]["Name"]
                        ip_address = instance.get("PublicIpAddress")
                        aws_address = instance.get("PublicDnsName")

                        instances_info[instance_id] = InstanceInfo(
                            instance_id=instance_id,
                            instance_type=instance["InstanceType"],
                            instance_state=state,
                            instance_ip=ip_address,
                            instance_aws_address=aws_address,
                        )
                        found_instance_ids.add(instance_id)
                # For instance IDs not found in response, set their information to None
                missing_instance_ids = set(chunk) - found_instance_ids
                for missing_id in missing_instance_ids:
                    logger.warning(f"Instance {missing_id} not found; setting info to None")
                    instances_info[missing_id] = None
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error getting info for instances: {e}")
            # Set info for all IDs to None in case of error
            for instance_id in instance_ids:
                instances_info[instance_id] = None
        return instances_info

    def wait_for_instance_running(self, instance_id: str, timeout: int = 300) -> Optional[InstanceInfo]:
        import time

        start_time = time.time()
        while time.time() - start_time < timeout:
            instance_info = self.get_instance_info(instance_id)
            if instance_info and instance_info.instance_state == "running" and instance_info.instance_ip:
                return instance_info
            else:
                time.sleep(5)
        logger.error(f"Instance {instance_id} did not become running within timeout")
        return None


# Session domain class
class SessionModel(DynamoDBModel[Session]):
    partition_key_value: str = "SESSION"

    @staticmethod
    def domain_name(session_id: str) -> str:
        return f"{session_id}.session.morskyi.org"

    def _deserialize(self, data: Dict[str, Any]) -> Session:
        return Session(**data)

    def update_last_accessed(self, session_id: str) -> None:
        # Check if the session exists
        session = self.get_item_by_id(session_id)
        if not session:
            logger.warning(f"Session {session_id} not found. Cannot update last_accessed_on.")
            return

        # Proceed with updating the last accessed time
        self.table.update_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: session_id,
            },
            UpdateExpression="SET last_accessed_on = :last_accessed_on",
            ExpressionAttributeValues={":last_accessed_on": datetime.now().isoformat()},
        )
        logger.info(f"Updated last_accessed_on for session {session_id}")

    def create_session(self, ami_id: str, user_ip: Optional[str], browser_info: Optional[str]) -> Session:
        try:
            instance_model = InstanceModel()
            instance_info = instance_model.create_instance(ami_id)
            session_id = ksuid().__str__()
            session = Session(
                PK=self.partition_key_value,
                SK=session_id,
                instance=instance_info,
                ami_id=ami_id,
                user_ip=user_ip,
                browser_info=browser_info,
                start_time=datetime.now().isoformat(),
            )
            self.create_item(session)
            self.update_last_accessed(session_id)
            logger.info(f"Session {session_id} created with instance {instance_info.instance_id}")

            # Send a message to the SQS queue
            self._enqueue_background_task(session_id, instance_info)

            return session
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            raise

    def _enqueue_background_task(self, session_id: str, instance_info: InstanceInfo) -> None:
        try:
            sqs = boto3.client("sqs")
            queue_url = os.environ["TASK_QUEUE_URL"]
            message_body = {
                "session_id": session_id,
                "instance_id": instance_info.instance_id,
            }
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
            logger.info(f"Enqueued background task for session {session_id}")
        except Exception as e:
            logger.error(f"Error enqueuing background task: {e}")
            raise

    def _setup_dns_and_certificate(self, session_id: str, instance_info: InstanceInfo) -> None:
        # Wait for instance to be running and get IP
        instance_model = InstanceModel()
        instance_info = instance_model.wait_for_instance_running(instance_info.instance_id)

        if instance_info and instance_info.instance_ip:
            # Create DNS record
            self.create_dns_record(session_id, instance_info.instance_ip)

            # Make API call to the instance
            self.configure_instance_certificate(session_id, instance_info)
        else:
            logger.error(f"Instance {instance_info.instance_id} did not reach running state or has no public IP.")

    def create_dns_record(self, session_id: str, instance_ip: str) -> None:
        route53 = boto3.client("route53")
        domain_name = self.domain_name(session_id)
        try:
            route53.change_resource_record_sets(
                HostedZoneId=os.environ["HOSTED_ZONE_ID"],
                ChangeBatch={
                    "Comment": f"Add record for {domain_name}",
                    "Changes": [{
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": domain_name,
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": instance_ip}],
                        },
                    }],
                },
            )
            logger.info(f"DNS record created for {domain_name} pointing to {instance_ip}")
        except Exception as e:
            logger.error(f"Error creating DNS record: {e}")

    def configure_instance_certificate(self, session_id: str, instance_info: InstanceInfo) -> None:
        try:
            # wait for 15 seconds before configuring the certificate
            url = f"https://{instance_info.instance_ip}/api/v1/configuration/certificate"
            data = [self.domain_name(session_id)]
            auth = ("genymotion", instance_info.instance_id)
            response = custom_requests(total_retries=9, backoff_factor=1.5, connect_timeout=5, read_timeout=15).post(
                url, json=data, auth=auth, verify=False  # Since the certificate might not be valid yet
            )
            if str(response.status_code).startswith("2"):
                logger.info(f"Certificate configured on instance {instance_info.instance_id}")
                # Update ssl_configured to True
                instance_info.ssl_configured = True
                instance_info.secure_address = self.domain_name(session_id)
                self.update_instance_in_session(session_id, instance_info)
            else:
                logger.error(f"Failed to configure certificate: {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Error configuring instance certificate: {e}")

    def end_session(self, session_id: str) -> None:
        try:
            session = self.get_item_by_id(session_id)
            if session and session.instance:
                instance_id = session.instance.instance_id
                instance_model = InstanceModel()
                # Terminate the instance
                instance_model.terminate_instance(instance_id)
                # Delete DNS record
                self._delete_dns_record(session_id, session.instance.instance_ip)
                session.instance.ssl_configured = False
                session.instance.secure_address = None
                self.update_instance_in_session(session_id, session.instance)
            self.table.update_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: session_id,
                },
                UpdateExpression="SET end_time = :end_time",
                ExpressionAttributeValues={":end_time": datetime.now().isoformat()},
            )
            self.update_last_accessed(session_id)
            logger.info(f"Session {session_id} ended, instance terminated, and DNS record deleted")
        except Exception as e:
            logger.error(f"Error ending session {session_id}: {e}")
            raise

    def _delete_dns_record(self, session_id: str, instance_ip: str) -> None:
        route53 = boto3.client("route53")
        domain_name = self.domain_name(session_id)
        try:
            route53.change_resource_record_sets(
                HostedZoneId=os.environ["HOSTED_ZONE_ID"],
                ChangeBatch={
                    "Comment": f"Delete record for {domain_name}",
                    "Changes": [{
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": domain_name,
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": instance_ip}],
                        },
                    }],
                },
            )
            logger.info(f"DNS record deleted for {domain_name}")
        except route53.exceptions.InvalidChangeBatch:
            logger.warning(f"DNS record for {domain_name} already deleted or does not exist")
        except Exception as e:
            logger.error(f"Error deleting DNS record: {e}")

    def end_all_active_sessions(self, background_tasks: BackgroundTasks) -> None:
        """
        Ends all sessions that have an active instance.
        """
        try:
            sessions = self.get_all_items()
            active_sessions = [
                session for session in sessions if session.instance and session.instance.instance_state == "running"
            ]

            logger.info(f"Found {len(active_sessions)} active sessions to terminate.")

            for session in active_sessions:
                # Add each session termination to background tasks
                background_tasks.add_task(self.end_session, session.SK)

            logger.info("All active sessions have been queued for termination.")
        except Exception as e:
            logger.error(f"Error ending all active sessions: {e}")
            raise

    def update_instance_in_session(self, session_id: str, instance_info: Optional[InstanceInfo] = None) -> None:
        try:
            session = self.get_item_by_id(session_id)
            if not session or not session.instance:
                logger.warning(f"Session {session_id} not found or has no associated instance.")
                return

            # If instance_info is not provided, fetch it from AWS
            if not instance_info:
                instance_model = InstanceModel()
                instance_info = instance_model.get_instance_info(session.instance.instance_id)
                if not instance_info:
                    logger.warning(f"Could not retrieve AWS instance info for session {session_id}")
                    return
                instance_info.ssl_configured = session.instance.ssl_configured
                instance_info.secure_address = session.instance.secure_address

            # Update the session's instance information
            session.instance = instance_info
            self.table.update_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: session_id,
                },
                UpdateExpression="SET instance = :instance",
                ExpressionAttributeValues={":instance": session.instance.model_dump()},
            )
            logger.info(f"Updated instance info for session {session_id}")

        except Exception as e:
            logger.error(f"Error updating instance info for session {session_id}: {e}")
            raise

    def get_all_sessions_with_updated_info(self, only_active: bool = False) -> List[Session]:
        """
        Retrieves all sessions and updates their instance information.
        If `only_active` is True, returns only sessions with active instances.
        """
        try:
            sessions = self.get_all_items()
            instance_ids = [session.instance.instance_id for session in sessions if session.instance]
            logger.info(f"Retrieved {len(sessions)} sessions, instances: {instance_ids}")
            if instance_ids:
                instance_model = InstanceModel()
                aws_instances_info = instance_model.get_instances_info(instance_ids)
                # Update each session's instance information
                for session in sessions:
                    if not session.instance:
                        continue
                    aws_instance_info = aws_instances_info.get(session.instance.instance_id)
                    if aws_instance_info:
                        aws_instance_info.ssl_configured = session.instance.ssl_configured
                        aws_instance_info.secure_address = session.instance.secure_address
                    session.instance = aws_instance_info
                    # Update DynamoDB with the new instance information
                    self.table.update_item(
                        Key={
                            self.partition_key_name: self.partition_key_value,
                            self.sort_key_name: session.SK,
                        },
                        UpdateExpression="SET instance = :instance",
                        ExpressionAttributeValues={
                            ":instance": session.instance.model_dump() if session.instance else None
                        },
                    )
            # If only_active is True, filter out non-active sessions
            if only_active:
                sessions = [
                    session
                    for session in sessions
                    if session.instance and session.instance.instance_state in ["running", "pending"]
                ]
            return sessions
        except Exception as e:
            logger.error(f"Error retrieving and updating sessions: {e}")
            raise


class AMIModel(DynamoDBModel[AMI]):
    partition_key_value: str = "AMI"

    def _deserialize(self, data: Dict[str, Any]) -> AMI:
        return AMI(**data)

    def create_ami(
        self,
        ami_id: str,
        representing_year: int,
        instance_type: str,
        disk_size: int,
        android_version: str,
        screen_width: int,
        screen_height: int,
    ) -> AMI:
        try:
            ami = AMI(
                PK=self.partition_key_value,
                SK=ami_id,
                representing_year=representing_year,
                instance_type=instance_type,
                disk_size=disk_size,
                android_version=android_version,
                screen_width=screen_width,
                screen_height=screen_height,
            )
            self.create_item(ami)
            logger.info(f"AMI {ami_id} created with instance type {instance_type}")
            return ami
        except Exception as e:
            logger.error(f"Error creating AMI: {e}")
            raise

    def get_ami_by_id(self, ami_id: str) -> Optional[AMI]:
        try:
            return self.get_item_by_id(ami_id)
        except Exception as e:
            logger.error(f"Error retrieving AMI with ID {ami_id}: {e}")
            raise

    def list_all_amis(self) -> List[AMI]:
        try:
            return self.get_all_items()
        except Exception:
            logger.error("Error retrieving all AMIs")
            raise


# Game domain class
class GameModel(DynamoDBModel[Game]):
    partition_key_value: str = "GAME"
    gsi1pk_value: str = "AMI"

    def _deserialize(self, data: Dict[str, Any]) -> Game:
        return Game(**data)

    def create_game(
        self,
        name: str,
        version: str,
        apk_s3_path: str,
        ami_id: str,
        android_package_name: Optional[str] = None,
        wifi_enabled: bool = True,
        screen_orientation: Literal["horizontal", "vertical"] = "vertical",
    ) -> Game:
        try:
            game_id = str(uuid.uuid4())
            game = Game(
                PK=self.partition_key_value,
                SK=game_id,
                name=name,
                game_version=version,
                apk_s3_path=apk_s3_path,
                android_package_name=android_package_name,
                wifi_enabled=wifi_enabled,
                screen_orientation=screen_orientation,
            )
            extra_attributes = {
                self.gsi1pk_name: self.gsi1pk_value,
                self.gsi1sk_name: ami_id,
            }
            self.create_item(game, extra_attributes=extra_attributes)
            logger.info(f"Game {game_id} created: {name} v{version}")
            return game
        except Exception as e:
            logger.error(f"Error creating game: {e}")
            raise

    def get_games_by_ami_id(self, ami_id: str) -> List[Game]:
        return self.query_by_gsi(self.gsi1_name, self.gsi1pk_value, ami_id)


# Video domain class
class VideoModel(DynamoDBModel[Video]):
    partition_key_value: str = "VIDEO"
    gsi1pk_value = "SESSION"
    gsi2pk_value = "GAME"

    def _deserialize(self, data: Dict[str, Any]) -> Video:
        return Video(**data)

    def create_video(
        self,
        video_id: str,
        session_id: str,
        game_id: str,
        duration: Optional[int] = None,
        size: Optional[int] = None,
    ) -> Video:
        try:
            s3_path = f"recordings/{video_id}.mp4"
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

    def update_video_size_and_duration(self, video_id: str, size: int, duration: Optional[int] = None):
        try:
            update_expression = "SET size = :size"
            expression_attribute_values = {":size": size}

            if duration is not None:
                update_expression += ", duration = :duration"
                expression_attribute_values[":duration"] = duration

            self.table.update_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: video_id,
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
            )
            logger.info(f"Video {video_id} updated with size {size} and duration {duration}")
        except Exception as e:
            logger.error(f"Error updating video {video_id}: {e}")
            raise

    def get_videos_by_session_id(self, session_id: str) -> List[Video]:
        return self.query_by_gsi(self.gsi1_name, self.gsi1pk_value, session_id)

    def get_videos_by_game_id(self, game_id: str) -> List[Video]:
        return self.query_by_gsi(self.gsi2_name, self.gsi2pk_value, game_id)
