import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Generic, List, Optional, TypeVar, Literal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import BotoCoreError, ClientError
from fastapi.encoders import jsonable_encoder
from ksuid import ksuid

from schemas import Game, InstanceInfo, Session, Video, AMI, CompleteInstanceInfo, SessionPing, SessionWithPing
from utils import custom_requests, execute_shell_command

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
            # Determine which GSI is being queried and set appropriate key names
            if gsi_name == self.gsi1_name:
                pk_name = self.gsi1pk_name
                sk_name = self.gsi1sk_name
            elif gsi_name == self.gsi2_name:
                pk_name = self.gsi2pk_name
                sk_name = self.gsi2sk_name
            else:
                raise ValueError(f"Unsupported GSI name: {gsi_name}")

            response = self.table.query(
                IndexName=gsi_name,
                KeyConditionExpression=Key(pk_name).eq(gsi_pk) & Key(sk_name).eq(gsi_sk),
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
            instance_info = InstanceInfo(instance_id=instance_id, instance_type=instance_type)
            logger.info(f"Created EC2 instance {instance_id} of type {instance_type}")
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

    def get_instance_info(self, instance_id: str) -> Optional[CompleteInstanceInfo]:
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

            return CompleteInstanceInfo(
                instance_id=instance_id,
                instance_type=instance["InstanceType"],
                instance_state=state,
                instance_ip=ip_address,
                instance_aws_address=aws_address,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error getting info for EC2 instance {instance_id}: {e}")
            return None

    def get_instances_info(self, instance_ids: List[str]) -> Dict[str, Optional[CompleteInstanceInfo]]:
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

                        instances_info[instance_id] = CompleteInstanceInfo(
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
            # # Set info for all IDs to None in case of error
            # for instance_id in instance_ids:
            #     instances_info[instance_id] = None
        return instances_info

    def wait_for_instance_running(self, instance_id: str, timeout: int = 300) -> Optional[CompleteInstanceInfo]:
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


class SessionPingModel(DynamoDBModel[SessionPing]):
    partition_key_value: str = "SESSION#PING"

    def _deserialize(self, data: Dict[str, Any]) -> SessionPing:
        return SessionPing(**data)

    def update_last_accessed(self, session_id: str, instance_active: bool = True) -> None:
        # Check if the session exists
        session = self.get_item_by_id(session_id)
        if not session:
            logger.warning(f"SessionPing {session_id} not found. Creating new one")
            self.create_item(
                SessionPing(SK=session_id, last_accessed_on=datetime.now().isoformat(), instance_active=instance_active)
            )
            return
        else:
            self.table.update_item(
                Key={
                    self.partition_key_name: self.partition_key_value,
                    self.sort_key_name: session_id,
                },
                UpdateExpression="SET last_accessed_on = :last_accessed_on, instance_active = :instance_active",
                ExpressionAttributeValues={
                    ":last_accessed_on": datetime.now().isoformat(),
                    ":instance_active": instance_active,
                },
            )
            logger.info(f"Updated last_accessed_on and {instance_active=} for SessionPing {session_id}")

    def update_instance_active(self, session_id: str, instance_active: bool = True) -> None:
        self.table.update_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: session_id,
            },
            UpdateExpression="SET instance_active = :instance_active",
            ExpressionAttributeValues={":instance_active": instance_active},
        )
        logger.info(f"Updated {instance_active=} for SessionPing {session_id}")

    def update_scheduled_for_deletion(self, session_id: str, scheduled_for_deletion: bool = True) -> None:
        self.table.update_item(
            Key={
                self.partition_key_name: self.partition_key_value,
                self.sort_key_name: session_id,
            },
            UpdateExpression="SET scheduled_for_deletion = :scheduled_for_deletion",
            ExpressionAttributeValues={":scheduled_for_deletion": scheduled_for_deletion},
        )
        logger.info(f"Updated {scheduled_for_deletion=} for SessionPing {session_id}")

    def get_inactive_session_pings(self, inactivity_minutes: int = 15) -> List[SessionPing]:
        try:
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(minutes=inactivity_minutes)
            cutoff_iso = cutoff_time.isoformat()

            # Initialize variables for pagination
            session_pings = []
            exclusive_start_key = None

            while True:
                # Scan the table with filter expressions
                scan_kwargs = {
                    "FilterExpression": (
                        Attr("PK").eq(self.partition_key_value)
                        & Attr("instance_active").eq(True)
                        & Attr("scheduled_for_deletion").eq(False)
                        & Attr("last_accessed_on").lt(cutoff_iso)
                    ),
                }

                if exclusive_start_key:
                    scan_kwargs["ExclusiveStartKey"] = exclusive_start_key

                response = self.table.scan(**scan_kwargs)
                items = response.get("Items", [])

                session_pings.extend([self._deserialize(item) for item in items])

                # Check if there are more items to scan
                exclusive_start_key = response.get("LastEvaluatedKey")
                if not exclusive_start_key:
                    break

            logger.info(f"Found {len(session_pings)} inactive session_pings.")
            return session_pings
        except Exception as e:
            logger.error(f"Error retrieving inactive sessions: {e}")
            raise


class SessionModel(DynamoDBModel[Session]):
    partition_key_value: str = "SESSION"

    def __init__(self):
        super().__init__()
        self.session_ping_model = SessionPingModel()
        self.instance_model = InstanceModel()

    def _deserialize(self, data: Dict[str, Any]) -> Session:
        return Session(**data)

    @staticmethod
    def domain_name(session_id: str) -> str:
        return f"{session_id}.session.morskyi.org"

    def get_session_by_id(self, session_id: str) -> Optional[SessionWithPing]:
        try:
            session = SessionWithPing(**self.get_item_by_id(session_id).model_dump())
            session.instance = InstanceModel().get_instance_info(session.instance.instance_id)
            session_ping = self.session_ping_model.get_item_by_id(session_id)
            if session_ping:
                session.instance_active = session_ping.instance_active
                session.last_accessed_on = session_ping.last_accessed_on
                session.scheduled_for_deletion = session_ping.scheduled_for_deletion
            return session
        except Exception as e:
            logger.error(f"Error retrieving session with ID {session_id}: {e}")

    def create_session(self, ami_id: str, user_ip: Optional[str], browser_info: Optional[str]) -> Session:
        try:
            instance_info = self.instance_model.create_instance(ami_id)
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
            self.session_ping_model.create_item(
                SessionPing(SK=session_id, instance_active=True, last_accessed_on=datetime.now().isoformat())
            )
            logger.info(f"Session {session_id} created with instance {instance_info.instance_id}")

            # Send a message to the SQS queue
            self._enqueue_session_creation_task(session_id, instance_info)

            return session
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            raise

    def _enqueue_session_creation_task(self, session_id: str, instance_info: InstanceInfo) -> None:
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

    def configure_instance_certificate(self, session_id: str, instance_info: CompleteInstanceInfo) -> None:
        try:
            url = f"https://{instance_info.instance_ip}/api/v1/configuration/certificate"
            data = [self.domain_name(session_id)]
            auth = ("genymotion", instance_info.instance_id)
            response = custom_requests(total_retries=9, backoff_factor=1.5, connect_timeout=5, read_timeout=15).post(
                url, json=data, auth=auth, verify=False  # Since the certificate might not be valid yet
            )
            success = False
            if response.status_code == 404:
                commands = [
                    'setprop persist.tls.acme.domains {"user_dns":"%s"}' % self.domain_name(session_id),
                    "am startservice -a genymotionacme.generate -n com.genymobile.genymotionacme/.AcmeService",
                ]
                logger.error(
                    f"Failed to configure certificate normally: {response.status_code}, {response.text}. Retrying with"
                    f" shell method, command: {commands}..."
                )
                new_response = execute_shell_command(
                    instance_info.instance_ip, instance_info.instance_id, commands, logger, verify_ssl=False
                )
                logger.info(f"Shell command response: {new_response.text}")
                success = new_response.status_code == 200 or new_response.text.startswith("Starting service: Intent")
                if success:
                    import time
                    time.sleep(15)  # Wait for the service to start

            if success or str(response.status_code).startswith("2"):
                logger.info(f"Certificate configured on instance {instance_info.instance_id}")
                self.table.update_item(
                    Key={
                        self.partition_key_name: self.partition_key_value,
                        self.sort_key_name: session_id,
                    },
                    UpdateExpression="SET ssl_configured = :ssl_configured",
                    ExpressionAttributeValues={":ssl_configured": True},
                )
            else:
                logger.error(f"Failed to configure certificate: {response.status_code}, {response.text}. Retrying...")

        except Exception as e:
            logger.error(f"Error configuring instance certificate: {e}")

    def end_session(self, session_id: str) -> None:
        try:
            session = self.get_item_by_id(session_id)
            if not session:
                logger.warning(f"Session {session_id} not found.")
                return

            # Enqueue a message to the SessionTerminationQueue
            self.session_ping_model.update_scheduled_for_deletion(session_id)
            self._enqueue_session_termination_task(session_id)

            logger.info(f"Session {session_id} scheduled for termination.")
        except Exception as e:
            logger.error(f"Error ending session {session_id}: {e}")
            raise

    def _enqueue_session_termination_task(self, session_id: str) -> None:
        try:
            sqs = boto3.client("sqs")
            queue_url = os.environ["SESSION_TERMINATION_QUEUE_URL"]
            message_body = {
                "session_id": session_id,
            }
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
            logger.info(f"Enqueued session termination task for session {session_id}")
        except Exception as e:
            logger.error(f"Error enqueuing session termination task: {e}")
            raise

    def end_all_running_sessions(self) -> List[Session]:
        """
        Ends all sessions that have an active instance.
        """
        try:
            sessions = self.get_all_sessions_with_updated_info()
            active_sessions = [
                session for session in sessions if session.instance and session.instance.instance_state == "running"
            ]

            logger.info(f"Found {len(active_sessions)} active sessions to terminate.")

            for session in active_sessions:
                self.end_session(session.SK)

            logger.info(
                f"{len(active_sessions)} active sessions have been scheduled for termination:"
                f" {[s.SK for s in active_sessions]}"
            )
            return active_sessions
        except Exception as e:
            logger.error(f"Error ending all active sessions: {e}")
            raise

    def get_inactive_sessions(self, inactivity_minutes: int = 15) -> List[SessionWithPing]:
        session_pings = self.session_ping_model.get_inactive_session_pings(inactivity_minutes)
        logger.info(f"Found {len(session_pings)} inactive session pings: {session_pings}")
        return [self.get_session_by_id(ping.SK) for ping in session_pings]

    def delete_dns_record(self, session_id: str, instance_ip: str) -> None:
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

    def get_all_sessions_with_updated_info(self, only_active: bool = False) -> List[Session]:
        try:
            sessions = self.get_all_items()
            instance_ids = [session.instance.instance_id for session in sessions if session.instance]
            logger.info(f"Retrieved {len(sessions)} sessions, instances: {instance_ids}")
            if instance_ids:
                aws_instances_info = self.instance_model.get_instances_info(instance_ids)
                # Update each session's instance information
                for session in sessions:
                    if not session.instance:
                        continue
                    aws_instance_info = aws_instances_info.get(session.instance.instance_id, None)
                    session.instance = aws_instance_info

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

    def get_recommended_ami(self) -> Optional[AMI]:
        """
        Returns the AMI with the lowest number of videos recorded for its assigned games.
        """
        try:
            amis = self.list_all_amis()
            if not amis:
                logger.warning("No AMIs found.")
                return None

            game_model = GameModel()
            video_model = VideoModel()

            ami_video_counts = {}

            for ami in amis:
                games = game_model.get_games_by_ami_id(ami.SK)
                total_videos = 0
                for game in games:
                    videos = video_model.get_videos_by_game_id(game.SK)
                    total_videos += len(videos)
                ami_video_counts[ami.SK] = total_videos
                logger.info(f"AMI {ami.SK} has {total_videos} videos.")

            # Find the AMI with the lowest video count
            recommended_ami_id = min(ami_video_counts, key=ami_video_counts.get)
            recommended_ami = self.get_ami_by_id(recommended_ami_id)
            logger.info(f"Recommended AMI is {recommended_ami_id} with {ami_video_counts[recommended_ami_id]} videos.")

            return recommended_ami
        except Exception as e:
            logger.error(f"Error getting recommended AMI: {e}")
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

    def get_recommended_game_for_ami(self, ami_id: str) -> Optional[Game]:
        """
        Returns the game assigned to the given AMI with the lowest number of videos.
        """
        try:
            games = self.get_games_by_ami_id(ami_id)
            if not games:
                logger.warning(f"No games found for AMI {ami_id}.")
                return None

            video_model = VideoModel()
            game_video_counts = {}

            for game in games:
                videos = video_model.get_videos_by_game_id(game.SK)
                game_video_counts[game.SK] = len(videos)
                logger.info(f"Game {game.SK} has {len(videos)} videos.")

            # Find the game with the lowest video count
            recommended_game_id = min(game_video_counts, key=game_video_counts.get)
            recommended_game = self.get_item_by_id(recommended_game_id)
            logger.info(
                f"Recommended game for AMI {ami_id} is {recommended_game_id} with"
                f" {game_video_counts[recommended_game_id]} videos."
            )

            return recommended_game
        except Exception as e:
            logger.error(f"Error getting recommended game for AMI {ami_id}: {e}")
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
