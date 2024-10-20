import json
import logging
from datetime import datetime

from application_manager import ApplicationManager
from domain import SessionModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    records = event.get("Records", [])
    logger.info(f"Processing {len(records)} records from SessionTerminationQueue")

    for record in records:
        body = json.loads(record["body"])
        session_id = body["session_id"]
        try:
            logger.info(f"Processing session termination for session {session_id}")

            session_model = SessionModel()
            app_manager = ApplicationManager()

            # Fetch the session
            session = session_model.get_item_by_id(session_id)
            if not session or not session.instance:
                logger.error(f"Session {session_id} not found or has no instance.")
                continue

            instance_id = session.instance.instance_id
            instance_info = session_model.instance_model.get_instance_info(instance_id)

            # Cleanup the session
            try:
                app_manager.cleanup_session(session_id)
            except Exception as e:
                logger.error(f"Error cleaning up session {session_id}: {e}")

            # Upload all recordings to S3
            try:
                app_manager.upload_all_recordings_to_s3(session_id)
            except Exception as e:
                logger.error(f"Error uploading recordings for session {session_id}: {e}")

            try:
                # Terminate the EC2 instance
                session_model.instance_model.terminate_instance(instance_id)

                # Update instance_active to False
                session_model.session_ping_model.update_instance_active(session_id, False)
            except Exception as e:
                logger.error(f"Error terminating instance {instance_id}: {e}")

            # Delete DNS record
            if instance_info:
                session_model.delete_dns_record(session_id, instance_info.instance_ip)

            # Update session's end_time and set scheduled_for_deletion to False
            session_model.table.update_item(
                Key={
                    session_model.partition_key_name: session_model.partition_key_value,
                    session_model.sort_key_name: session_id,
                },
                UpdateExpression="SET end_time = :end_time",
                ExpressionAttributeValues={":end_time": datetime.now().isoformat()},
            )
            session_model.session_ping_model.table.update_item(
                Key={
                    session_model.partition_key_name: session_model.session_ping_model.partition_key_value,
                    session_model.sort_key_name: session_id,
                },
                UpdateExpression="SET scheduled_for_deletion = :scheduled_for_deletion",
                ExpressionAttributeValues={":scheduled_for_deletion": False},
            )

            logger.info(f"Session {session_id} terminated successfully.")

        except Exception as e:
            logger.error(f"Error processing session termination for session {session_id}: {e}")
            continue
