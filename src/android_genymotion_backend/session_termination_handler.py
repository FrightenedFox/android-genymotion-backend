import json
import logging
from datetime import datetime
from domain import SessionModel, InstanceModel
from application_manager import ApplicationManager

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
            instance_model = InstanceModel()
            app_manager = ApplicationManager()

            # Fetch the session
            session = session_model.get_item_by_id(session_id)
            if not session or not session.instance:
                logger.error(f"Session {session_id} not found or has no instance.")
                continue

            instance_id = session.instance.instance_id

            # Cleanup the session
            app_manager.cleanup_session(session_id)

            # Upload all recordings to S3
            app_manager.upload_all_recordings_to_s3(session_id)

            # Terminate the EC2 instance
            instance_model.terminate_instance(instance_id)

            # Delete DNS record
            session_model._delete_dns_record(session_id, session.instance.instance_ip)

            # Update session's end_time and set scheduled_for_deletion to False
            session_model.table.update_item(
                Key={
                    session_model.partition_key_name: session_model.partition_key_value,
                    session_model.sort_key_name: session_id,
                },
                UpdateExpression="SET end_time = :end_time, scheduled_for_deletion = :scheduled_for_deletion",
                ExpressionAttributeValues={":end_time": datetime.now().isoformat(), ":scheduled_for_deletion": False},
            )

            logger.info(f"Session {session_id} terminated successfully.")

        except Exception as e:
            logger.error(f"Error processing session termination for session {session_id}: {e}")
            continue
