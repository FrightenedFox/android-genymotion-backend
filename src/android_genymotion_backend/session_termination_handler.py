import json
import logging
from datetime import datetime

from application_manager import ApplicationManager
from domain import SessionModel
from schemas import Session

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
            session = session_model.get_session_by_id(session_id)
            if not session or not session.instance:
                logger.error(f"Session {session_id} not found or has no instance.")
                session_model.update_session_to_inactive(session_id)
                continue

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
                session_model.instance_model.terminate_instance(session.instance.instance_id)

            except Exception as e:
                logger.error(f"Error terminating instance {session.instance.instance_id}: {e}")

            # Delete DNS record
            if session.instance:
                session_model.delete_dns_record(session_id, session.instance.instance_ip)

            # Update session status
            session_model.update_session_to_inactive(session_id)

            logger.info(f"Session {session_id} terminated successfully.")

        except Exception as e:
            logger.error(f"Error processing session termination for session {session_id}: {e}")
            continue
