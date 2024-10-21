import json
import logging
import time

from domain import InstanceModel, SessionModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    records = event.get("Records", [])
    logger.info(f"Processing {len(records)} records from SQS")

    for record in records:
        try:
            body = json.loads(record["body"])
            session_id = body["session_id"]
            instance_id = body["instance_id"]

            logger.info(f"Processing background task for session {session_id}, instance {instance_id}")

            # Perform background tasks
            session_model = SessionModel()

            # Wait for instance to be running
            instance_info = session_model.instance_model.wait_for_instance_running(instance_id)
            if not instance_info:
                logger.error(f"Instance {instance_id} did not become running")
                continue

            # Create DNS record
            session_model.create_dns_record(session_id, instance_info.instance_ip)

            # Wait for Genymotion API to be available
            session_model.wait_for_genymotion_api(session_id)

            # Wait for Genymotion instance to be ready
            time.sleep(5)

            # Configure SSL certificate
            session_model.configure_instance_certificate(session_id, instance_info)

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            continue
