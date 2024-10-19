import logging
from domain import SessionModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    try:
        session_model = SessionModel()

        # Retrieve inactive sessions
        inactive_sessions = session_model.get_inactive_sessions(inactivity_minutes=15)

        if not inactive_sessions:
            logger.info("No inactive sessions found.")
            return

        logger.info(f"Found {len(inactive_sessions)} inactive sessions to terminate.")

        for session in inactive_sessions:
            try:
                session_model.end_session(session.SK)
                logger.info(f"Session {session.SK} scheduled for termination.")
            except Exception as e:
                logger.error(f"Error ending session {session.SK}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in inactive session cleanup handler: {e}")
        raise
