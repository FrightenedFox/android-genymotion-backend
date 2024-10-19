import logging
import os
from typing import List

import boto3
import requests
from ksuid import ksuid

from domain import SessionModel, GameModel, VideoModel
from utils import genymotion_request

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ApplicationManager:
    def __init__(self) -> None:
        self.session_model = SessionModel()
        self.game_model = GameModel()
        self.video_model = VideoModel()
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME", "android-project")

    def _execute_shell_command(self, address: str, instance_id: str, command: str):
        """
        Executes a shell command on the device via the Genymotion API.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            command (str): The shell command to execute.
        """
        endpoint = "/android/shell"
        data = {"commands": [command], "timeout_in_seconds": 10}

        response = genymotion_request(
            address=address, instance_id=instance_id, method="POST", endpoint=endpoint, data=data, verify_ssl=True
        )
        return response.json()

    def _set_screen_orientation(self, address: str, instance_id: str, orientation: str):
        """
        Sets the screen orientation.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            orientation (str): 'horizontal' or 'vertical'.
        """
        angle = 0 if orientation == "vertical" else 90
        endpoint = "/sensors/orientation"
        data = {"angle": angle}

        genymotion_request(
            address=address, instance_id=instance_id, method="POST", endpoint=endpoint, data=data, verify_ssl=True
        )
        logger.info(f"Screen orientation set to {orientation} on {address}")

    def _set_virtual_keyboard(self, address: str, instance_id: str, enabled: bool):
        """
        Enables or disables the virtual keyboard.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            enabled (bool): True to enable, False to disable.
        """
        if enabled:
            command = "settings put secure show_ime_with_hard_keyboard 1"
        else:
            command = "settings put secure show_ime_with_hard_keyboard 0"

        self._execute_shell_command(address, instance_id, command)
        logger.info(f"Virtual keyboard {'enabled' if enabled else 'disabled'} on {address}")

    def _launch_application(self, address: str, instance_id: str, package_name: str):
        """
        Launches the application.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            package_name (str): The package name of the application.
        """
        command = f"monkey -p {package_name} -c android.intent.category.LAUNCHER 1"
        self._execute_shell_command(address, instance_id, command)
        logger.info(f"Application {package_name} launched on {address}")

    def _stop_all_applications(self, address: str, instance_id: str):
        """
        Stops all user applications.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
        """
        command = "pm list packages -3 | cut -f 2 -d ':' | while read line; do am force-stop $line; done"
        self._execute_shell_command(address, instance_id, command)
        logger.info(f"All applications stopped on {address}")

    def _start_screen_recording(self, address: str, instance_id: str, game_id: str, video_id: str):
        """
        Starts screen recording.
        """
        recording_file_path = f"/sdcard/recordings/recording_{game_id}_{video_id}.mp4"
        create_dir_command = f"mkdir -p /sdcard/recordings"
        self._execute_shell_command(address, instance_id, create_dir_command)

        # Set recording timeout to 900 seconds (15 minutes)
        command = f"screenrecord --time-limit 900 {recording_file_path}"
        self._execute_shell_command(address, instance_id, f"{command} &")
        logger.info(f"Screen recording started, saving to {recording_file_path}")

    def _stop_screen_recording(self, address: str, instance_id: str):
        """
        Stops screen recording on the device.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
        """
        shell_command = "pkill -INT screenrecord"
        self._execute_shell_command(address, instance_id, shell_command)

    def _list_recording_files(self, address: str, instance_id: str) -> List[str]:
        """
        Lists all recording files on the device.
        """
        command = "ls /sdcard/recordings/"
        result = self._execute_shell_command(address, instance_id, command)
        output = result.get("results", [{}])[0].get("stdout", "")
        filenames = output.strip().split("\n")
        file_list = [
            f"/sdcard/recordings/{filename}"
            for filename in filenames
            if filename.startswith("recording_") and filename.endswith(".mp4")
        ]
        return file_list

    def _pull_file_from_device(self, address: str, instance_id: str, device_path: str, local_path: str):
        """
        Pulls a file from the device to the local filesystem.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            device_path (str): The path to the file on the device.
            local_path (str): The local path where the file will be saved.
        """
        # Download the file using the Genymotion API
        endpoint = "/files"
        params = {"guest_filepath": device_path}

        response = genymotion_request(
            address=address,
            instance_id=instance_id,
            method="GET",
            endpoint=endpoint,
            params=params,
            verify_ssl=True,
            stream=True,
        )

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"File {device_path} pulled from device to {local_path}")

    def set_kiosk_mode(self, session_id: str, enabled: bool) -> None:
        """
        Enable or disable kiosk mode on the instance.

        Args:
            session_id (str): The session ID.
            enabled (bool): True to enable, False to disable.
        """
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return

        endpoint = "/configuration/kiosk"
        method = "POST" if enabled else "DELETE"

        try:
            response = genymotion_request(
                address=address,
                instance_id=instance_id,
                method=method,
                endpoint=endpoint,
                verify_ssl=True,  # SSL certificate should be valid
            )
            logger.info(f"Kiosk mode {'enabled' if enabled else 'disabled'} for session {session_id}")
        except requests.HTTPError as e:
            logger.error(f"Error setting kiosk mode for session {session_id}: {e}")
            raise

    def set_internet_access(self, session_id: str, enabled: bool) -> None:
        """
        Enable or disable internet access (WiFi and cellular) on the instance.

        Args:
            session_id (str): The session ID.
            enabled (bool): True to enable, False to disable.
        """
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return

        endpoint = "/network/baseband"
        data = {"state": enabled}

        try:
            response = genymotion_request(
                address=address,
                instance_id=instance_id,
                method="POST",
                endpoint=endpoint,
                data=data,
                verify_ssl=True,  # SSL certificate should be valid
            )
            logger.info(f"Internet access {'enabled' if enabled else 'disabled'} for session {session_id}")
        except requests.HTTPError as e:
            logger.error(f"Error setting internet access for session {session_id}: {e}")
            raise

    def cleanup_session(self, session_id: str) -> None:
        """
        Performs the cleaning of the session.

        Args:
            session_id (str): The session ID.
        """
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        try:
            # Stop screen recording (if any)
            self._stop_screen_recording(address, instance_id)

            # Close all open applications
            self._stop_all_applications(address, instance_id)

            logger.info(f"Session {session_id} cleaned up.")
        except Exception as e:
            logger.error(f"Error cleaning up session {session_id}: {e}")
            raise

    def start_game_in_session(self, session_id: str, game_id: str, virtual_keyboard: bool = True) -> None:
        """
        Starts the game in the session.

        Args:
            session_id (str): The session ID.
            game_id (str): The game ID.
            virtual_keyboard (bool): Whether to enable the virtual keyboard.
        """
        session = self.session_model.get_item_by_id(session_id)
        game = self.game_model.get_item_by_id(game_id)

        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        if not game:
            logger.error(f"Game {game_id} not found.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return

        try:
            # Set screen orientation
            self._set_screen_orientation(address, instance_id, game.screen_orientation)

            # Enable or disable virtual keyboard
            self._set_virtual_keyboard(address, instance_id, virtual_keyboard)

            # Enable or disable WiFi
            self.set_internet_access(session_id, game.wifi_enabled)

            # Enable kiosk mode
            self.set_kiosk_mode(session_id, enabled=True)

            # Launch the game application
            self._launch_application(address, instance_id, game.android_package_name)

            # Generate recording_id
            recording_id = ksuid().__str__()

            # Start screen recording
            self._start_screen_recording(address, instance_id, game.SK, recording_id)

            logger.info(f"Game {game.name} started in session {session_id}")
        except Exception as e:
            logger.error(f"Error starting game in session {session_id}: {e}")
            raise

    def upload_all_recordings_to_s3(self, session_id: str) -> None:
        """
        Uploads all recordings from the device to S3 and creates Video entries.
        """
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        try:
            # List all recording files
            file_list = self._list_recording_files(address, instance_id)

            if not file_list:
                logger.info(f"No recordings found for session {session_id}")
                return

            for file_path in file_list:
                filename = os.path.basename(file_path)
                if filename.startswith("recording_") and filename.endswith(".mp4"):
                    parts = filename[len("recording_") : -len(".mp4")].split("_")
                    if len(parts) == 2:
                        game_id, video_id = parts
                    else:
                        logger.warning(f"Unexpected recording file name format: {filename}")
                        continue

                    # Create Video entry
                    video = self.video_model.create_video(
                        video_id=video_id,
                        session_id=session_id,
                        game_id=game_id,
                    )

                    # Pull file from device
                    local_path = f"/tmp/{video_id}.mp4"
                    self._pull_file_from_device(address, instance_id, file_path, local_path)

                    # Upload to S3
                    s3 = boto3.client("s3")
                    s3_key = f"recordings/{video_id}.mp4"
                    s3.upload_file(local_path, self.s3_bucket_name, s3_key)
                    logger.info(f"Uploaded recording {video_id} to S3 at {s3_key}")

                    # Update Video entry with size
                    size = os.path.getsize(local_path)
                    self.video_model.update_video_size_and_duration(video_id, size=size)

                    # Clean up
                    os.remove(local_path)

            logger.info(f"All recordings uploaded for session {session_id}")

        except Exception as e:
            logger.error(f"Error uploading recordings for session {session_id}: {e}")
            raise

    def stop_game_in_session(self, session_id: str) -> None:
        """
        Stops the game in the session.

        Args:
            session_id (str): The session ID.
        """
        session = self.session_model.get_item_by_id(session_id)

        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return

        address = session.instance.secure_address
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return

        try:
            # Stop screen recording
            self._stop_screen_recording(address, instance_id)

            # Stop the game application
            self._stop_all_applications(address, instance_id)

            # Disable kiosk mode
            self.set_kiosk_mode(session_id, enabled=False)

            # Enable internet access
            self.set_internet_access(session_id, enabled=True)

            logger.info(f"Game stopped in session {session_id}")
        except Exception as e:
            logger.error(f"Error stopping game in session {session_id}: {e}")
            raise
