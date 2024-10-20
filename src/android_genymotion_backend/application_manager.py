import logging
import os
from typing import List, Optional

import boto3
import requests
from ksuid import ksuid


from domain import SessionModel, GameModel, VideoModel, logger
from utils import genymotion_request, execute_shell_command


class ApplicationManager:
    def __init__(self) -> None:
        self.session_model = SessionModel()
        self.game_model = GameModel()
        self.video_model = VideoModel()
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME", "android-project")

    def _get_address_and_instance_id(self, session_id: str) -> tuple[Optional[str], Optional[str]]:
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return None, None

        address = self.session_model.domain_name(session.SK)
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return None, instance_id

        return address, instance_id

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

        execute_shell_command(address, instance_id, command, logger)
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
        execute_shell_command(address, instance_id, command, logger)
        logger.info(f"Application {package_name} launched on {address}")

    def _stop_all_applications(self, address: str, instance_id: str):
        """
        Stops all user applications.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
        """
        command = "pm list packages -3 | cut -f 2 -d ':' | while read line; do am force-stop $line; done"
        execute_shell_command(address, instance_id, command, logger)
        logger.info(f"All applications stopped on {address}")

    # def _start_screen_recording(self, address: str, instance_id: str, game_id: str, video_id: str):
    #     """
    #     Starts screen recording.
    #     """
    #     recording_file_path = f"/sdcard/recordings/recording_{game_id}_{video_id}.mp4"
    #     create_dir_command = f"mkdir -p /sdcard/recordings"
    #     self._execute_shell_command(address, instance_id, create_dir_command, logger)
    #
    #     # Set recording timeout to 900 seconds (15 minutes)
    #     command = f"screenrecord --time-limit 900 {recording_file_path}"
    #     self._execute_shell_command(address, instance_id, f"{command} &", logger)
    #     logger.info(f"Screen recording started, saving to {recording_file_path}")
    #
    #
    # def _stop_screen_recording(self, address: str, instance_id: str):
    #     """
    #     Stops screen recording on the device.
    #
    #     Args:
    #         address (str): The secure address.
    #         instance_id (str): The instance ID.
    #     """
    #     shell_command = "pkill -INT screenrecord"
    #     self._execute_shell_command(address, instance_id, shell_command, logger)
    #     logger.info("Screen recording stopped.")

    def _start_screen_recording(self, address: str, instance_id: str, game_id: str, video_id: str):
        """
        Starts long-duration screen recording by chaining multiple screenrecord commands.
        """
        # Create a control file path
        control_file = "/sdcard/recordings/stop_recording.flag"

        # Properly format the shell script with semicolons and redirects
        script = f"""
            touch {control_file};
            mkdir -p /sdcard/recordings;
            counter=1;
            while [ ! -f {control_file} ]; do
                screenrecord --time-limit 180 "/sdcard/recordings/recording_{game_id}_{video_id}_part${{counter}}.mp4";
                counter=$((counter + 1));
            done;
            rm {control_file};
            """

        # Combine the script into a single line and redirect all output to /dev/null
        full_command = f"nohup sh -c '{script}' >/dev/null 2>&1 &"

        execute_shell_command(address, instance_id, full_command)
        logger.info("Long-duration screen recording started.")

    def _stop_screen_recording(self, address: str, instance_id: str):
        """
        Stops long-duration screen recording by creating a stop flag.
        """
        # Path to the control file
        control_file = "/sdcard/recordings/stop_recording.flag"

        # Create the stop flag file
        execute_shell_command(address, instance_id, f"touch {control_file}")
        logger.info("Screen recording stop signal sent.")

    def _list_recording_files(self, address: str, instance_id: str) -> List[str]:
        """
        Lists all recording files on the device.
        """
        command = "ls /sdcard/recordings/"
        result = execute_shell_command(address, instance_id, command, logger)
        logger.info(f"Listing recording files on {address}")
        filenames = result.text.strip().split("\n")
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
        params = {"path": device_path}

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
        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
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
        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
            return

        # Set root access to 3 (always allow)
        endpoint = "/configuration/properties/persist.sys.root_access"
        genymotion_request(
            address=address, instance_id=instance_id, method="POST", endpoint=endpoint, data={"value": 3}, logger=logger
        )

        command = f"su -c 'svc data enable'" if enabled else f"su -c 'svc data disable'"
        execute_shell_command(address, instance_id, command, logger)
        print("Still working")

        command = f"su -c 'svc wifi enable'" if enabled else f"su -c 'svc wifi disable'"
        execute_shell_command(address, instance_id, command, logger)
        print("Still working")

        # Disable root access
        genymotion_request(
            address=address, instance_id=instance_id, method="POST", endpoint=endpoint, data={"value": 0}, logger=logger
        )
        logger.info(f"Internet access {'enabled' if enabled else 'disabled'} for session {session_id}")

    def cleanup_session(self, session_id: str) -> None:
        """
        Performs the cleaning of the session.

        Args:
            session_id (str): The session ID.
        """
        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
            return

        try:
            # Stop screen recording (if any)
            self._stop_screen_recording(address, instance_id)

            # Close all open applications
            self._stop_all_applications(address, instance_id)

            # Set screen orientation to vertical
            self._set_screen_orientation(address, instance_id, "vertical")

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
        game = self.game_model.get_item_by_id(game_id)

        if not game:
            logger.error(f"Game {game_id} not found.")
            return

        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
            return

        try:
            # Set screen orientation
            self._set_screen_orientation(address, instance_id, game.screen_orientation)

            # Enable or disable virtual keyboard
            self._set_virtual_keyboard(address, instance_id, virtual_keyboard)

            # Enable or disable WiFi
            self.set_internet_access(session_id, game.wifi_enabled)

            # Launch the game application
            self._launch_application(address, instance_id, game.android_package_name)

            # Enable kiosk mode
            self.set_kiosk_mode(session_id, enabled=True)

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
        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
            return

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
                    if len(parts) == 3:
                        game_id, video_id, part_name = parts
                    else:
                        logger.warning(f"Unexpected recording file name format: {filename}")
                        continue

                    video_id = f"{video_id}_{part_name}"

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
        address, instance_id = self._get_address_and_instance_id(session_id)
        if not address:
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

            # Set screen orientation to vertical
            self._set_screen_orientation(address, instance_id, "vertical")

            logger.info(f"Game stopped in session {session_id}")
        except Exception as e:
            logger.error(f"Error stopping game in session {session_id}: {e}")
            raise
