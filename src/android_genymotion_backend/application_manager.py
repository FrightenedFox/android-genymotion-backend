import os
from typing import List

import boto3
from domain import AMIModel, GameModel, SessionModel, VideoModel, logger
from ksuid import ksuid
from utils import execute_shell_command, genymotion_request


class ApplicationManager:
    def __init__(self) -> None:
        self.session_model = SessionModel()
        self.game_model = GameModel()
        self.video_model = VideoModel()
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME", "android-project")

        # Path to the control file
        self.recordings_device_dir = "/sdcard/recordings"
        self.recordings_control_file = f"{self.recordings_device_dir}/stop_recording.flag"

    def _get_address_and_instance_id(self, session_id: str) -> tuple[str, str] | None:
        session = self.session_model.get_item_by_id(session_id)
        if not session or not session.instance:
            logger.error(f"Session {session_id} not found or instance not available.")
            return None

        address = session.domain_name
        instance_id = session.instance.instance_id

        if not address:
            logger.error(f"Secure address for session {session_id} not found.")
            return None

        return address, instance_id

    def _set_screen_orientation(self, address: str, instance_id: str, orientation: str) -> None:
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

        try:
            genymotion_request(
                address=address,
                instance_id=instance_id,
                method="POST",
                endpoint=endpoint,
                data=data,
                verify_ssl=True,
                timeout=5,
                logger=logger,
            )
            logger.info(f"Screen orientation set to {orientation} on {address}")
        except Exception as e:
            logger.error(f"Error setting screen orientation to {orientation} on {address}: {e}")

    def _set_virtual_keyboard(self, address: str, instance_id: str, enabled: bool) -> None:
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

        try:
            execute_shell_command(address, instance_id, command, logger, timeout=5)
        except Exception as e:
            logger.error(f"Error setting virtual keyboard on {address}: {e}")
            return
        else:
            logger.info(f"Virtual keyboard {'enabled' if enabled else 'disabled'} on {address}")

    def _launch_application(self, address: str, instance_id: str, package_name: str, session_id: str) -> None:
        """
        Launches the application.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
            package_name (str): The package name of the application.
        """
        command = f"monkey -p {package_name} -c android.intent.category.LAUNCHER 1"
        try:
            execute_shell_command(address, instance_id, command, logger)
        except Exception as e:
            import time

            logger.error(
                f"Error launching application {package_name} on {address}: {e}. Retrying by reconfiguring SSL."
            )
            instance_info = self.session_model.instance_model.get_instance_info(instance_id)
            self.session_model.configure_instance_certificate(session_id, instance_info)

            time.sleep(7)

            execute_shell_command(address, instance_id, command, logger)
            logger.info(f"Application {package_name} launched on {address} after reconfiguring SSL.")
        else:
            logger.info(f"Application {package_name} launched on {address}")

    def _stop_all_applications(self, address: str, instance_id: str) -> None:
        """
        Stops all user applications.

        Args:
            address (str): The secure address.
            instance_id (str): The instance ID.
        """
        command = "pm list packages -3 | cut -f 2 -d ':' | while read line; do am force-stop $line; done"
        try:
            execute_shell_command(address, instance_id, command, logger)
        except Exception as e:
            logger.error(f"Error stopping applications on {address}: {e}")
        else:
            logger.info(f"All applications stopped on {address}")

    def _start_screen_recording(self, address: str, instance_id: str, game_id: str, video_id: str) -> None:
        """
        Starts long-duration screen recording by chaining multiple screenrecord commands.
        """
        # Properly format the shell script with semicolons and redirects
        recording_file = f"recording_{game_id}_{video_id}_part${{counter}}.mp4"
        script = f"""
            if [ ! -d {self.recordings_device_dir} ]; then
                mkdir -p {self.recordings_device_dir};
            elif [ -f {self.recordings_control_file} ]; then
                rm {self.recordings_control_file};
            fi;

            counter=1;
            while [ ! -f {self.recordings_control_file} ]; do
                screenrecord --bit-rate 8000000 --time-limit 180 "{self.recordings_device_dir}/{recording_file}";
                counter=$((counter + 1));
            done;
            rm {self.recordings_control_file};
            """

        # Combine the script into a single line and redirect all output to /dev/null
        full_command = f"nohup sh -c '{script}' >/dev/null 2>&1 &"

        execute_shell_command(address, instance_id, full_command, logger=logger)
        logger.info("Long-duration screen recording started.")

    def _stop_screen_recording(self, address: str, instance_id: str) -> None:
        """
        Stops long-duration screen recording by creating a stop flag.
        """
        import time

        # Create the stop flag file
        execute_shell_command(address, instance_id, f"touch {self.recordings_control_file}", logger=logger)
        execute_shell_command(address, instance_id, "pkill -INT screenrecord", logger=logger)
        logger.info("Screen recording stop signal sent.")

        # Remove the stop flag file after 1 second
        time.sleep(1)
        execute_shell_command(address, instance_id, f"rm {self.recordings_control_file}", logger=logger)

    def _list_recording_files(self, address: str, instance_id: str) -> List[str]:
        """
        Lists all recording files on the device.
        """
        command = "ls /sdcard/recordings/"
        result = execute_shell_command(address, instance_id, command, logger=logger)
        logger.info(f"Listing recording files on {address}")
        filenames = result.text.strip().split("\n")
        file_list = [
            f"/sdcard/recordings/{filename}"
            for filename in filenames
            if filename.startswith("recording_") and filename.endswith(".mp4")
        ]
        logger.info(f"Found {len(file_list)} recording files on {address}: \n{file_list}")
        return file_list

    def _pull_file_from_device(self, session_id: str, instance_id: str, device_path: str, local_path: str) -> None:
        """
        Pulls a file from the device to the local filesystem.

        Args:
            session_id (str): The session ID.
            instance_id (str): The instance ID.
            device_path (str): The path to the file on the device.
            local_path (str): The local path where the file will be saved.
        """
        # Download the file using the Genymotion API

        session = self.session_model.get_session_by_id(session_id)
        if not session:
            logger.error(f"Session {session_id} not found, unable to pull file from device.")
            return

        ami_model = AMIModel()
        ami_info = ami_model.get_ami_by_id(session.ami_id)
        if float(ami_info.android_version) >= 9.0:
            endpoint = "/files"
            params = {"path": device_path}
            response = genymotion_request(
                address=session.domain_name,
                instance_id=instance_id,
                method="GET",
                endpoint=endpoint,
                params=params,
                verify_ssl=True,
                stream=True,
                logger=logger,
            )
        else:
            endpoint = f"/files{device_path}"
            response = genymotion_request(
                address=session.domain_name,
                instance_id=instance_id,
                method="GET",
                endpoint=endpoint,
                verify_ssl=True,
                stream=True,
                logger=logger,
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
        logger.info(f"Setting kiosk mode {'enabled' if enabled else 'disabled'} for session {session_id}")
        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

        endpoint = "/configuration/kiosk"
        method = "POST" if enabled else "DELETE"

        try:
            genymotion_request(
                address=address,
                instance_id=instance_id,
                method=method,
                endpoint=endpoint,
                verify_ssl=True,
                timeout=5,
                logger=logger,
            )
            logger.info(f"Kiosk mode {'enabled' if enabled else 'disabled'} for session {session_id}")
        except Exception as e:
            logger.error(f"Error setting kiosk mode for session {session_id}: {e}")

    def set_internet_access(self, session_id: str, enabled: bool) -> None:
        """
        Enable or disable internet access (Wi-Fi and cellular) on the instance.

        Args:
            session_id (str): The session ID.
            enabled (bool): True to enable, False to disable.
        """
        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

        # Set root access to 3 (always allow)
        endpoint = "/configuration/properties/persist.sys.root_access"
        try:
            genymotion_request(
                address=address,
                instance_id=instance_id,
                method="POST",
                endpoint=endpoint,
                data={"value": 3},
                logger=logger,
                timeout=7,
            )
        except Exception as e:
            logger.error(f"Error setting root access for session {session_id}: {e}")
        else:
            logger.info(f"Root access set to 3 for session {session_id}")

        # Repeat the command 3 times to ensure it is executed
        for i in range(3):
            command = "su -c 'svc data enable'" if enabled else "su -c 'svc data disable'"
            execute_shell_command(address, instance_id, command, logger)

            command = "su -c 'svc wifi enable'" if enabled else "su -c 'svc wifi disable'"
            execute_shell_command(address, instance_id, command, logger)

        # Disable root access
        try:
            genymotion_request(
                address=address,
                instance_id=instance_id,
                method="POST",
                endpoint=endpoint,
                data={"value": 0},
                logger=logger,
                timeout=7,
            )
        except Exception as e:
            logger.error(f"Error setting internet access for session {session_id}: {e}")
        else:
            logger.info(f"Internet access {'enabled' if enabled else 'disabled'} for session {session_id}")

    def cleanup_session(self, session_id: str) -> None:
        """
        Performs the cleaning of the session.

        Args:
            session_id (str): The session ID.
        """
        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

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

        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

        try:
            # Set screen orientation
            self._set_screen_orientation(address, instance_id, game.screen_orientation)

            # Enable or disable virtual keyboard
            self._set_virtual_keyboard(address, instance_id, virtual_keyboard)

            # Enable or disable WiFi
            self.set_internet_access(session_id, game.wifi_enabled)

            # Launch the game application
            self._launch_application(address, instance_id, game.android_package_name, session_id)

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
        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

        try:
            # List all recording files
            file_list = self._list_recording_files(address, instance_id)

            if not file_list:
                logger.info(f"No recordings found for session {session_id}")
                return

            for file_path in file_list:
                try:
                    filename = os.path.basename(file_path)
                    if filename.startswith("recording_") and filename.endswith(".mp4"):
                        parts = filename[len("recording_") : -len(".mp4")].split("_")  # noqa: E203
                        if len(parts) == 3:
                            game_id, video_id, part_name = parts
                        else:
                            logger.warning(f"Unexpected recording file name format: {filename}")
                            continue

                        video_id = f"{video_id}_{part_name}"

                        # Create Video entry
                        self.video_model.create_video(
                            video_id=video_id,
                            session_id=session_id,
                            game_id=game_id,
                        )
                        logger.debug("Created Video entry for recording %s", video_id)

                        # Pull file from device
                        local_path = f"/tmp/{video_id}.mp4"
                        self._pull_file_from_device(session_id, instance_id, file_path, local_path)

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
                except Exception as e:
                    logger.error(f"Error processing recording file {file_path}: {e}")
                    continue

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
        addr_ins_id = self._get_address_and_instance_id(session_id)
        if not addr_ins_id:
            return
        address, instance_id = addr_ins_id

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
