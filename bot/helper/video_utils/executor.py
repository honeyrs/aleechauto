from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from aioshutil import rmtree
from ast import literal_eval
from asyncio import create_subprocess_exec, gather, Event, wait_for
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk

from bot import task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.media_utils import get_document_type, FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage
from bot.helper.video_utils.extra_selector import ExtraSelect
from bot.helper.video_utils.task_coordinator import TaskCoordinator

async def get_metavideo(video_file):
    """Fetches metadata streams from a video file using ffprobe."""
    try:
        stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_streams', video_file])
        if rcode != 0:
            LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
            return []
        metadata = literal_eval(stdout)
        return metadata.get('streams', [])
    except Exception as e:
        LOGGER.error(f"Error in get_metavideo: {e}")
        return []

class VidEcxecutor(FFProgress):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        super().__init__()
        self.data = {}
        self.event = Event()
        self.listener = listener
        self.path = path
        self.name = ''
        self.outfile = ''
        self.size = 0
        self._metadata = metadata
        self._up_path = path
        self._gid = gid
        self._files = []
        self.is_cancelled = False
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {self.path}")

    async def _cleanup(self):
        """Cleans up temporary files and resets state."""
        try:
            for f in self._files:
                if await aiopath.exists(f):
                    await clean_target(f)
            self._files.clear()
            input_file = ospath.join(self.path, f'input_{self._gid}.txt')
            if await aiopath.exists(input_file):
                await clean_target(input_file)
            self.data.clear()
            self.is_cancelled = True
            LOGGER.debug(f"Cleanup completed for {self.mode}")
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

    async def _extract_zip(self, zip_path):
        """Extracts ZIP/RAR archives using 7z."""
        extract_dir = ospath.join(ospath.dirname(zip_path), f"extracted_{self._gid}")
        try:
            await makedirs(extract_dir, exist_ok=True)
            cmd = ['7z', 'x', zip_path, f'-o{extract_dir}', '-y']
            _, stderr, rcode = await cmd_exec(cmd)
            if rcode != 0:
                LOGGER.error(f"Failed to extract ZIP/RAR: {stderr}")
                await rmtree(extract_dir, ignore_errors=True)
                return None
            LOGGER.info(f"Extracted ZIP/RAR to {extract_dir}")
            self._files.append(extract_dir)
            return extract_dir
        except Exception as e:
            LOGGER.error(f"ZIP/RAR extraction error: {e}")
            await rmtree(extract_dir, ignore_errors=True)
            return None

    async def _get_files(self):
        """Collects media files from path or archive."""
        file_list = []
        if self._metadata:
            file_list.append(self.path)
        elif await aiopath.isfile(self.path):
            if self.path.lower().endswith(('.zip', '.rar')):
                extract_dir = await self._extract_zip(self.path)
                if extract_dir:
                    self._files.append(extract_dir)
                    for dirpath, _, files in await sync_to_async(walk, extract_dir):
                        for file in natsorted(files):
                            file_path = ospath.join(dirpath, file)
                            if (await get_document_type(file_path))[0]:
                                file_list.append(file_path)
                                LOGGER.debug(f"Found media file: {file_path}")
            elif (await get_document_type(self.path))[0]:
                file_list.append(self.path)
        else:
            for dirpath, _, files in await sync_to_async(walk, self.path):
                for file in natsorted(files):
                    file_path = ospath.join(dirpath, file)
                    if (await get_document_type(file_path))[0]:
                        file_list.append(file_path)
                        LOGGER.debug(f"Found media file: {file_path}")
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
        return file_list

    async def execute(self):
        """Executes video processing based on mode."""
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            await TaskCoordinator.coordinate_error(self.listener, "Invalid video mode configuration.")
            return None

        LOGGER.info(f"Executing {self.mode} with name: {self.name}")
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            await TaskCoordinator.coordinate_error(self.listener, "No valid video files found.")
            return None

        try:
            if self.mode == 'merge_rmaudio':
                result = await self._merge_and_rmaudio(file_list)
            else:
                LOGGER.error(f"Unsupported mode: {self.mode}")
                await self._cleanup()
                await TaskCoordinator.coordinate_error(self.listener, f"Mode '{self.mode}' is not supported.")
                return None

            if self.is_cancelled or not result:
                await self._cleanup()
                await TaskCoordinator.coordinate_error(self.listener, f"{self.mode} processing failed or was cancelled.")
                return None
            
            LOGGER.info(f"Video processing completed for {self.mode}, final path: {result}")
            await TaskCoordinator.coordinate_success(self.listener, result)
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            await TaskCoordinator.coordinate_error(self.listener, f"Failed to process {self.mode}: {str(e)}")
            return None

    @new_task
    async def _start_handler(self, *args):
        """Initiates stream selection."""
        selector = ExtraSelect(self)
        await selector.get_buttons(*args)

    async def _send_status(self, status='wait'):
        """Sends status update to Telegram."""
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.debug(f"Sent status update: {status}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    async def _final_path(self, outfile=''):
        """Determines final output path and cleans up extras."""
        try:
            if self._metadata:
                self._up_path = outfile or self.outfile
            else:
                scan_dir = self._up_path if self._is_dir else ospath.split(self._up_path)[0]
                for dirpath, _, files in await sync_to_async(walk, scan_dir):
                    for file in files:
                        if file != ospath.basename(outfile or self.outfile):
                            await clean_target(ospath.join(dirpath, file))
                all_files = [(dirpath, file) for dirpath, _, files in await sync_to_async(walk, scan_dir) for file in files]
                if len(all_files) == 1:
                    self._up_path = ospath.join(*all_files[0])
            self._files.clear()
            LOGGER.info(f"Final path: {self._up_path}")
            return self._up_path
        except Exception as e:
            LOGGER.error(f"Final path error: {e}")
            await self._cleanup()
            return None

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        """Generates output name and base directory."""
        base_dir, file_name = ospath.split(path)
        if not self.name or multi:
            if info:
                if await aiopath.isfile(path):
                    file_name = file_name.rsplit('.', 1)[0]
                file_name += f'_{info}.mkv'
                LOGGER.debug(f"Generated name: {file_name}")
            self.name = file_name
        if not self.name.upper().endswith(('MKV', 'MP4')):
            self.name += '.mkv'
        LOGGER.debug(f"Set name: {self.name} with base_dir: {base_dir}")
        return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        """Runs FFmpeg command with progress tracking."""
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg cmd: {' '.join(cmd)}")
            process = await create_subprocess_exec(*cmd, stderr=PIPE)
            self.listener.suproc = process
            _, code = await gather(self.progress(status), process.wait())
            if code == 0:
                LOGGER.info("FFmpeg succeeded")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info("FFmpeg cancelled")
            else:
                error_msg = (await process.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error: {error_msg}")
                self.is_cancelled = True
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}", exc_info=True)
            self.is_cancelled = True
            return False

    async def _merge_and_rmaudio(self, file_list):
        """Merges files and removes unwanted audio streams."""
        streams = await get_metavideo(file_list[0]) if file_list else []
        if not streams and file_list:
            LOGGER.warning(f"No streams found in {file_list[0]}, proceeding with defaults")
            streams = []

        base_dir = await self._name_base_dir(file_list[0] if file_list else self.path, 'Merge-RemoveAudio', multi=len(file_list) > 1)
        self._files = file_list
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0

        await self._start_handler(streams)
        try:
            await wait_for(self.event.wait(), timeout=180)
        except Exception as e:
            LOGGER.error(f"Stream selection timeout or error: {e}")
            self.is_cancelled = True
            return None

        if self.is_cancelled:
            LOGGER.info("Cancelled in _merge_and_rmaudio.")
            return None

        streams_to_remove = self.data.get('streams_to_remove', [])
        self.outfile = ospath.join(base_dir, self.name)
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')

        try:
            if len(file_list) > 1:
                async with aiopen(input_file, 'w') as f:
                    await f.write('\n'.join([f"file '{f}'" for f in file_list]))
                cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file]
            else:
                cmd = [FFMPEG_NAME, '-i', file_list[0] if file_list else self.path]

            cmd.extend(['-map', '0:v'])
            if streams:
                kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
                for stream in kept_streams:
                    cmd.extend(['-map', stream])
            else:
                cmd.extend(['-map', '0:a?'])
            cmd.extend(['-c', 'copy', self.outfile, '-y'])

            if not await self._run_cmd(cmd, 'direct'):
                await sendMessage("Merging failed due to FFmpeg error.", self.listener.message)
                return None
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error in _merge_and_rmaudio: {e}", exc_info=True)
            await sendMessage("Processing failed.", self.listener.message)
            return None
        finally:
            if len(file_list) > 1:
                await clean_target(input_file)