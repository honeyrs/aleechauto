from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from aioshutil import rmtree
from ast import literal_eval
from asyncio import create_subprocess_exec, gather, Event, wait_for, TimeoutError as AsyncTimeoutError
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk

from bot import task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME, bot
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec
from bot.helper.ext_utils.files_utils import get_path_size, clean_target, clean_download
from bot.helper.ext_utils.media_utils import get_document_type
from bot.helper.ext_utils.task_manager import ffmpeg_queue, ffmpeg_queue_lock, active_ffmpeg, non_queued_up, queue_dict_lock, start_from_queued
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.message_utils import sendMessage

async def get_metavideo(video_file):
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

class VidEcxecutor:
    def __init__(self, listener, path: str, gid: str, metadata=False):
        self.listener = listener
        self.path = path
        self.gid = gid
        self.metadata = metadata
        self.name = ''
        self.outfile = ''
        self.size = 0
        self._up_path = path
        self._files = []
        self.is_cancelled = False
        self.event = Event()
        self.data = {}
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {self.path}")

    async def _cleanup(self):
        try:
            for f in self._files:
                if await aiopath.exists(f):
                    await clean_target(f)
            self._files.clear()
            input_file = ospath.join(self.path, f'input_{self.gid}.txt')
            if await aiopath.exists(input_file):
                await clean_target(input_file)
            self.data.clear()
            self.is_cancelled = True
            async with task_dict_lock:
                task_dict.pop(self.listener.mid, None)
            await clean_download(self.path)
            async with queue_dict_lock:
                if self.listener.mid in non_queued_up:
                    non_queued_up.remove(self.listener.mid)
            await start_from_queued()
            LOGGER.info(f"Cleanup completed for MID: {self.listener.mid}")
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

    async def _extract_zip(self, zip_path):
        extract_dir = ospath.join(ospath.dirname(zip_path), f"extracted_{self.gid}")
        try:
            await makedirs(extract_dir, exist_ok=True)
            cmd = ['7z', 'x', zip_path, f'-o{extract_dir}', '-y']
            _, stderr, rcode = await cmd_exec(cmd)
            if rcode != 0:
                LOGGER.error(f"Failed to extract ZIP: {stderr}")
                await rmtree(extract_dir, ignore_errors=True)
                return None
            LOGGER.info(f"Extracted ZIP to {extract_dir}")
            self._files.append(extract_dir)
            return extract_dir
        except Exception as e:
            LOGGER.error(f"ZIP extraction error: {e}")
            await rmtree(extract_dir, ignore_errors=True)
            return None

    async def _get_files(self):
        file_list = []
        if self.metadata:
            file_list.append(self.path)
        elif await aiopath.isfile(self.path):
            if self.path.lower().endswith('.zip'):
                extract_dir = await self._extract_zip(self.path)
                if extract_dir:
                    for dirpath, _, files in await sync_to_async(walk, extract_dir):
                        for file in natsorted(files):
                            file_path = ospath.join(dirpath, file)
                            if (await get_document_type(file_path))[0]:
                                file_list.append(file_path)
                                LOGGER.info(f"Found media file: {file_path}")
            elif (await get_document_type(self.path))[0]:
                file_list.append(self.path)
        else:
            for dirpath, _, files in await sync_to_async(walk, self.path):
                for file in natsorted(files):
                    file_path = ospath.join(dirpath, file)
                    if (await get_document_type(file_path))[0]:
                        file_list.append(file_path)
                        LOGGER.info(f"Found media file: {file_path}")
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
        return file_list

    async def execute(self):
        global active_ffmpeg
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except (AttributeError, ValueError) as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            await self.listener.onUploadError("Invalid video mode configuration.")
            return None

        LOGGER.info(f"Executing {self.mode} with name: {self.name} for MID: {self.listener.mid}")
        file_list = await self._get_files()
        if not file_list:
            await sendMessage("No valid video files found.", self.listener.message)
            await self._cleanup()
            return None

        event = Event()
        async with ffmpeg_queue_lock:
            if self.listener.mid in ffmpeg_queue:
                LOGGER.warning(f"MID {self.listener.mid} already in ffmpeg_queue, skipping")
                return None
            ffmpeg_queue[self.listener.mid] = (event, self.mode, file_list)
            LOGGER.info(f"Queued FFmpeg task for MID: {self.listener.mid}, mode: {self.mode}")

        try:
            await wait_for(event.wait(), timeout=600)  # Increased timeout to ensure FFmpeg completes
            if active_ffmpeg != self.listener.mid:
                LOGGER.warning(f"FFmpeg not active for MID: {self.listener.mid}, retrying queue")
                return await self.execute()  # Retry if not active
        except AsyncTimeoutError:
            LOGGER.error(f"FFmpeg queue timeout for MID: {self.listener.mid}")
            async with ffmpeg_queue_lock:
                ffmpeg_queue.pop(self.listener.mid, None)
                if active_ffmpeg == self.listener.mid:
                    active_ffmpeg = None
            await self._cleanup()
            await self.listener.onUploadError("FFmpeg processing timed out.")
            return None

        try:
            if self.mode == 'merge_rmaudio':
                result = await self._merge_and_rmaudio(file_list)
            else:
                LOGGER.error(f"Unsupported mode: {self.mode}")
                result = None
            if self.is_cancelled or not result:
                await self._cleanup()
                await self.listener.onUploadError(f"{self.mode} processing failed.")
                return None
            LOGGER.info(f"FFmpeg completed for MID: {self.listener.mid}")
            async with ffmpeg_queue_lock:
                if active_ffmpeg == self.listener.mid:
                    active_ffmpeg = None
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode} for MID: {self.listener.mid}: {e}")
            await self._cleanup()
            await self.listener.onUploadError(f"Failed to process {self.mode}.")
            return None

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                if self.listener.mid in task_dict:
                    task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self.gid, status)
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    async def _final_path(self, outfile=''):
        try:
            if self.metadata:
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
            LOGGER.info(f"Final path set: {self._up_path} for MID: {self.listener.mid}")
            self.listener.name = ospath.basename(self._up_path)
            return self._up_path
        except Exception as e:
            LOGGER.error(f"Final path error: {e}")
            await self._cleanup()
            return None

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        base_dir, file_name = ospath.split(path)
        if not self.name or multi:
            if info:
                if await aiopath.isfile(path):
                    file_name = file_name.rsplit('.', 1)[0]
                file_name += f'_{info}.mkv'
                LOGGER.info(f"Generated name: {file_name}")
            self.name = file_name
        if not self.name.upper().endswith(('MKV', 'MP4')):
            self.name += '.mkv'
        LOGGER.info(f"Set name: {self.name} with base_dir: {base_dir}")
        return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg cmd: {' '.join(cmd)}")
            process = await create_subprocess_exec(*cmd, stderr=PIPE)
            self.listener.suproc = process
            _, code = await gather(process.wait(), process.stderr.read())
            if code == 0:
                LOGGER.info(f"FFmpeg succeeded for MID: {self.listener.mid}")
                return True
            error_msg = code.decode().strip() if isinstance(code, bytes) else str(code)
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info(f"FFmpeg cancelled for MID: {self.listener.mid}")
            else:
                LOGGER.error(f"FFmpeg error for MID: {self.listener.mid}: {error_msg}")
                self.is_cancelled = True
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error for MID: {self.listener.mid}: {e}")
            self.is_cancelled = True
            return False

    async def _merge_and_rmaudio(self, file_list):
        streams = await get_metavideo(file_list[0])
        if not streams:
            LOGGER.error(f"No streams found in {file_list[0]}")
            await sendMessage("No streams found in the video file.", self.listener.message)
            return None

        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', multi=len(file_list) > 1)
        self._files = file_list
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0

        self.outfile = ospath.join(base_dir, self.name)
        input_file = ospath.join(base_dir, f'input_{self.gid}.txt')

        try:
            if len(file_list) > 1:
                async with aiopen(input_file, 'w') as f:
                    await f.write('\n'.join([f"file '{f}'" for f in file_list]))
                cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file]
            else:
                cmd = [FFMPEG_NAME, '-i', file_list[0]]

            cmd.extend(['-map', '0:v', '-c', 'copy', self.outfile, '-y'])
            if not await self._run_cmd(cmd, 'direct'):
                await sendMessage("Merging failed due to FFmpeg error.", self.listener.message)
                return None
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error in _merge_and_rmaudio for MID: {self.listener.mid}: {e}")
            await sendMessage("Processing failed.", self.listener.message)
            return None
        finally:
            if await aiopath.exists(input_file):
                await clean_target(input_file)