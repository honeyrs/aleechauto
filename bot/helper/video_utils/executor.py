from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from aioshutil import rmtree
from ast import literal_eval
from asyncio import create_subprocess_exec, gather, Event, Semaphore, wait_for, TimeoutError
from asyncio.subprocess import PIPE
from mimetypes import guess_type
from natsort import natsorted
from os import path as ospath, walk
from time import time

from bot import bot, task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME, config_dict, queue_dict_lock, non_queued_up, non_queued_dl, queued_up, queued_dl, aria2, Intervals, DATABASE_URL
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_target, clean_download
from bot.helper.ext_utils.media_utils import FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage, update_status_message, delete_status

UPLOAD_SEMAPHORE = Semaphore(3)

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

class VidEcxecutor(FFProgress, TaskConfig):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        FFProgress.__init__(self)
        TaskConfig.__init__(self)
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
        self._processed_bytes = 0
        self._last_uploaded = 0
        self._start_time = time()
        self.mid = listener.mid
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.mid}, path: {self.path}")

    @staticmethod
    async def clean():
        try:
            if st := Intervals['status']:
                for intvl in list(st.values()):
                    intvl.cancel()
            Intervals['status'].clear()
            await gather(sync_to_async(aria2.purge), delete_status())
        except:
            pass

    async def _cleanup(self):
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
            LOGGER.info(f"Cleanup completed for MID: {self.mid}")
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

    async def _extract_zip(self, zip_path):
        extract_dir = ospath.join(ospath.dirname(zip_path), f"extracted_{self._gid}")
        try:
            await makedirs(extract_dir, exist_ok=True)
            cmd = ['7z', 'x', zip_path, f'-o{extract_dir}', '-y']
            _, stderr, rcode = await cmd_exec(cmd)
            if rcode != 0:
                LOGGER.error(f"Failed to extract ZIP: {stderr}")
                await rmtree(extract_dir, ignore_errors=True)
                return None
            LOGGER.info(f"Extracted ZIP to {extract_dir}")
            return extract_dir
        except Exception as e:
            LOGGER.error(f"ZIP extraction error: {e}")
            await rmtree(extract_dir, ignore_errors=True)
            return None

    async def _is_media_file(self, file_path):
        """Check if a file is a video or audio file using mimetypes."""
        mime_type, _ = guess_type(file_path)
        return mime_type and (mime_type.startswith('video') or mime_type.startswith('audio'))

    async def _get_files(self):
        file_list = []
        if self._metadata:
            file_list.append(self.path)
        elif await aiopath.isfile(self.path):
            if self.path.lower().endswith('.zip'):
                extract_dir = await self._extract_zip(self.path)
                if extract_dir:
                    self._files.append(extract_dir)
                    for dirpath, _, files in await sync_to_async(walk, extract_dir):
                        for file in natsorted(files):
                            file_path = ospath.join(dirpath, file)
                            if await self._is_media_file(file_path):
                                file_list.append(file_path)
                                LOGGER.info(f"Found media file: {file_path}")
            elif await self._is_media_file(self.path):
                file_list.append(self.path)
        else:
            for dirpath, _, files in await sync_to_async(walk, self.path):
                for file in natsorted(files):
                    file_path = ospath.join(dirpath, file)
                    if await self._is_media_file(file_path):
                        file_list.append(file_path)
                        LOGGER.info(f"Found media file: {file_path}")
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
        return file_list

    async def _upload_progress(self, current, _):
        if self.is_cancelled:
            bot.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _upload_file(self, file_path):
        async with UPLOAD_SEMAPHORE:
            try:
                LOGGER.info(f"Uploading file for MID: {self.mid}: {file_path}")
                caption = f"<code>{ospath.basename(file_path)}</code>"
                self._send_msg = await bot.send_document(
                    chat_id=self.listener.message.chat.id,
                    document=file_path,
                    caption=caption,
                    disable_notification=True,
                    progress=self._upload_progress,
                    reply_to_message_id=self.listener.message.id
                )
                if not self._send_msg or not hasattr(self._send_msg, 'link'):
                    raise ValueError("Upload failed: No valid message returned")
                LOGGER.info(f"Upload completed for MID: {self.mid}: {file_path}")
                return self._send_msg.link
            except TimeoutError:
                LOGGER.error(f"Upload timed out for MID: {self.mid}: {file_path}")
                self.is_cancelled = True
                await self.listener.onUploadError("Upload timed out after 10 minutes")
                return None
            except Exception as e:
                LOGGER.error(f"Upload error for MID: {self.mid}: {e}")
                self.is_cancelled = True
                await self.listener.onUploadError(f"Upload failed: {e}")
                return None

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            await self.listener.onUploadError("Invalid video mode configuration.")
            return None

        LOGGER.info(f"Executing {self.mode} with name: {self.name} for MID: {self.mid}")
        file_list = await self._get_files()
        if not file_list:
            await sendMessage("No valid video or audio files found.", self.listener.message)
            await self._cleanup()
            await self.listener.onUploadError("No files to process or upload.")
            return None

        # Queue management
        add_to_queue, event = False, None
        if config_dict.get('QUEUE_ALL') or config_dict.get('QUEUE_COMPLETE'):
            async with queue_dict_lock:
                if self.mid not in non_queued_up:
                    add_to_queue = True
                    event = Event()
                    queued_up[self.mid] = event
                    LOGGER.info(f"Added to Queue/Upload: {self.name}")
                    async with task_dict_lock:
                        task_dict[self.mid] = QueueStatus(self, self.size, self._gid, 'Up')
            if add_to_queue:
                await event.wait()
                async with task_dict_lock:
                    if self.mid not in task_dict:
                        return None
                LOGGER.info(f"Starting from Queued/Upload: {self.name}")

        async with queue_dict_lock:
            non_queued_up.add(self.mid)

        try:
            if self.mode == 'merge_rmaudio':
                result = await self._merge_and_rmaudio(file_list)
                if result and not self.is_cancelled:
                    uploaded_link = await self._upload_file(result)
                    if uploaded_link:
                        await self.listener.onUploadComplete(uploaded_link, self.size, {uploaded_link: self.name}, 1, 0)
                    else:
                        await self._cleanup()
                        await self.listener.onUploadError("Upload failed after processing.")
                        return None
            else:
                LOGGER.error(f"Unsupported mode: {self.mode}")
                result = None
            if self.is_cancelled or not result:
                await self._cleanup()
                await self.listener.onUploadError(f"{self.mode} processing failed.")
                return None
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            await self.listener.onUploadError(f"Failed to process {self.mode}.")
            return None
        finally:
            await self._finalize_task()

    async def _finalize_task(self):
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
            count = len(task_dict)
        LOGGER.info(f"Task {self.mid} removed from task_dict")
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.listener.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]

        await clean_download(self.path)
        if config_dict.get('INCOMPLETE_TASK_NOTIFIER') and DATABASE_URL:
            await Dba await DbManager().rm_complete_task(self.listener.message.link)

    @new_task
    async def _start_handler(self, *args):
        from bot.helper.video_utils.extra_selector import ExtraSelect
        selector = ExtraSelect(self)
        await selector.get_buttons(*args)

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update: {status} for MID: {self.mid}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    async def _final_path(self, outfile=''):
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
            LOGGER.info(f"Final path for MID: {self.mid}: {self._up_path}")
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
            LOGGER.info(f"Running FFmpeg cmd for MID: {self.mid}: {' '.join(cmd)}")
            process = await create_subprocess_exec(*cmd, stderr=PIPE)
            self.listener.suproc = process
            _, code = await gather(self.progress(status), process.wait())
            if code == 0:
                LOGGER.info(f"FFmpeg succeeded for MID: {self.mid}")
                if not self.listener.seed:
                    await gather(*[clean_target(file) for file in self._files])
                self._files.clear()
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info(f"FFmpeg cancelled for MID: {self.mid}")
            else:
                error_msg = (await process.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error for MID: {self.mid}: {error_msg}")
                self.is_cancelled = True
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}", exc_info=True)
            self.is_cancelled = True
            return False

    async def _merge_and_rmaudio(self, file_list):
        streams = await get_metavideo(file_list[0])
        if not streams:
            LOGGER.error(f"No streams found in {file_list[0]} for MID: {self.mid}")
            await sendMessage("No streams found in the video file.", self.listener.message)
            return None

        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', multi=len(file_list) > 1)
        self._files = file_list
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0

        await self._start_handler(streams)
        try:
            await wait_for(self.event.wait(), timeout=180)
        except TimeoutError:
            LOGGER.error(f"Stream selection timed out for MID: {self.mid}")
            self.is_cancelled = True
            await self._cleanup()
            await self.listener.onUploadError("Stream selection timed out after 180 seconds.")
            return None

        if self.is_cancelled:
            LOGGER.info(f"Cancelled before processing for MID: {self.mid}")
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
                cmd = [FFMPEG_NAME, '-i', file_list[0]]

            cmd.extend(['-map', '0:v'])
            kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
            for stream in kept_streams:
                cmd.extend(['-map', stream])
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