from asyncio import create_subprocess_exec, gather, Event, Semaphore, wait_for, TimeoutError as AsyncTimeoutError
from aiofiles.os import path as aiopath, makedirs
from aioshutil import rmtree
from ast import literal_eval
from mimetypes import guess_type
from natsort import natsorted
from os import path as ospath, walk
from time import time

from bot import bot, task_dict, task_dict_lock, LOGGER, config_dict, queue_dict_lock, non_queued_up, queued_up, Intervals, DATABASE_URL
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec
from bot.helper.ext_utils.files_utils import get_path_size, clean_target, clean_download
from bot.helper.ext_utils.media_utils import FFProgress
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage, update_status_message, delete_status
from bot.helper.ext_utils.task_manager import start_from_queued, check_running_tasks

UPLOAD_SEMAPHORE = Semaphore(5)

async def get_metavideo(video_file):
    stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_streams', video_file])
    if rcode != 0:
        LOGGER.error(f"ffprobe error: {stderr}")
        return []
    return literal_eval(stdout).get('streams', [])

class VidEcxecutor(FFProgress):
    def __init__(self, listener, path, gid):
        super().__init__()
        self.listener = listener
        self.path = path
        self.gid = gid
        self.mid = listener.message.id
        self.name = listener.name
        self.mode, _, _ = listener.vidMode  # Extract mode from vidMode tuple
        self.size = 0
        self.outfile = ''
        self.event = Event()
        self.is_cancelled = False
        self._files = []
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.mid}, path: {self.path}, mode: {self.mode}")

    async def clean(self):
        try:
            for f in self._files:
                if await aiopath.exists(f):
                    await clean_target(f)
            self._files.clear()
            self.is_cancelled = True
            async with task_dict_lock:
                task_dict.pop(self.mid, None)
            await clean_download(self.path)
            if not task_dict:
                if st := Intervals['status']:
                    for intvl in list(st.values()):
                        intvl.cancel()
                    Intervals['status'].clear()
                await delete_status()
            await start_from_queued()
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

    async def _is_media_file(self, file_path):
        mime_type, _ = guess_type(file_path)
        return mime_type and (mime_type.startswith('video') or mime_type.startswith('audio'))

    async def _get_files(self):
        file_list = []
        if await aiopath.isfile(self.path) and await self._is_media_file(self.path):
            file_list.append(self.path)
        else:
            for dirpath, _, files in await sync_to_async(walk, self.path):
                for file in natsorted(files):
                    file_path = ospath.join(dirpath, file)
                    if await self._is_media_file(file_path):
                        file_list.append(file_path)
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
        return file_list

    async def _upload_file(self, file_path):
        async with UPLOAD_SEMAPHORE:
            try:
                LOGGER.info(f"Uploading file for MID: {self.mid}: {file_path}")
                caption = f"<code>{ospath.basename(file_path)}</code>"
                msg = await bot.send_document(
                    chat_id=self.listener.message.chat.id,
                    document=file_path,
                    caption=caption,
                    disable_notification=True,
                    reply_to_message_id=self.listener.message.id
                )
                if not msg or not hasattr(msg, 'link'):
                    raise ValueError("Upload failed: No valid message returned")
                LOGGER.info(f"Upload completed for MID: {self.mid}: {file_path}")
                return msg.link
            except Exception as e:
                LOGGER.error(f"Upload error for MID: {self.mid}: {e}")
                self.is_cancelled = True
                await self.listener.onUploadError(f"Upload failed: {e}")
                return None

    async def execute(self):
        try:
            LOGGER.info(f"Executing {self.mode} for MID: {self.mid}")
            file_list = await self._get_files()
            if not file_list:
                await self.listener.onUploadError("No valid media files found.")
                return None

            add_to_queue, event = await check_running_tasks(self.mid, "up")
            if add_to_queue:
                LOGGER.info(f"Added to Queue/Upload: {self.name} (MID: {self.mid})")
                async with task_dict_lock:
                    task_dict[self.mid] = QueueStatus(self.listener, self.size, self.gid, 'Up')
                await event.wait()
                async with task_dict_lock:
                    if self.mid not in task_dict:
                        return None
                LOGGER.info(f"Starting from Queue/Upload: {self.name} (MID: {self.mid})")
            async with queue_dict_lock:
                non_queued_up.add(self.mid)

            if self.mode == 'merge_rmaudio':
                result = await self._merge_and_rmaudio(file_list)
                if result and not self.is_cancelled:
                    link = await self._upload_file(result)
                    if link:
                        await self.listener.onUploadComplete(link, self.size, {link: ospath.basename(result)}, 1, None)
                        async with queue_dict_lock:
                            if self.mid in non_queued_up:
                                non_queued_up.remove(self.mid)
                        await start_from_queued()
                    else:
                        await self.clean()
                        return None
                else:
                    await self.clean()
                    return None
            else:
                await self.listener.onUploadError(f"Unsupported mode: {self.mode}")
                await self.clean()
                return None
            return result
        except Exception as e:
            LOGGER.error(f"Execution error for MID: {self.mid}: {e}", exc_info=True)
            await self.clean()
            await self.listener.onUploadError(f"Execution failed: {e}")
            return None

    async def _merge_and_rmaudio(self, file_list):
        streams = await get_metavideo(file_list[0])
        if not streams:
            await self.listener.onUploadError("No streams found in video file.")
            return None

        base_dir = ospath.dirname(file_list[0])
        self.outfile = ospath.join(base_dir, f"{self.name}_Merge-RemoveAudio.mkv")
        self._files = file_list

        from bot.helper.video_utils.extra_selector import ExtraSelect
        selector = ExtraSelect(self)
        await selector.get_buttons(streams)
        try:
            await wait_for(self.event.wait(), timeout=180)
        except AsyncTimeoutError:
            LOGGER.error(f"Stream selection timed out for MID: {self.mid}")
            await self.clean()
            await self.listener.onUploadError("Stream selection timed out.")
            return None

        if self.is_cancelled:
            return None

        streams_to_remove = getattr(self, 'streams_to_remove', [])
        cmd = ['ffmpeg', '-i', file_list[0], '-map', '0:v']
        kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
        cmd.extend(['-map', stream] for stream in kept_streams)
        cmd.extend(['-c', 'copy', self.outfile, '-y'])

        async with task_dict_lock:
            task_dict[self.mid] = FFMpegStatus(self.listener, self, self.gid, 'direct')
        await sendStatusMessage(self.listener.message)
        process = await create_subprocess_exec(*cmd, stderr=PIPE)
        _, code = await gather(self.progress('direct'), process.wait())
        if code != 0:
            error_msg = (await process.stderr.read()).decode().strip()
            LOGGER.error(f"FFmpeg error for MID: {self.mid}: {error_msg}")
            await self.clean()
            await self.listener.onUploadError("FFmpeg processing failed.")
            return None

        LOGGER.info(f"FFmpeg succeeded for MID: {self.mid}")
        return self.outfile