from aiofiles.os import remove as aioremove, path as aiopath, listdir
from asyncio import gather, sleep, create_subprocess_exec
from asyncio.subprocess import PIPE
from hashlib import sha1
from math import ceil
from natsort import natsorted
from os import path as ospath, walk
from random import randrange
from time import time

from bot import task_dict, task_dict_lock, LOGGER, config_dict, non_queued_up, queue_dict_lock, bot
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.links_utils import is_gdrive_link
from bot.helper.ext_utils.media_utils import get_document_type, take_ss, get_video_resolution
from bot.helper.ext_utils.status_utils import get_readable_time  # Corrected import
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, sendFile

class TgUploader:
    def __init__(self, listener, path: str, size=0):
        self._listener = listener
        self._path = path
        self._size = size or get_path_size(path)
        self._sent_msg = None
        self._file = None
        self._is_cancelled = False
        self._thumb = None
        self._files_dict = {}
        self._total_files = 0
        self._sent_files = 0
        self._start_time = time()
        self._last_uploaded = 0
        LOGGER.info(f"Initialized TgUploader for MID: {self._listener.mid}, path: {self._path}")

    @property
    def speed(self):
        elapsed = max(time() - self._start_time, 1)
        return (self._sent_files * self._size - self._last_uploaded) / elapsed

    @property
    def uploaded_bytes(self):
        return self._sent_files * self._size

    @property
    def total_size(self):
        return self._total_files * self._size

    async def _gen_thumb(self, video_file, duration):
        thumb_path = ospath.join(self._path, f'thumb_{self._listener.mid}.jpg')
        try:
            await take_ss(video_file, thumb_path, duration // 2)
            if await aiopath.exists(thumb_path) and get_path_size(thumb_path) > 0:
                LOGGER.info(f"Generated thumbnail: {thumb_path}")
                return thumb_path
            LOGGER.warning(f"Thumbnail generation failed for {video_file}")
            return None
        except Exception as e:
            LOGGER.error(f"Thumbnail error: {e}")
            if await aiopath.exists(thumb_path):
                await aioremove(thumb_path)
            return None

    async def _send_file(self, file_, multi_files=False):
        self._file = file_
        try:
            is_video, *_ = await get_document_type(file_)
            split_size = 2_097_152_000  # Telegram bot limit (2GB)
            file_size = get_path_size(file_)
            LOGGER.info(f"Sending file: {file_}, size: {file_size / (1024*1024):.2f} MB")

            if file_size > split_size:
                base_name = ospath.splitext(file_)[0]
                parts = ceil(file_size / split_size)
                duration = (await get_video_resolution(file_))[2] if is_video else 0
                self._thumb = await self._gen_thumb(file_, duration) if is_video and not self._thumb else self._thumb

                for i in range(parts):
                    if self._is_cancelled:
                        return
                    part_file = f"{base_name}.part{i+1}{ospath.splitext(file_)[1]}"
                    start_bytes = i * split_size
                    cmd = [
                        'ffmpeg', '-i', file_, '-ss', str(start_bytes / 1_000_000),
                        '-fs', str(split_size), '-c', 'copy', '-map', '0', '-y', part_file
                    ]
                    process = await create_subprocess_exec(*cmd, stderr=PIPE)
                    self._listener.suproc = process
                    _, stderr = await process.communicate()
                    if process.returncode != 0 or not await aiopath.exists(part_file):
                        LOGGER.error(f"Split error for {file_}, part {i+1}: {stderr.decode()}")
                        raise Exception(f"Failed to split {file_}")
                    await self._send_part(part_file, is_video, duration, i + 1, parts)
                    await aioremove(part_file)
            else:
                duration = (await get_video_resolution(file_))[2] if is_video else 0
                self._thumb = await self._gen_thumb(file_, duration) if is_video and not self._thumb else self._thumb
                await self._send_part(file_, is_video, duration, 1 if multi_files else None, None)

        except Exception as e:
            LOGGER.error(f"Error sending file {file_}: {e}")
            self._is_cancelled = True
            if multi_files:
                await clean_target(file_)
        finally:
            if not multi_files and await aiopath.exists(file_):
                await clean_target(file_)
            if self._thumb and await aiopath.exists(self._thumb):
                await aioremove(self._thumb)

    async def _send_part(self, part_file, is_video, duration, part_num=None, total_parts=None):
        caption = f"{ospath.basename(self._path)}.part{part_num}" if part_num else ospath.basename(self._path)
        try:
            async with task_dict_lock:
                if self._listener.mid not in task_dict:
                    LOGGER.warning(f"MID {self._listener.mid} not in task_dict, aborting upload")
                    self._is_cancelled = True
                    return
                task_dict[self._listener.mid] = TelegramStatus(self._listener, self, self._size, self._listener.gid, 'up')

            if not self._sent_msg:
                self._sent_msg = await sendMessage('Uploading...', self._listener.message)
            if is_video:
                sent_file = await sendFile(
                    self._sent_msg.chat.id, part_file, self._listener.message,
                    caption=caption, thumb=self._thumb, duration=duration,
                    progress=self.progress, progress_args=(self._sent_msg, self._start_time, None, True)
                )
            else:
                sent_file = await sendFile(
                    self._sent_msg.chat.id, part_file, self._listener.message,
                    caption=caption, progress=self.progress, progress_args=(self._sent_msg, self._start_time, None, True)
                )
            if sent_file:
                self._files_dict[caption] = f"https://t.me/c/{str(sent_file.chat.id)[4:]}/{sent_file.id}"
                self._sent_files += 1
                self._last_uploaded = self.uploaded_bytes
                LOGGER.info(f"Uploaded {caption} for MID: {self._listener.mid}")
        except Exception as e:
            LOGGER.error(f"Error sending part {part_file}: {e}")
            self._is_cancelled = True

    async def progress(self, current, total, message, start_time, info=None, active=True):
        if self._is_cancelled:
            bot.stop_transmission()
        elapsed = time() - start_time
        speed = current / elapsed if elapsed > 0 else 0
        percentage = current * 100 / total
        eta = (total - current) / speed if speed > 0 else 0
        progress_text = f"{info or 'Uploading...'}\n" + \
                        f"Progress: {percentage:.2f}%\n" + \
                        f"Speed: {get_readable_time(speed)}/s\n" + \
                        f"ETA: {get_readable_time(eta)}\n"
        await editMessage(progress_text, message)

    async def upload(self, o_files=None, m_size=None):
        if not self._listener.isLeech:
            LOGGER.error(f"Task is not leech for MID: {self._listener.mid}")
            await self._listener.onUploadError("This task is not set for Telegram upload.")
            return None

        try:
            if o_files and m_size:
                self._total_files = len(o_files)
                files = [(ospath.join(self._path, f), s) for f, s in zip(o_files, m_size)]
            elif await aiopath.isfile(self._path):
                self._total_files = 1
                files = [(self._path, self._size)]
            else:
                files = []
                for dirpath, _, fnames in await sync_to_async(walk, self._path):
                    for file in natsorted(fnames):
                        file_path = ospath.join(dirpath, file)
                        if not file.endswith(('.aria2', '.!qB')):
                            files.append((file_path, get_path_size(file_path)))
                self._total_files = len(files)

            if not files:
                await sendMessage("No files to upload.", self._listener.message)
                return None

            for file_path, file_size in files:
                if self._is_cancelled:
                    break
                self._size = file_size
                self._sent_files = 0
                self._last_uploaded = 0
                self._start_time = time()
                await self._send_file(file_path, multi_files=len(files) > 1)

            if self._is_cancelled:
                await self._listener.onUploadError("Upload cancelled or failed.")
                return None

            if self._sent_msg:
                buttons = ButtonMaker()
                if not is_gdrive_link(self._listener.link):
                    buttons.button_link("Original Link", self._listener.link)
                await editMessage(f"Uploaded {self._sent_files}/{self._total_files} files successfully.", self._sent_msg, buttons.build_menu(1))

            LOGGER.info(f"Upload completed for MID: {self._listener.mid}, files uploaded: {self._sent_files}")
            return self._files_dict

        except Exception as e:
            LOGGER.error(f"Upload error for MID: {self._listener.mid}: {e}")
            await self._listener.onUploadError(f"Upload failed: {str(e)}")
            return None
        finally:
            async with queue_dict_lock:
                non_queued_up.discard(self._listener.mid)
            await start_from_queued()

    def cancel_download(self):
        self._is_cancelled = True
        LOGGER.info(f"Upload cancelled for MID: {self._listener.mid}")
        if self._sent_msg:
            bot.stop_transmission()