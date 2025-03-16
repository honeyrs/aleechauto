from aiofiles.os import path as aiopath, rename as aiorename
from asyncio import sleep, gather
from logging import getLogger
from os import path as ospath
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, RetryError
from time import time

from bot import bot, bot_dict, bot_lock, config_dict, LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.files_utils import clean_target, get_path_size
from bot.helper.ext_utils.media_utils import get_document_type, get_media_info, create_thumbnail

LOGGER = getLogger(__name__)

class TgUploader:
    def __init__(self, listener, path: str, size: int):
        self._listener = listener
        self._path = path
        self._size = size
        self._start_time = time()
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._is_cancelled = False
        self._thumb = self._listener.thumb if self._listener.thumb and aiopath.exists(self._listener.thumb) else None
        self._msgs_dict = {}
        self._is_corrupted = False
        self._client = None
        self._send_msg = None
        self._leech_log = config_dict['LEECH_LOG']

    async def _upload_progress(self, current, _):
        if self._is_cancelled:
            self._client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def upload(self, o_files, m_size):
        await self._msg_to_reply()
        corrupted_files = total_files = 0
        TELEGRAM_LIMIT = 2097152000  # 2,000 MiB
        total_parts = len(o_files)

        for i, file_path in enumerate(o_files):
            try:
                f_size = m_size[i]
                if f_size > TELEGRAM_LIMIT:
                    LOGGER.error(f"File {file_path} size {f_size} exceeds Telegram limit {TELEGRAM_LIMIT}")
                    corrupted_files += 1
                    continue
                if f_size == 0:
                    LOGGER.error(f"{file_path} size is zero, Telegram doesn't upload zero-size files")
                    corrupted_files += 1
                    continue
                if self._is_cancelled:
                    return
                if not await aiopath.exists(file_path):
                    LOGGER.error(f"File not found for upload: {file_path}")
                    corrupted_files += 1
                    continue
                part_num = i + 1
                caption = f"{ospath.basename(file_path)} (Part {part_num} of {total_parts})"
                self._last_uploaded = 0
                await self._upload_file(caption, file_path)
                total_files += 1
                if self._is_cancelled:
                    return
                if not self._is_corrupted and (self._listener.isSuperChat or self._leech_log):
                    self._msgs_dict[self._send_msg.link] = ospath.basename(file_path)
                await sleep(3)
            except Exception as err:
                if isinstance(err, RetryError):
                    LOGGER.info(f'Total Attempts: {err.last_attempt.attempt_number}')
                    corrupted_files += 1
                    self._is_corrupted = True
                    err = err.last_attempt.exception()
                LOGGER.error(f'{err}. Path: {file_path}')
                corrupted_files += 1
                if self._is_cancelled:
                    return
                continue
            finally:
                if not self._is_cancelled and await aiopath.exists(file_path) and (
                    not self._listener.seed or self._listener.newDir
                ):
                    await clean_target(file_path)

        if self._is_cancelled:
            return
        if total_files == 0:
            await self._listener.onUploadError("No files to upload!")
            return
        if total_files <= corrupted_files:
            await self._listener.onUploadError('Files corrupted or unable to upload. Check logs!')
            return
        LOGGER.info(f'Leech Completed: {self._listener.name}')
        await self._listener.onUploadComplete(None, self._size, self._msgs_dict, total_files, corrupted_files)

    @retry(wait=wait_exponential(multiplier=2, min=4, max=8), stop=stop_after_attempt(4), retry=retry_if_exception_type(Exception))
    async def _upload_file(self, caption, up_path, force_document=False):
        if not up_path or not await aiopath.exists(up_path):
            raise FileNotFoundError(f"Upload path is invalid or missing: {up_path}")

        thumb = self._thumb
        if self._is_cancelled:
            return

        try:
            async with bot_lock:
                self._client = bot
            is_video, is_audio, is_image = await get_document_type(up_path)
            LOGGER.debug(f"File type for {up_path}: video={is_video}, audio={is_audio}, image={is_image}")

            if is_video and not thumb:
                duration = (await get_media_info(up_path))[0]
                thumb = await create_thumbnail(up_path, duration)
                if not thumb or not await aiopath.exists(thumb):
                    LOGGER.warning(f"Thumbnail creation failed or missing for {up_path}, proceeding without")
                    thumb = None

            if self._listener.as_doc or force_document or (not is_video and not is_audio and not is_image):
                LOGGER.debug(f"Uploading {up_path} as document")
                self._send_msg = await self._client.send_document(
                    chat_id=self._send_msg.chat.id,
                    document=up_path,
                    thumb=thumb,
                    caption=caption,
                    disable_notification=True,
                    progress=self._upload_progress,
                    reply_to_message_id=self._send_msg.id
                )
            elif is_video:
                LOGGER.debug(f"Uploading {up_path} as video")
                duration = (await get_media_info(up_path))[0]
                self._send_msg = await self._client.send_video(
                    chat_id=self._send_msg.chat.id,
                    video=up_path,
                    thumb=thumb,
                    caption=caption,
                    duration=duration,
                    disable_notification=True,
                    progress=self._upload_progress,
                    reply_to_message_id=self._send_msg.id
                )
            # Note: Original code truncated; assuming typical video upload logic
        except Exception as e:
            LOGGER.error(f"Upload file error: {e}")
            raise