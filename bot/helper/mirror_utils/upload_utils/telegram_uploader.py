from aiofiles.os import path as aiopath, makedirs
from aioshutil import copy
from asyncio import sleep
from logging import getLogger
from natsort import natsorted
from os import path as ospath, walk
from pyrogram.errors import FloodWait, RPCError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from time import time

from bot import bot, config_dict, LOGGER
from bot.helper.ext_utils.files_utils import clean_unwanted, clean_target, get_path_size, get_base_name
from bot.helper.ext_utils.media_utils import get_document_type, get_media_info, take_ss

LOGGER = getLogger(__name__)

class TgUploader:
    def __init__(self, listener, path: str, size: int):
        self._listener = listener
        self._path = path
        self._size = size
        self._processed_bytes = 0
        self._last_uploaded = 0
        self._start_time = time()
        self._is_cancelled = False
        self._thumb = ospath.join(self._path, 'thumb.jpg') if ospath.exists(ospath.join(self._path, 'thumb.jpg')) else None
        self._msgs_dict = {}
        LOGGER.info(f"Initialized TgUploader for {self._listener.name} (MID: {self._listener.mid}), path: {self._path}")

    async def upload(self, o_files: list, m_size: list):
        """Upload files to Telegram."""
        LOGGER.info(f"Starting upload for {self._listener.name} (MID: {self._listener.mid}) with {len(o_files)} files")
        total_files = 0

        for file_name, file_size in zip(o_files, m_size):
            if self._is_cancelled:
                LOGGER.info(f"Upload cancelled for {self._listener.name} (MID: {self._listener.mid})")
                return None

            self._up_path = ospath.join(self._path, file_name)
            if not await aiopath.exists(self._up_path):
                LOGGER.error(f"File not found: {self._up_path}")
                continue
            if file_size > 2_097_152_000:  # Telegram limit: 2000 MiB
                LOGGER.warning(f"Skipping {file_name}: Size {file_size / (1024*1024):.2f} MiB exceeds Telegram limit")
                continue

            caption = f'<code>{file_name}</code>'
            if total_files == 0 and not self._thumb and await aiopath.isfile(self._up_path):
                is_video, _, _ = await get_document_type(self._up_path)
                if is_video:
                    self._thumb = await take_ss(self._up_path, None)
                    if self._is_cancelled:
                        return None

            LOGGER.info(f"Uploading {file_name} ({file_size / (1024*1024):.2f} MiB)")
            await self._upload_file(caption, file_name)
            total_files += 1
            await sleep(3)  # Avoid flooding

        if total_files == 0:
            LOGGER.error(f"No valid files uploaded for {self._listener.name} (MID: {self._listener.mid})")
            await self._listener.onUploadError("No files could be uploaded.")
            return None

        LOGGER.info(f"Upload completed: {self._listener.name} (MID: {self._listener.mid}), {total_files} files")
        return self._msgs_dict

    @retry(wait=wait_exponential(multiplier=2, min=4, max=8), stop=stop_after_attempt(4), retry=retry_if_exception_type(Exception))
    async def _upload_file(self, caption, file, force_document=False):
        """Upload a single file with retry logic."""
        if self._is_cancelled:
            LOGGER.info(f"Stopping transmission for {file} due to cancellation")
            bot.stop_transmission()
            return

        try:
            is_video, is_audio, _ = await get_document_type(self._up_path)
            if not force_document and (is_video or is_audio):
                metadata = await get_media_info(self._up_path)
                width = metadata.get('width', 0)
                height = metadata.get('height', 0) if is_video else 0
                duration = metadata.get('duration', 0)

                if is_video:
                    sent_msg = await bot.send_video(
                        chat_id=self._listener.message.chat.id,
                        video=self._up_path,
                        caption=caption,
                        duration=duration,
                        width=width,
                        height=height,
                        thumb=self._thumb,
                        supports_streaming=True,
                        disable_notification=True,
                        progress=self._upload_progress,
                        reply_to_message_id=self._listener.message.id
                    )
                else:  # is_audio
                    sent_msg = await bot.send_audio(
                        chat_id=self._listener.message.chat.id,
                        audio=self._up_path,
                        caption=caption,
                        duration=duration,
                        disable_notification=True,
                        progress=self._upload_progress,
                        reply_to_message_id=self._listener.message.id
                    )
            else:
                sent_msg = await bot.send_document(
                    chat_id=self._listener.message.chat.id,
                    document=self._up_path,
                    caption=caption,
                    thumb=self._thumb,
                    disable_notification=True,
                    progress=self._upload_progress,
                    reply_to_message_id=self._listener.message.id
                )

            self._msgs_dict[sent_msg.link] = file
            LOGGER.info(f"Uploaded {file} to {sent_msg.link}")

            if not self._is_cancelled and await aiopath.exists(self._up_path):
                await clean_target(self._up_path)
            if self._thumb and await aiopath.exists(self._thumb) and self._thumb != ospath.join(self._path, 'thumb.jpg'):
                await clean_target(self._thumb)

        except FloodWait as f:
            LOGGER.warning(f"FloodWait: Sleeping for {f.value * 1.2}s")
            await sleep(f.value * 1.2)
            raise  # Retry after sleep
        except RPCError as err:
            LOGGER.error(f"RPCError: {err}")
            if 'Telegram says: [400' in str(err) or 'MEDIA_EMPTY' in str(err):
                LOGGER.info(f"Forcing document upload for {file}")
                await self._upload_file(caption, file, True)
            else:
                raise
        except Exception as e:
            LOGGER.error(f"Upload error for {file}: {e}", exc_info=True)
            raise

    async def _upload_progress(self, current, total):
        """Track upload progress."""
        if self._is_cancelled:
            LOGGER.info("Cancelling upload via progress callback")
            bot.stop_transmission()
            return

        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size
        # LOGGER.debug(f"Progress: {current}/{total} bytes uploaded")

    @property
    def speed(self):
        """Calculate upload speed in bytes per second."""
        try:
            return self._processed_bytes / (time() - self._start_time)
        except ZeroDivisionError:
            return 0

    @property
    def processed_bytes(self):
        """Return total bytes uploaded."""
        return self._processed_bytes

    async def cancel_task(self):
        """Cancel the upload task."""
        self._is_cancelled = True
        LOGGER.info(f"Upload cancelled for {self._listener.name} (MID: {self._listener.mid})")
        await self._listener.onUploadError('Upload stopped by user!')