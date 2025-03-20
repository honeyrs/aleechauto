from asyncio import gather
from os import path as ospath
from time import time

from bot import LOGGER, bot, task_dict_lock, task_dict
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.files_utils import get_path_size
from bot.helper.ext_utils.status_utils import get_readable_file_size
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.message_utils import update_status_message

class TgUploader:
    def __init__(self, listener, path, size=0):
        self._listener = listener
        self._path = path
        self._size = size
        self._file = None
        self._last_uploaded = 0
        self._start_time = time()
        self._is_cancelled = False
        self._files_dict = {}
        LOGGER.info(f"Initialized TgUploader for MID: {self._listener.mid}, path: {self._path}")

    @property
    def speed(self):
        try:
            return (self._last_uploaded / (time() - self._start_time))
        except ZeroDivisionError:
            return 0

    @property
    def uploaded_bytes(self):
        return self._last_uploaded

    @property
    def total_size(self):
        return self._size

    async def _send_file(self, file_path):
        try:
            LOGGER.info(f"Sending file: {file_path}")
            file_size = await get_path_size(file_path)
            msg = await bot.send_document(
                chat_id=self._listener.message.chat.id,
                document=file_path,
                caption=f"{ospath.basename(file_path)} ({get_readable_file_size(file_size)})",
                reply_to_message_id=self._listener.message.id,
                progress=self._progress_callback
            )
            if msg.document:
                self._files_dict[msg.document.file_name] = f"https://t.me/c/{str(msg.chat.id)[4:]}/{msg.id}"
                self._last_uploaded += file_size
            LOGGER.info(f"Successfully sent file: {file_path}")
            return True
        except Exception as e:
            LOGGER.error(f"Error sending file {file_path}: {e}")
            return False

    async def _progress_callback(self, current, total):
        if self._is_cancelled:
            raise Exception("Upload cancelled")
        self._last_uploaded = current
        async with task_dict_lock:
            if self._listener.mid in task_dict:
                task_dict[self._listener.mid] = TelegramStatus(self._listener, self, self._size, self._listener.mid[:12], 'up')

    async def upload(self, o_files, m_size):
        if await sync_to_async(ospath.isdir, self._path):
            if o_files and m_size:
                for i, file_ in enumerate(o_files):
                    file_path = ospath.join(self._path, file_)
                    if not await self._send_file(file_path):
                        self._is_cancelled = True
                        return {}
            else:
                for dirpath, _, files in await sync_to_async(ospath.walk, self._path):
                    for file_ in sorted(files):
                        file_path = ospath.join(dirpath, file_)
                        if not file_.endswith(('.aria2', '.!qB')):
                            if not await self._send_file(file_path):
                                self._is_cancelled = True
                                return {}
        else:
            if not await self._send_file(self._path):
                self._is_cancelled = True
                return {}
        if not self._is_cancelled:
            await gather(update_status_message(self._listener.message.chat.id))
            LOGGER.info(f"Upload completed for MID: {self._listener.mid}")
            return self._files_dict
        return {}

    async def cancel_download(self):
        self._is_cancelled = True
        LOGGER.info(f"Cancelling upload for MID: {self._listener.mid}")