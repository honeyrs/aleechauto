from asyncio import Lock, gather
from logging import getLogger, ERROR
from os import path as ospath
from pyrogram import Client
from time import time

from bot import bot, task_dict, task_dict_lock, non_queued_dl, queue_dict_lock, LOGGER
from bot.helper.ext_utils.links_utils import is_media
from bot.helper.ext_utils.status_utils import get_readable_file_size
from bot.helper.ext_utils.task_manager import check_running_tasks, stop_duplicate_check, check_limits_size
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage

global_lock = Lock()
GLOBAL_GID = set()
getLogger('pyrogram').setLevel(ERROR)

class TelegramDownloadHelper:
    def __init__(self, listener):
        self._processed_bytes = 0
        self._start_time = time()
        self._listener = listener
        self._id = ''
        self._is_cancelled = False
        self._client = bot

    @property
    def speed(self):
        return self._processed_bytes / max(time() - self._start_time, 1)

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def _onDownloadStart(self, size, file_id, from_queue):
        async with global_lock:
            GLOBAL_GID.add(file_id)
        self._id = file_id
        async with task_dict_lock:
            task_dict[self._listener.mid] = TelegramStatus(self._listener, self, size, file_id[:12], 'dl')
        async with queue_dict_lock:
            non_queued_dl.add(self._listener.mid)
        if not from_queue:
            await self._listener.onDownloadStart()
            if self._listener.multi <= 1:
                await sendStatusMessage(self._listener.message)
            LOGGER.info(f"Download from Telegram: {self._listener.name}")
        else:
            LOGGER.info(f"Start Queued Download from Telegram: {self._listener.name}")

    async def _onDownloadProgress(self, current, total):
        if self._is_cancelled:
            self._client.stop_transmission()
            return
        self._processed_bytes = current

    async def _onDownloadError(self, error, listfile=None):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        await self._listener.onDownloadError(error, listfile)
        async with queue_dict_lock:
            non_queued_dl.discard(self._listener.mid)

    async def _onDownloadComplete(self):
        await self._listener.onDownloadComplete()
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        async with queue_dict_lock:
            non_queued_dl.discard(self._listener.mid)

    async def _download(self, message, path):
        try:
            download = await self._client.download_media(
                message=message,
                file_name=path,
                progress=self._onDownloadProgress
            )
            if self._is_cancelled:
                return
            if download:
                self._listener.name = ospath.basename(download)
                await self._onDownloadComplete()
            elif not self._is_cancelled:
                await self._onDownloadError('Internal error occurred')
        except Exception as e:
            LOGGER.error(f"Download error: {e}")
            await self._onDownloadError(str(e))

    async def add_download(self, message, path):
        if self._listener.session and self._listener.session != bot:
            self._client = self._listener.session
        if media := is_media(message):
            async with global_lock:
                download = media.file_unique_id not in GLOBAL_GID
            if download:
                if not self._listener.name:
                    self._listener.name = getattr(media, 'file_name', media.file_unique_id) or media.file_unique_id
                path = ospath.join(path, self._listener.name)
                size = media.file_size
                gid = media.file_unique_id

                file, name = await stop_duplicate_check(self._listener)
                if file:
                    self._listener.name = name
                    LOGGER.info('File/folder already in Drive!')
                    await self._onDownloadError('File already in Drive!', file)
                    return

                if msg := await check_limits_size(self._listener, size):
                    LOGGER.info('File/folder size over the limit size!')
                    await self._onDownloadError(f'{msg}. File/folder size is {get_readable_file_size(size)}.')
                    return

                add_to_queue, event = await check_running_tasks(self._listener.mid)
                if add_to_queue:
                    LOGGER.info(f"Added to Queue/Download: {self._listener.name}")
                    async with task_dict_lock:
                        task_dict[self._listener.mid] = QueueStatus(self._listener, size, gid, 'dl')
                    await gather(self._listener.onDownloadStart(), sendStatusMessage(self._listener.message), event.wait())
                    async with task_dict_lock:
                        if self._listener.mid not in task_dict:
                            return
                    from_queue = True
                else:
                    from_queue = False
                await self._onDownloadStart(size, gid, from_queue)
                LOGGER.info(f"Downloading Telegram file with id: {gid}")
                await self._download(message, path)
            else:
                await self._onDownloadError('File already being downloaded!')
        else:
            await self._onDownloadError('No document from given link!' if self._listener.session else 'No document in the replied message')

    async def cancel_task(self):
        self._is_cancelled = True
        LOGGER.info(f"Cancelling download on user request: name: {self._listener.name} id: {self._id}")
        await self._onDownloadError('Download cancelled by user!')