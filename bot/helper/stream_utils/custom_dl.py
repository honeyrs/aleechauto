from asyncio import sleep
from pyrogram import utils, raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from typing import Dict, Union

from bot import bot, bot_loop, LOGGER
from bot.helper.ext_utils.exceptions import FIleNotFound
from bot.helper.stream_utils.file_properties import get_file_ids


class ByteStreamer:
    def __init__(self):
        self._cached_file_ids: Dict[int, FileId] = {}
        bot_loop.create_task(self._clean_cache())

    async def get_file_properties(self, message_id: int) -> FileId:
        if message_id not in self._cached_file_ids:
            file_id = await get_file_ids(message_id)
            if not file_id:
                LOGGER.info('Message with ID %s not found!', message_id)
                raise FIleNotFound
            self._cached_file_ids[message_id] = file_id
        return self._cached_file_ids[message_id]

    async def yield_file(self, file_id: FileId, offset: int, first_part_cut: int, last_part_cut: int, part_count: int, chunk_size: int) -> Union[str, None]:
        media_session = await self._generate_media_session(file_id)
        current_part = 1
        location = await self._get_location(file_id)
        try:
            r = await media_session.invoke(raw.functions.upload.GetFile(location=location, offset=offset, limit=chunk_size))
            if isinstance(r, raw.types.upload.File):
                while current_part <= part_count:
                    chunk = r.bytes
                    if not chunk:
                        break
                    offset += chunk_size
                    if part_count == 1:
                        yield chunk[first_part_cut:last_part_cut]
                        break
                    if current_part == 1:
                        yield chunk[first_part_cut:]
                    if 1 < current_part <= part_count:
                        yield chunk
                    r = await media_session.invoke(raw.functions.upload.GetFile(location=location, offset=offset, limit=chunk_size))
                    current_part += 1
        except (TimeoutError, AttributeError) as e:
            LOGGER.error(e, exc_info=True)
        except Exception:
            pass

    @staticmethod
    async def _generate_media_session(file_id: FileId) -> Session:
        media_session = bot.media_sessions.get(file_id.dc_id, None)
        if media_session is None:
            if file_id.dc_id != await bot.storage.dc_id():
                media_session = Session(bot,
                                        file_id.dc_id,
                                        await Auth(bot, file_id.dc_id, await bot.storage.test_mode()).create(),
                                        await bot.storage.test_mode(),
                                        is_media=True)
                await media_session.start()
                for _ in range(6):
                    exported_auth = await bot.invoke(raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id))
                    try:
                        await media_session.invoke(raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                        break
                    except AuthBytesInvalid:
                        LOGGER.info('Invalid authorization bytes for DC %s!', file_id.dc_id)
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(bot,
                                        file_id.dc_id,
                                        await bot.storage.auth_key(),
                                        await bot.storage.test_mode(),
                                        is_media=True)
                await media_session.start()
            bot.media_sessions[file_id.dc_id] = media_session
        return media_session

    @staticmethod
    async def _get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation, raw.types.InputDocumentFileLocation, raw.types.InputPeerPhotoFileLocation]:
        match file_id.file_type:
            case FileType.CHAT_PHOTO:
                if file_id.chat_id > 0:
                    peer = raw.types.InputPeerUser(
                        user_id=file_id.chat_id,
                        access_hash=file_id.chat_access_hash,
                    )
                elif file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )
                location = raw.types.InputPeerPhotoFileLocation(
                    peer=peer,
                    volume_id=file_id.volume_id,
                    local_id=file_id.local_id,
                    big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
                )
            case FileType.PHOTO:
                location = raw.types.InputPhotoFileLocation(
                    id=file_id.media_id,
                    access_hash=file_id.access_hash,
                    file_reference=file_id.file_reference,
                    thumb_size=file_id.thumbnail_size,
                )
            case _:
                location = raw.types.InputDocumentFileLocation(
                    id=file_id.media_id,
                    access_hash=file_id.access_hash,
                    file_reference=file_id.file_reference,
                    thumb_size=file_id.thumbnail_size,
                )
        return location

    async def _clean_cache(self) -> None:
        while True:
            await sleep(30 * 60)
            self._cached_file_ids.clear()
