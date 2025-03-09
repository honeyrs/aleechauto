from html import escape
from pyrogram.file_id import FileId
from pyrogram.types import Message
from typing import Optional
from urllib.parse import quote_plus

from bot import bot, config_dict
from bot.helper.ext_utils.exceptions import FIleNotFound
from bot.helper.ext_utils.links_utils import is_media


async def get_file_ids(message_id: int) -> Optional[FileId]:
    message = await bot.get_messages(config_dict['LEECH_LOG'], message_id)
    if message.empty:
        raise FIleNotFound
    file_id = file_unique_id = None
    if media := is_media(message):
        file_id, file_unique_id = FileId.decode(media.file_id), media.file_unique_id
    setattr(file_id, 'file_size', getattr(media, 'file_size', 0))
    setattr(file_id, 'mime_type', getattr(media, 'mime_type', ''))
    setattr(file_id, 'file_name', getattr(media, 'file_name', ''))
    setattr(file_id, 'unique_id', file_unique_id)
    return file_id


async def gen_link(message: Message):
    stream_link = dl_link = None
    if config_dict['ENABLE_STREAM_LINK'] and config_dict['STREAM_PORT'] and config_dict['LEECH_LOG'] and (base_url := config_dict['STREAM_BASE_URL']):
        media = is_media(message)
        name, hash = getattr(media, 'file_name', media.file_unique_id) or media.file_unique_id, getattr(media, 'file_unique_id', '')[:6]
        if getattr(media, 'mime_type', 'None/unknown').startswith('video'):
            stream_link = f'{base_url}/watch/{hash}{message.id}'
        dl_link = f'{base_url}/{message.id}/{quote_plus(escape(name))}?hash={hash}'
    return stream_link, dl_link
