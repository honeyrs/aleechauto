from aiofiles.os import path as aiopath, rename as aiorename, makedirs
from aioshutil import copy
from asyncio import sleep, gather
from logging import getLogger
from natsort import natsorted
from os import path as ospath, walk
from PIL import Image
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaVideo, InputMediaDocument, InputMediaPhoto, Message
from re import match as re_match
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, RetryError
from time import time

from bot import bot, bot_dict, bot_lock, config_dict, DEFAULT_SPLIT_SIZE, LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async, default_button
from bot.helper.ext_utils.files_utils import clean_unwanted, clean_target, get_path_size, is_archive, get_base_name
from bot.helper.ext_utils.media_utils import create_thumbnail, take_ss, get_document_type, get_media_info, get_audio_thumb, post_media_info
from bot.helper.ext_utils.shortenurl import short_url
from bot.helper.listeners import tasks_listener as task
from bot.helper.stream_utils.file_properties import gen_link
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import deleteMessage, handle_message

LOGGER = getLogger(__name__)

class TgUploader:
    def __init__(self, listener: task.TaskListener, path: str, size: int):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._start_time = time()
        self._is_cancelled = False
        self._thumb = self._listener.thumb or ospath.join('thumbnails', f'{self._listener.user_id}.jpg')
        self._msgs_dict = {}
        self._is_corrupted = False
        self._size = size
        self._media_dict = {'videos': {}, 'documents': {}}
        self._last_msg_in_group = False
        self._client = None
        self._send_msg = None
        self._up_path = ''
        self._leech_log = config_dict['LEECH_LOG']

    async def _upload_progress(self, current, _):
        if self._is_cancelled:
            self._client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def upload(self, o_files, m_size):
        await self._user_settings()
        await self._msg_to_reply()
        corrupted_files = total_files = 0

        if o_files:
            files_to_upload = [(self._path, f) for f in o_files]
        else:
            files_to_upload = []
            for dirpath, _, files in sorted(await sync_to_async(walk, self._path)):
                if dirpath.endswith('/yt-dlp-thumb'):
                    continue
                for file_ in natsorted(files):
                    files_to_upload.append((dirpath, file_))

        for dirpath, file_ in files_to_upload:
            self._up_path = ospath.join(dirpath, file_)
            if file_.lower().endswith(tuple(self._listener.extensionFilter)) or file_.startswith('Thumb'):
                if not file_.startswith('Thumb'):
                    await clean_target(self._up_path)
                continue
            try:
                f_size = await get_path_size(self._up_path)
                if self._listener.seed and file_ in o_files and f_size in m_size:
                    continue
                if f_size == 0:
                    corrupted_files += 1
                    LOGGER.error(f'{self._up_path} size is zero, telegram don\'t upload zero size files')
                    continue
                if self._is_cancelled:
                    return
                caption = await self._prepare_file(file_, dirpath)
                if self._last_msg_in_group:
                    group_lists = [x for v in self._media_dict.values() for x in v.keys()]
                    match = re_match(r'.+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)', self._up_path)
                    if not match or match and match.group(0) not in group_lists:
                        for key, value in list(self._media_dict.items()):
                            for subkey, msgs in list(value.items()):
                                if len(msgs) > 1:
                                    await self._send_media_group(msgs, subkey, key)
                self._last_msg_in_group = False
                self._last_uploaded = 0
                await self._upload_file(caption, file_)
                total_files += 1
                if self._is_cancelled:
                    return
                if not self._is_corrupted and (self._listener.isSuperChat or self._leech_log):
                    self._msgs_dict[self._send_msg.link] = file_  # Fixed line 101
                await sleep(3)
            except Exception as err:
                if isinstance(err, RetryError):
                    LOGGER.info(f'Total Attempts: {err.last_attempt.attempt_number}', exc_info=True)
                    corrupted_files += 1
                    self._is_corrupted = True
                    err = err.last_attempt.exception()
                LOGGER.error(f'{err}. Path: {self._up_path}')
                corrupted_files += 1
                if self._is_cancelled:
                    return
                continue
            finally:
                if not self._is_cancelled and await aiopath.exists(self._up_path) and (not self._listener.seed or self._listener.newDir or
                    dirpath.endswith('/splited_files_mltb') or '/copied_mltb/' in self._up_path):
                    await clean_target(self._up_path)

        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    await self._send_media_group(msgs, subkey, key)
        if self._is_cancelled:
            return
        if self._listener.seed and not self._listener.newDir:
            await clean_unwanted(self._path)
        if total_files == 0:
            await self._listener.onUploadError(f"No files to upload or in blocked list ({', '.join(self._listener.extensionFilter[2:])})!")
            return
        if total_files <= corrupted_files:
            await self._listener.onUploadError('Files Corrupted or unable to upload. Check logs!')
            return
        LOGGER.info(f'Leech Completed: {self._listener.name}')
        await self._listener.onUploadComplete(None, self._size, self._msgs_dict, total_files, corrupted_files)

    @retry(wait=wait_exponential(multiplier=2, min=4, max=8), stop=stop_after_attempt(4), retry=retry_if_exception_type(Exception))
    async def _upload_file(self, caption, file, force_document=False):
        if self._thumb and not await aiopath.exists(self._thumb):
            self._thumb = None
        thumb = self._thumb
        if self._is_cancelled:
            return
        try:
            async with bot_lock:
                self._client = (bot_dict['USERBOT'] if bot_dict['IS_PREMIUM'] and await get_path_size(self._up_path) > DEFAULT_SPLIT_SIZE
                                or bot_dict['USERBOT'] and config_dict['USERBOT_LEECH'] else bot)
            is_video, is_audio, is_image = await get_document_type(self._up_path)
            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = ospath.join(self._path, 'yt-dlp-thumb', f'{file_name}.jpg')
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif is_audio and not is_video:
                    thumb = await get_audio_thumb(self._up_path)
            if is_video and not self._listener.as_doc:
                duration = (await get_media_info(self._up_path))[0]
                if not thumb:
                    thumb = await create_thumbnail(self._up_path, duration)
                if thumb:
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width, height = 480, 320
                if not self._up_path.upper().endswith(('.MKV', '.MP4')):
                    dirpath, file_ = ospath.split(self._up_path)
                    if self._listener.seed and not self._listener.newDir and not dirpath.endswith('/splited_files_mltb'):
                        dirpath = ospath.join(dirpath, 'copied_mltb')
                        await makedirs(dirpath, exist_ok=True)
                        new_path = ospath.join(dirpath, f'{ospath.splitext(file_)[0]}.mp4')
                        self._up_path = await copy(self._up_path, new_path)
                    else:
                        new_path = f'{ospath.splitext(self._up_path)[0]}.mp4'
                        await aiorename(self._up_path, new_path)
                        self._up_path = new_path
                if self._is_cancelled:
                    return
                self._send_msg = await self._client.send_video(chat_id=self._send_msg.chat.id,
                                                               video=self._up_path,
                                                               caption=caption,
                                                               duration=duration,
                                                               width=width,
                                                               height=height,
                                                               thumb=thumb,
                                                               supports_streaming=True,
                                                               disable_notification=True,
                                                               progress=self._upload_progress,
                                                               reply_to_message_id=self._send_msg.id)
            else:
                if self._is_cancelled:
                    return
                self._send_msg = await self._client.send_document(chat_id=self._send_msg.chat.id,
                                                                  document=self._up_path,
                                                                  thumb=thumb,
                                                                  caption=caption,
                                                                  disable_notification=True,
                                                                  progress=self._upload_progress,
                                                                  reply_to_message_id=self._send_msg.id)
            if self._is_cancelled:
                return
            if not self._thumb and thumb:
                await clean_target(thumb)
        except FloodWait as f:
            LOGGER.warning(f, exc_info=True)
            await sleep(f.value * 1.2)
        except Exception as err:
            if not self._thumb and thumb:
                await clean_target(thumb)
            err_type = 'RPCError: ' if isinstance(err, RPCError) else ''
            LOGGER.error(f'{err_type}{err}. Path: {self._up_path}')
            raise err

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get('media_group', False) or ('media_group' not in self._listener.user_dict and config_dict['MEDIA_GROUP'])
        self._cap_mode = self._listener.user_dict.get('caption_style', 'mono')
        self._log_title = self._listener.user_dict.get('log_title', False)
        self._send_pm = self._listener.user_dict.get('enable_pm', False) and (self._listener.isSuperChat or self._leech_log)

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._is_cancelled = True
        LOGGER.info(f'Cancelling Upload: {self._listener.name}')
        await self._listener.onUploadError('Upload stopped by user!')
        if hasattr(self, '_up_path') and await aiopath.exists(self._up_path):
            await clean_target(self._up_path)

    async def _prepare_file(self, file_, dirpath):
        caption = self._caption_mode(file_)
        if len(file_) > 60:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r'.+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)', file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name, ext = fsplit[0], fsplit[1]
            else:
                name, ext = file_, ''
            name = name[:60 - len(ext)]
            if self._listener.seed and not self._listener.newDir and not dirpath.endswith('/splited_files_mltb'):
                dirpath = ospath.join(dirpath, 'copied_mltb')
                await makedirs(dirpath, exist_ok=True)
                new_path = ospath.join(dirpath, f'{name}{ext}')
                self._up_path = await copy(self._up_path, new_path)
            else:
                new_path = ospath.join(dirpath, f'{name}{ext}')
                await aiorename(self._up_path, new_path)
                self._up_path = new_path
        return caption

    def _caption_mode(self, file):
        match self._cap_mode:
            case 'italic':
                caption = f'<i>{file}</i>'
            case 'bold':
                caption = f'<b>{file}</b>'
            case 'normal':
                caption = file
            case 'mono':
                caption = f'<code>{file}</code>'
        return caption

    @handle_message
    async def _msg_to_reply(self):
        if self._leech_log and self._leech_log != self._listener.message.chat.id:
            caption = f'<b>▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n{self._listener.name}\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬</b>'
            if self._thumb and await aiopath.exists(self._thumb):
                self._send_msg: Message = await bot.send_photo(self._leech_log, photo=self._thumb, caption=caption)
            else:
                self._send_msg: Message = await bot.send_message(self._leech_log, caption, disable_web_page_preview=True)
        else:
            self._send_msg: Message = await bot.get_messages(self._listener.message.chat.id, self._listener.mid)
            if not self._send_msg or not self._send_msg.chat:
                self._send_msg = self._listener.message

    @handle_message
    async def _send_media_group(self, msgs: list[Message], subkey: str, key: str):
        msgs_list = await msgs[0].reply_to_message.reply_media_group(media=self._get_input_media(subkey, key),
                                                                     quote=True, disable_notification=True)
        for msg in msgs:
            self._msgs_dict.pop(msg.link, None)
            await deleteMessage(msg)
        del self._media_dict[key][subkey]
        if self._listener.isSuperChat or self._leech_log:
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption.split('\n')[0] + ' ~ (Grouped)'
        self._send_msg = msgs_list[-1]

    def _get_input_media(self, subkey: str, key: str):
        imlist = []
        for msg in self._media_dict[key][subkey]:
            caption = self._caption_mode(msg.caption.split('\n')[0])
            if key == 'videos':
                input_media = InputMediaVideo(media=msg.video.file_id, caption=caption)
            else:
                input_media = InputMediaDocument(media=msg.document.file_id, caption=caption)
            imlist.append(input_media)
        return imlist