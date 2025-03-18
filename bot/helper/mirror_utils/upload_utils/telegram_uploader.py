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
import subprocess
import math

from bot import bot, bot_dict, bot_lock, config_dict, DEFAULT_SPLIT_SIZE, LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async, default_button
from bot.helper.ext_utils.files_utils import clean_unwanted, clean_target, get_path_size, is_archive, get_base_name
from bot.helper.ext_utils.media_utils import create_thumbnail, take_ss, get_document_type, get_media_info, get_audio_thumb, post_media_info, GenSS
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
        LOGGER.debug(f"Initialized TgUploader: {self._listener.name}, Path: {self._path}, Size: {self._size}")

    async def _upload_progress(self, current, _):
        if self._is_cancelled:
            LOGGER.info("Upload cancelled, stopping transmission")
            self._client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size
        LOGGER.debug(f"Progress: {self._processed_bytes / (1024*1024):.2f} MB for {self._up_path}")

    async def _adjust_part_size(self, input_file, part_file, start_time, current_size_bytes, target_min_bytes, target_max_bytes, total_duration):
        """Adjust part size if outside 1.95-1.99 GB range."""
        if target_min_bytes <= current_size_bytes <= target_max_bytes:
            LOGGER.debug(f"Part {part_file} size {current_size_bytes / (1024*1024*1024):.2f} GB is within range")
            return True

        remaining_duration = total_duration - start_time
        shrink_factor = 0.95
        expansion_factor = 1.05
        max_attempts = 3

        for attempt in range(max_attempts):
            if current_size_bytes > target_max_bytes:
                new_duration = remaining_duration * shrink_factor * (1 - attempt * 0.05)
                LOGGER.debug(f"Attempt {attempt + 1}: Shrinking {part_file} from {current_size_bytes / (1024*1024*1024):.2f} GB to {new_duration:.2f}s")
            elif current_size_bytes < target_min_bytes:
                new_duration = min(remaining_duration * expansion_factor * (1 + attempt * 0.05), remaining_duration)
                LOGGER.debug(f"Attempt {attempt + 1}: Expanding {part_file} from {current_size_bytes / (1024*1024*1024):.2f} GB to {new_duration:.2f}s")
            else:
                return True

            cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(new_duration), '-c', 'copy', part_file]
            try:
                await sync_to_async(subprocess.run, cmd, capture_output=True, text=True, check=True, timeout=300)
                new_size = await get_path_size(part_file)
                LOGGER.debug(f"Adjusted {part_file} to {new_size / (1024*1024*1024):.2f} GB")
                if target_min_bytes <= new_size <= target_max_bytes:
                    return True
                current_size_bytes = new_size
            except Exception as e:
                LOGGER.error(f"Adjustment attempt {attempt + 1} failed for {part_file}: {e}")
                return False

        LOGGER.warning(f"Failed to adjust {part_file} after {max_attempts} attempts")
        return False

    async def _split_file(self, input_file):
        """Split file into parts between 1.95GB and 1.99GB with adjustment."""
        target_min_bytes = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
        target_max_bytes = 1.99 * 1024 * 1024 * 1024  # 1.99 GB
        split_dir = ospath.join(self._path, "splited_files_mltb")
        await makedirs(split_dir, exist_ok=True)
        split_files = []

        file_size = await get_path_size(input_file)
        LOGGER.debug(f"File {input_file}: {file_size / (1024*1024*1024):.2f} GB")
        if file_size <= target_max_bytes:
            LOGGER.debug("No split required")
            return [input_file]

        try:
            await sync_to_async(subprocess.run, ['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            await sync_to_async(subprocess.run, ['ffprobe', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            LOGGER.error("FFmpeg or ffprobe not found")
            raise Exception("FFmpeg/ffprobe not installed")

        try:
            cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
            result = await sync_to_async(subprocess.run, cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            duration = float(result.stdout.strip())
            LOGGER.debug(f"Duration: {duration:.2f}s")
        except (subprocess.CalledProcessError, ValueError) as e:
            LOGGER.error(f"Duration fetch failed: {e}")
            raise Exception(f"Cannot get duration: {e}")

        num_parts = max(1, math.ceil(file_size / target_max_bytes))
        start_time = 0
        base_name = ospath.splitext(ospath.basename(input_file))[0]
        parts_info = []  # (part_file, start_time, size)
        LOGGER.info(f"Splitting {input_file} into {num_parts} parts")

        for part_num in range(1, num_parts + 1):
            part_file = ospath.join(split_dir, f"{base_name}.part{part_num}.mkv")
            is_last_part = (part_num == num_parts)

            if is_last_part:
                cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-c', 'copy', part_file]
                LOGGER.debug(f"Last part {part_num}: {part_file}")
            else:
                low, high = 0, duration - start_time
                bytes_per_sec = file_size / duration
                split_duration = target_min_bytes / bytes_per_sec
                low, high = max(0, split_duration * 0.9), min(high, split_duration * 1.1)
                best_duration = split_duration

                for _ in range(5):
                    if high - low <= 5.0:
                        break
                    mid = (low + high) / 2
                    temp_file = ospath.join(split_dir, "temp_split.mkv")
                    cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(mid), '-c', 'copy', temp_file]
                    try:
                        await sync_to_async(subprocess.run, cmd, capture_output=True, text=True, check=True, timeout=300)
                        temp_size = await get_path_size(temp_file)
                        LOGGER.debug(f"Test split: {mid:.2f}s, {temp_size / (1024*1024*1024):.2f} GB")
                        await clean_target(temp_file)
                        if temp_size > target_max_bytes:
                            high = mid
                        elif temp_size < target_min_bytes:
                            low = mid
                        else:
                            best_duration = mid
                            break
                        best_duration = mid
                    except Exception as e:
                        LOGGER.error(f"Test split failed: {e}")
                        if await aiopath.exists(temp_file):
                            await clean_target(temp_file)
                        raise Exception(f"Split test failed: {e}")

                cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(best_duration), '-c', 'copy', part_file]
                LOGGER.debug(f"Part {part_num}: {part_file}, duration {best_duration:.2f}s")

            try:
                await sync_to_async(subprocess.run, cmd, capture_output=True, text=True, check=True, timeout=300)
                part_size = await get_path_size(part_file)
                parts_info.append((part_file, start_time, part_size))
                split_files.append(part_file)
                LOGGER.info(f"Created {part_file}: {part_size / (1024*1024*1024):.2f} GB")
                if not is_last_part:
                    start_time += best_duration
            except Exception as e:
                LOGGER.error(f"Split failed for {part_file}: {e}")
                raise Exception(f"Split failed: {e}")

        adjusted_start_time = 0
        for i, (part_file, old_start, part_size) in enumerate(parts_info):
            if not await self._adjust_part_size(input_file, part_file, adjusted_start_time, part_size, target_min_bytes, target_max_bytes, duration):
                LOGGER.warning(f"Adjustment failed for {part_file}")
            new_size = await get_path_size(part_file)
            LOGGER.debug(f"Adjusted {part_file} to {new_size / (1024*1024*1024):.2f} GB")
            adjusted_start_time = old_start + (new_size / (file_size / duration)) if i < num_parts - 1 else duration

        await clean_target(input_file)
        return split_files

    async def upload(self, o_files, m_size):
        await self._user_settings()
        await self._msg_to_reply()
        corrupted_files = total_files = 0
        LOGGER.info(f"Starting upload for {self._listener.name}")

        for dirpath, _, files in sorted(await sync_to_async(walk, self._path)):
            if dirpath.endswith('/yt-dlp-thumb'):
                continue
            for file_ in natsorted(files):
                self._up_path = ospath.join(dirpath, file_)
                if file_.lower().endswith(tuple(self._listener.extensionFilter)) or file_.startswith('Thumb'):
                    if not file_.startswith('Thumb'):
                        await clean_target(self._up_path)
                    continue
                try:
                    f_size = await get_path_size(self._up_path)
                    LOGGER.debug(f"Processing {self._up_path}: {f_size / (1024*1024*1024):.2f} GB")
                    if f_size == 0:
                        corrupted_files += 1
                        LOGGER.error(f"Zero size file: {self._up_path}")
                        continue
                    if self._listener.seed and file_ in o_files and f_size in m_size:
                        continue
                    if self._is_cancelled:
                        return

                    files_to_upload = await self._split_file(self._up_path) if f_size > 1.99 * 1024 * 1024 * 1024 else [self._up_path]
                    for up_path in files_to_upload:
                        self._up_path = up_path
                        caption = await self._prepare_file(ospath.basename(self._up_path), ospath.dirname(self._up_path))
                        if self._last_msg_in_group:
                            group_lists = [x for v in self._media_dict.values() for x in v.keys()]
                            match = re_match(r'.+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)', self._up_path)
                            if not match or match.group(0) not in group_lists:
                                for key, value in list(self._media_dict.items()):
                                    for subkey, msgs in list(value.items()):
                                        if len(msgs) > 1:
                                            await self._send_media_group(msgs, subkey, key)
                        self._last_msg_in_group = False
                        self._last_uploaded = 0
                        await self._upload_file(caption, ospath.basename(self._up_path))
                        total_files += 1
                        if not self._is_corrupted and (self._listener.isSuperChat or self._leech_log):
                            self._msgs_dict[self._send_msg.link] = ospath.basename(self._up_path)
                        await sleep(3)
                except Exception as err:
                    if isinstance(err, RetryError):
                        corrupted_files += 1
                        self._is_corrupted = True
                        err = err.last_attempt.exception()
                    LOGGER.error(f"Error: {err}", exc_info=True)
                    corrupted_files += 1
                    continue
                finally:
                    if not self._is_cancelled and await aiopath.exists(self._up_path) and (
                        not self._listener.seed or self._listener.newDir or
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
        LOGGER.info(f"Leech completed: {self._listener.name}")
        await self._listener.onUploadComplete(None, self._size, self._msgs_dict, total_files, corrupted_files)

    @retry(wait=wait_exponential(multiplier=2, min=4, max=8), stop=stop_after_attempt(4), retry=retry_if_exception_type(Exception))
    async def _upload_file(self, caption, file, force_document=False):
        if self._thumb and not await aiopath.exists(self._thumb):
            self._thumb = None
        thumb, ss_image = self._thumb, None
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
            if is_video:
                duration = (await get_media_info(self._up_path))[0]
                ss_image = await self._gen_ss(self._up_path)
                if self._listener.screenShots:
                    await self._send_screenshots()
                if not thumb:
                    thumb = await create_thumbnail(self._up_path, duration)

            if self._listener.as_doc or force_document or (not is_video and not is_audio and not is_image):
                key = 'documents'
                self._send_msg = await self._client.send_document(chat_id=self._send_msg.chat.id,
                                                                 document=self._up_path,
                                                                 thumb=thumb,
                                                                 caption=caption,
                                                                 disable_notification=True,
                                                                 progress=self._upload_progress,
                                                                 reply_to_message_id=self._send_msg.id)
            elif is_video:
                key = 'videos'
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
            elif is_audio:
                key = 'audios'
                duration, artist, title = await get_media_info(self._up_path)
                self._send_msg = await self._client.send_audio(chat_id=self._send_msg.chat.id,
                                                              audio=self._up_path,
                                                              caption=caption,
                                                              duration=duration,
                                                              performer=artist,
                                                              title=title,
                                                              thumb=thumb,
                                                              disable_notification=True,
                                                              progress=self._upload_progress,
                                                              reply_to_message_id=self._send_msg.id)
            else:
                key = 'photos'
                self._send_msg = await bot.send_photo(chat_id=self._send_msg.chat.id,
                                                     photo=self._up_path,
                                                     caption=caption,
                                                     disable_notification=True,
                                                     progress=self._upload_progress,
                                                     reply_to_message_id=self._send_msg.id)
            await self._final_message(ss_image, bool(is_video or is_audio))
            await self._copy_Leech(self._listener.user_id, self._send_msg)
            if self._listener.upDest:
                await self._copy_Leech(self._listener.upDest, self._send_msg)

            if not self._is_cancelled and self._media_group and (self._send_msg.video or self._send_msg.document):
                if match := re_match(r'.+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)', self._up_path):
                    subkey = match.group(0)
                    if subkey in self._media_dict[key].keys():
                        self._media_dict[key][subkey].append(self._send_msg)
                    else:
                        self._media_dict[key][subkey] = [self._send_msg]
                    msgs = self._media_dict[key][subkey]
                    if len(msgs) == 10:
                        await self._send_media_group(msgs, subkey, key)
                    else:
                        self._last_msg_in_group = True

            if not self._thumb and thumb and await aiopath.exists(thumb):
                await clean_target(thumb)
        except FloodWait as f:
            LOGGER.warning(f"Flood wait: {f.value}s")
            await sleep(f.value * 1.2)
        except Exception as err:
            if not self._thumb and thumb and await aiopath.exists(thumb):
                await clean_target(thumb)
            if isinstance(err, RPCError) and 'Telegram says: [400' in str(err) and key != 'documents':
                LOGGER.error(f"Retrying as document: {err}")
                return await self._upload_file(caption, file, True)
            raise err

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get('media_group', False) or ('media_group' not in self._listener.user_dict and config_dict['MEDIA_GROUP'])
        self._cap_mode = self._listener.user_dict.get('caption_style', 'mono')
        self._log_title = self._listener.user_dict.get('log_title', False)
        self._send_pm = self._listener.user_dict.get('enable_pm', False) and (self._listener.isSuperChat or self._leech_log)
        self._enable_ss = self._listener.user_dict.get('enable_ss', False)
        self._user_caption = self._listener.user_dict.get('captions', False)
        self._user_fnamecap = self._listener.user_dict.get('fnamecap', True)
        if config_dict['AUTO_THUMBNAIL']:
            for dirpath, _, files in await sync_to_async(walk, self._path):
                for file in files:
                    filepath = ospath.join(dirpath, file)
                    if file.startswith('Thumb') and (await get_document_type(filepath))[-1]:
                        self._thumb = filepath
                        break

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except ZeroDivisionError:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._is_cancelled = True
        LOGGER.info(f"Cancelling upload: {self._listener.name}")
        await self._listener.onUploadError('Upload stopped by user!')

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
        if self._user_caption:
            caption = f'{caption}\n\n{self._user_caption}' if self._user_fnamecap else self._user_caption
        return caption

    async def _gen_ss(self, vid_path):
        if not self._enable_ss or self._is_cancelled:
            return None
        ss = GenSS(self._listener.message, vid_path)
        await ss.file_ss()
        return ss.rimage if not ss.error else None

    @handle_message
    async def _msg_to_reply(self):
        if self._leech_log and self._leech_log != self._listener.message.chat.id:
            caption = f'<b>▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n{self._listener.name}\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬</b>'
            if self._thumb and await aiopath.exists(self._thumb):
                self._send_msg = await bot.send_photo(self._leech_log, photo=self._thumb, caption=caption)
            else:
                self._send_msg = await bot.send_message(self._leech_log, caption, disable_web_page_preview=True)
            if config_dict['LEECH_INFO_PIN']:
                await self._send_msg.pin(both_sides=True)
        else:
            self._send_msg = await bot.get_messages(self._listener.message.chat.id, self._listener.mid)
            if not self._send_msg or not self._send_msg.chat:
                self._send_msg = self._listener.message
        if self._send_msg and self._log_title and self._listener.upDest:
            await self._copy_Leech(self._listener.upDest, self._send_msg)

    @handle_message
    async def _send_media_group(self, msgs: list[Message], subkey: str, key: str):
        msgs_list = await msgs[0].reply_to_message.reply_media_group(media=self._get_input_media(subkey, key),
                                                                    quote=True, disable_notification=True)
        await self._copy_media_group(self._listener.user_id, msgs_list)
        if self._listener.upDest:
            await self._copy_media_group(self._listener.upDest, msgs_list)
        for msg in msgs:
            self._msgs_dict.pop(msg.link, None)
            await deleteMessage(msg)
        del self._media_dict[key][subkey]
        if self._listener.isSuperChat or self._leech_log:
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption.split('\n')[0] + ' ~ (Grouped)'
        self._send_msg = msgs_list[-1]

    @handle_message
    async def _send_screenshots(self):
        if isinstance(self._listener.screenShots, str):
            ss_nb = int(self._listener.screenShots)
        else:
            ss_nb = 10
        outputs = await take_ss(self._up_path, ss_nb)
        inputs = []
        if outputs:
            for m in outputs:
                if await aiopath.exists(m):
                    cap = m.rsplit('/', 1)[-1]
                    inputs.append(InputMediaPhoto(m, cap))
                else:
                    outputs.remove(m)
        if outputs:
            msgs_list = await self._send_msg.reply_media_group(media=inputs, quote=True, disable_notification=True)
            await self._copy_media_group(self._listener.user_id, msgs_list)
            if self._listener.upDest:
                await self._copy_media_group(self._listener.upDest, msgs_list)
            self._send_msg = msgs_list[-1]
            await gather(*[clean_target(m) for m in outputs])

    @handle_message
    async def _copy_media_group(self, chat_id: int, msgs: list[Message]):
        captions = [self._caption_mode(msg.caption.split('\n')[0]) for msg in msgs]
        await bot.copy_media_group(chat_id=chat_id, from_chat_id=msgs[0].chat.id, message_id=msgs[0].id, captions=captions)

    @handle_message
    async def _copy_Leech(self, chat_id: int, message: Message):
        reply_markup = await default_button(message) if config_dict['SAVE_MESSAGE'] and self._listener.isSuperChat else message.reply_markup
        return await message.copy(chat_id, disable_notification=True, reply_markup=reply_markup,
                                  reply_to_message_id=message.reply_to_message.id if chat_id == message.chat.id else None)

    @handle_message
    async def _final_message(self, ss_image, media_info: bool=False):
        self._buttons = ButtonMaker()
        media_result = await post_media_info(self._up_path, self._size, ss_image) if media_info else None
        await clean_target(ss_image)
        if media_result:
            self._buttons.button_link('Media Info', media_result)
        if config_dict['SAVE_MESSAGE'] and self._listener.isSuperChat:
            self._buttons.button_data('Save Message', 'save', 'footer')
        for mode, link in zip(['Stream', 'Download'], await gen_link(self._send_msg)):
            if link:
                self._buttons.button_link(mode, await sync_to_async(short_url, link, self._listener.user_id), 'header')
        self._send_msg = await bot.get_messages(self._send_msg.chat.id, self._send_msg.id)
        if (buttons := self._buttons.build_menu(2)) and (cmsg := await self._send_msg.edit_reply_markup(buttons)):
            self._send_msg = cmsg

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