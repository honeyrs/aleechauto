from aiofiles.os import path as aiopath, listdir, makedirs, remove
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError
from html import escape
from os import path as ospath
from time import time
from bot import bot, bot_lock, config_dict, LOGGER, DATABASE_URL, task_dict, task_dict_lock
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import cmd_exec, sync_to_async
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_download, clean_target
from bot.helper.ext_utils.links_utils import is_magnet, is_url, get_link
from bot.helper.ext_utils.media_utils import get_document_type, get_media_info, create_thumbnail
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendingMessage, copyMessage, auto_delete_message, update_status_message
from bot.helper.ext_utils.telegraph_helper import TelePost

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self._is_cancelled = False

    async def onDownloadComplete(self):
        up_path = ospath.join(self.dir, self.name)
        if not await aiopath.exists(up_path):
            try:
                files = await listdir(self.dir)
                self.name = files[-1] if files else self.name
            except Exception as e:
                await self.onUploadError(f"File not found: {str(e)}")
                return

        size = await get_path_size(up_path)
        TELEGRAM_LIMIT = 2097152000  # 2 GB exact

        o_files, m_size = [], []
        if size > TELEGRAM_LIMIT and await aiopath.isfile(up_path):
            LOGGER.info(f"Splitting file {self.name} (size: {size}) into 2 GB parts")
            o_files, m_size = await self._split_file(up_path)
            if not o_files:
                await self.onUploadError(f"Failed to split {self.name} into parts.")
                return
        else:
            o_files.append(up_path)
            m_size.append(size)

        LOGGER.info(f"Preparing to leech {self.name} (MID: {self.mid}) with o_files: {o_files}, m_size: {m_size}")
        tg = TgUploader(self, self.dir, size)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, size, self.gid(), 'up')

        try:
            for f in o_files:
                if not await aiopath.exists(f):
                    raise FileNotFoundError(f"Missing file: {f}")
            await wait_for(gather(update_status_message(self.message.chat.id), tg.upload(o_files, m_size)), timeout=600)
            LOGGER.info(f"Leech Completed: {self.name} (MID: {self.mid})")
        except AsyncTimeoutError:
            LOGGER.error(f"Upload timeout for MID: {self.mid}")
            await self.onUploadError("Upload timed out after 10 minutes.")
            return
        except Exception as e:
            LOGGER.error(f"Upload error for MID: {self.mid}: {e}", exc_info=True)
            await self.onUploadError(f"Upload failed: {str(e)}")
            return

        await clean_download(self.dir)
        async with task_dict_lock:
            task_dict.pop(self.mid, None)

    async def _split_file(self, file_path):
        TELEGRAM_LIMIT = 2097152000  # 2 GB exact
        try:
            file_size = await get_path_size(file_path)
            if file_size <= TELEGRAM_LIMIT:
                return [file_path], [file_size]

            base_name = ospath.splitext(ospath.basename(file_path))[0]
            output_dir = ospath.dirname(file_path)
            num_full_parts = file_size // TELEGRAM_LIMIT
            remainder_size = file_size % TELEGRAM_LIMIT

            LOGGER.info(f"Splitting {file_path} (size: {file_size}) into {num_full_parts} full 2 GB parts and remainder {remainder_size}")

            o_files, m_size = [], []
            duration = (await get_media_info(file_path))[0] or (file_size / 1000000)  # Fallback bitrate
            bytes_per_second = file_size / duration

            for i in range(num_full_parts):
                output_file = ospath.join(output_dir, f"{base_name}_part{i:03d}.mkv")
                start_time = i * (TELEGRAM_LIMIT / bytes_per_second)
                part_duration = TELEGRAM_LIMIT / bytes_per_second

                cmd = ['ffmpeg', '-i', file_path, '-ss', str(start_time), '-t', str(part_duration),
                       '-c', 'copy', '-map', '0', '-f', 'matroska', output_file, '-y']
                _, stderr, rcode = await cmd_exec(cmd)
                if rcode != 0:
                    LOGGER.error(f"FFmpeg split failed for part {i}: {stderr}")
                    await self._cleanup_files(o_files)
                    return [], []

                part_size = await get_path_size(output_file)
                o_files.append(output_file)
                m_size.append(part_size)

            if remainder_size > 0:
                output_file = ospath.join(output_dir, f"{base_name}_part{num_full_parts:03d}.mkv")
                start_time = num_full_parts * (TELEGRAM_LIMIT / bytes_per_second)
                cmd = ['ffmpeg', '-i', file_path, '-ss', str(start_time),
                       '-c', 'copy', '-map', '0', '-f', 'matroska', output_file, '-y']
                _, stderr, rcode = await cmd_exec(cmd)
                if rcode != 0:
                    LOGGER.error(f"FFmpeg split failed for remainder: {stderr}")
                    await self._cleanup_files(o_files)
                    return [], []

                part_size = await get_path_size(output_file)
                o_files.append(output_file)
                m_size.append(part_size)

            total_split_size = sum(m_size)
            if total_split_size != file_size:
                LOGGER.error(f"Split size mismatch: original={file_size}, split_total={total_split_size}")
                await self._cleanup_files(o_files)
                return [], []

            LOGGER.info(f"Split {file_path} into {len(o_files)} parts: {o_files}")
            return o_files, m_size
        except Exception as e:
            LOGGER.error(f"Error splitting {file_path}: {e}", exc_info=True)
            await self._cleanup_files(o_files if 'o_files' in locals() else [])
            return [], []

    async def _cleanup_files(self, files):
        for f in files:
            if await aiopath.exists(f):
                await remove(f)
        LOGGER.info(f"Cleaned up files: {files}")

    async def onUploadComplete(self, link, size, files, folders, mime_type):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)

        size_str = get_readable_file_size(size)
        msg = f'<code>{escape(self.name)}</code>\n'
        msg += f'<b>┌ Size: </b>{size_str}\n'
        msg += f'<b>├ Total Files: </b>{folders}\n'
        if mime_type and mime_type != 0:
            msg += f'<b>├ Corrupted Files: </b>{mime_type}\n'
        msg += f'<b>├ Elapsed: </b>{get_readable_time(time() - self.message.date.timestamp())}\n'
        msg += f'<b>└ Cc: </b>{self.tag}\n'
        if files:
            msg += '<b>Leech File(s):</b>\n'
            for index, (tlink, name) in enumerate(files.items(), start=1):
                msg += f'{index}. <a href="{tlink}">{name}</a>\n'

        buttons = ButtonMaker()
        if config_dict['SOURCE_LINK']:
            scr_link = get_link(self.message)
            if is_magnet(scr_link):
                tele = TelePost(config_dict['SOURCE_LINK_TITLE'])
                mag_link = await sync_to_async(tele.create_post, f'<code>{escape(self.name)}<br>({size_str})</code><br>{scr_link}')
                buttons.button_link('Source Link', mag_link)
            elif is_url(scr_link):
                buttons.button_link('Source Link', scr_link)

        try:
            uploadmsg = await sendingMessage(msg, self.message, None, buttons.build_menu(2))
            if not uploadmsg:
                raise ValueError("Failed to send upload message")
        except Exception as e:
            LOGGER.error(f"Error sending upload message for MID: {self.mid}: {e}")
            return

        if self.user_dict.get('enable_pm') and self.isSuperChat:
            await copyMessage(self.user_id, uploadmsg)
        if chat_id := config_dict.get('LEECH_LOG'):
            await copyMessage(chat_id, uploadmsg)

    async def onUploadError(self, error):
        LOGGER.error(f"Upload error for MID: {self.mid}: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Upload failed: {error}", self.message, None)
        await clean_download(self.dir)

class TgUploader:
    def __init__(self, listener, path: str, size: int):
        self._listener = listener
        self._path = path
        self._size = size
        self._start_time = time()
        self._processed_bytes = 0
        self._is_cancelled = False
        self._thumb = self._listener.thumb if self._listener.thumb and await aiopath.exists(self._listener.thumb) else None
        self._msgs_dict = {}
        self._client = None
        self._send_msg = None

    async def _upload_progress(self, current, _):
        if self._is_cancelled:
            self._client.stop_transmission()
        chunk_size = current - self._processed_bytes
        self._processed_bytes += chunk_size

    async def upload(self, o_files, m_size):
        await self._msg_to_reply()
        total_files = len(o_files)
        uploaded_files = 0

        for i, file_path in enumerate(o_files):
            if self._is_cancelled:
                return
            if not await aiopath.exists(file_path):
                LOGGER.error(f"File not found for upload: {file_path}")
                continue

            part_num = i + 1
            caption = f"{ospath.basename(file_path)} (Part {part_num} of {total_files})"
            self._processed_bytes = 0

            try:
                await self._upload_file(caption, file_path)
                uploaded_files += 1
                self._msgs_dict[self._send_msg.link] = ospath.basename(file_path)
                await sleep(3)  # Avoid rate limits
            except Exception as e:
                LOGGER.error(f"Upload failed for {file_path}: {e}")
                continue
            finally:
                if not self._is_cancelled and await aiopath.exists(file_path):
                    await clean_target(file_path)

        if self._is_cancelled:
            return
        if uploaded_files == 0:
            await self._listener.onUploadError("No files uploaded successfully!")
            return
        if uploaded_files < total_files:
            await self._listener.onUploadError(f"Only {uploaded_files}/{total_files} files uploaded successfully. Check logs!")
            return

        if sum(m_size) != self._size:
            LOGGER.error(f"Total uploaded size {sum(m_size)} does not match original size {self._size}")
            await self._listener.onUploadError("Uploaded size mismatch detected!")
            return

        LOGGER.info(f"Upload completed: {self._listener.name}")
        await self._listener.onUploadComplete(None, self._size, self._msgs_dict, total_files, total_files - uploaded_files)

    async def _upload_file(self, caption, up_path):
        if not await aiopath.exists(up_path):
            raise FileNotFoundError(f"Upload path missing: {up_path}")

        thumb = self._thumb
        async with bot_lock:
            self._client = bot

        is_video, is_audio, is_image = await get_document_type(up_path)
        LOGGER.debug(f"File type for {up_path}: video={is_video}, audio={is_audio}, image={is_image}")

        if is_video and not thumb:
            duration = (await get_media_info(up_path))[0]
            thumb = await create_thumbnail(up_path, duration)
            if not thumb or not await aiopath.exists(thumb):
                LOGGER.warning(f"Thumbnail creation failed for {up_path}, using None")
                thumb = None

        if self._listener.as_doc or (not is_video and not is_audio and not is_image):
            LOGGER.debug(f"Uploading {up_path} as document")
            self._send_msg = await self._client.send_document(
                chat_id=self._send_msg.chat.id,
                document=up_path,
                thumb=thumb if thumb and await aiopath.exists(thumb) else None,
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
                thumb=thumb if thumb and await aiopath.exists(thumb) else None,
                caption=caption,
                duration=duration,
                disable_notification=True,
                progress=self._upload_progress,
                reply_to_message_id=self._send_msg.id
            )
        else:
            LOGGER.debug(f"Uploading {up_path} as document (default)")
            self._send_msg = await self._client.send_document(
                chat_id=self._send_msg.chat.id,
                document=up_path,
                thumb=thumb if thumb and await aiopath.exists(thumb) else None,
                caption=caption,
                disable_notification=True,
                progress=self._upload_progress,
                reply_to_message_id=self._send_msg.id
            )

    async def _msg_to_reply(self):
        self._send_msg = self._listener.message