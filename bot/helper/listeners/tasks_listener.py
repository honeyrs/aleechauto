from aiofiles.os import listdir, path as aiopath, makedirs
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError
from html import escape
from os import path as ospath
from time import time

from bot import bot, bot_loop, task_dict, task_dict_lock, config_dict, non_queued_up, non_queued_dl, queued_up, queued_dl, queue_dict_lock, LOGGER, DATABASE_URL, bot_lock, DEFAULT_SPLIT_SIZE
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import cmd_exec, sync_to_async, is_premium_user
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_download, clean_target, join_files
from bot.helper.ext_utils.links_utils import is_magnet, is_url, get_link
from bot.helper.ext_utils.status_utils import action, get_readable_file_size, get_readable_time
from bot.helper.ext_utils.task_manager import start_from_queued, check_running_tasks
from bot.helper.ext_utils.telegraph_helper import TelePost
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendingMessage, update_status_message, copyMessage, auto_delete_message
from bot.helper.video_utils.executor import VidEcxecutor
from bot.helper.ext_utils.media_utils import get_document_type, get_media_info, create_thumbnail

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self._is_cancelled = False

    async def onDownloadStart(self):
        LOGGER.info(f"Download started for MID: {self.mid}")
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def onDownloadComplete(self):
        async with task_dict_lock:
            task = task_dict[self.mid]
            self.name = task.name()
            gid = task.gid()

        if self.sameDir and self.mid in self.sameDir['tasks']:
            folder_name = self.sameDir['name']
            des_path = ospath.join(self.dir, folder_name)
            await makedirs(des_path, exist_ok=True)
            async with queue_dict_lock:
                self.sameDir['tasks'].remove(self.mid)
                self.sameDir['total'] -= 1
            LOGGER.info(f"Consolidating files for MID: {self.mid} into {des_path}")
            for item in await listdir(self.dir):
                if item.endswith(('.aria2', '.!qB')) or item == folder_name:
                    continue
                item_path = ospath.join(self.dir, item)
                target_path = ospath.join(des_path, item)
                if await aiopath.exists(target_path):
                    await clean_target(item_path)
                else:
                    await sync_to_async(ospath.rename, item_path, target_path)
            if self.sameDir['total'] > 0:
                LOGGER.info(f"Waiting for other tasks in sameDir for MID: {self.mid}")
                return
            self.name = folder_name
            up_path = des_path
        else:
            up_path = ospath.join(self.dir, self.name)

        if not await aiopath.exists(up_path):
            try:
                files = await listdir(self.dir)
                self.name = files[-1] if files else self.name
                up_path = ospath.join(self.dir, self.name)
            except Exception as e:
                await self.onUploadError(f"File not found: {str(e)}")
                return

        size = await get_path_size(up_path)
        TELEGRAM_LIMIT = 2097152000  # 2 GB for free users

        if self.join and await aiopath.isdir(up_path):
            await join_files(up_path)

        if self.extract:
            up_path = await self.proceedExtract(up_path, size, gid)
            if not up_path:
                return
            self.name = ospath.basename(up_path)
            size = await get_path_size(up_path)

        if self.sampleVideo:
            up_path = await self.generateSampleVideo(up_path, gid)
            if not up_path:
                return
            self.name = ospath.basename(up_path)
            size = await get_path_size(up_path)

        if self.compress:
            if self.vidMode:
                up_path = await VidEcxecutor(self, up_path, gid).execute()
                if not up_path:
                    return
                self.seed = False
            up_path = await self.proceedCompress(up_path, size, gid)
            if not up_path:
                return
            self.name = ospath.basename(up_path)
            size = await get_path_size(up_path)

        if not self.compress and self.vidMode:
            LOGGER.info(f"Processing video with VidEcxecutor for MID: {self.mid}")
            up_path = await VidEcxecutor(self, up_path, gid).execute()
            if not up_path:
                return
            self.seed = False
            self.name = ospath.basename(up_path)
            size = await get_path_size(up_path)

        if not await aiopath.exists(up_path):
            await self.onUploadError(f"Processed file {self.name} not found!")
            return

        o_files, m_size = [], []
        if size > TELEGRAM_LIMIT and await aiopath.isfile(up_path):
            LOGGER.info(f"Splitting file {self.name} (size: {size}) into parts")
            o_files, m_size = await self._split_file(up_path, size, gid)
            if not o_files:
                await self.onUploadError(f"Failed to split {self.name} into parts.")
                return
        else:
            o_files.append(up_path)
            m_size.append(size)

        LOGGER.info(f"Preparing to leech {self.name} (MID: {self.mid}) with o_files: {o_files}, m_size: {m_size}")
        add_to_queue, event = await check_running_tasks(self.mid, 'up')
        if add_to_queue:
            LOGGER.info(f"Added to upload queue: {self.name} (MID: {self.mid})")
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, None, size, gid, 'Queue')
            await wait_for(event.wait(), timeout=300)
            if self._is_cancelled:
                return

        tg = TgUploader(self, self.dir, size)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')

        try:
            for f in o_files:
                if not await aiopath.exists(f):
                    raise FileNotFoundError(f"Missing file: {f}")
            await wait_for(gather(update_status_message(self.message.chat.id), tg.upload(o_files, m_size)), timeout=600)
            LOGGER.info(f"Leech Completed: {self.name} (MID: {self.mid})")
            await clean_download(self.dir)
        except AsyncTimeoutError:
            LOGGER.error(f"Upload timeout for MID: {self.mid}")
            await self.onUploadError("Upload timed out after 10 minutes.")
        except Exception as e:
            LOGGER.error(f"Upload error for MID: {self.mid}: {e}", exc_info=True)
            await self.onUploadError(f"Upload failed: {str(e)}")
        finally:
            async with task_dict_lock:
                task_dict.pop(self.mid, None)
            async with queue_dict_lock:
                if self.mid in non_queued_up:
                    non_queued_up.remove(self.mid)
            await start_from_queued()

    async def _split_file(self, file_path, size, gid):
        """Split logic adapted from parent project's proceedSplit"""
        # Determine split size based on user type and config
        split_size = config_dict.get('LEECH_SPLIT_SIZE', DEFAULT_SPLIT_SIZE)
        if is_premium_user(self.user_id):
            split_size = max(split_size, 4 * 1024**3)  # 4GB for premium users
        else:
            split_size = min(split_size, 2 * 1024**3)  # Cap at 2GB for basic users

        LOGGER.info(f"Splitting {file_path} (size: {size}) with split_size: {split_size}")

        # If file is smaller than split_size, no split needed
        if size <= split_size:
            LOGGER.info(f"File size {size} <= split_size {split_size}, no split required")
            return [file_path], [size]

        # Prepare output
        o_files, m_size = [], []
        base_name = ospath.splitext(ospath.basename(file_path))[0]
        output_dir = ospath.dirname(file_path)

        # Split file into parts
        try:
            num_parts = (size + split_size - 1) // split_size  # Ceiling division
            part_size_target = size // num_parts  # Aim for equal parts where possible

            for part in range(num_parts):
                output_file = ospath.join(output_dir, f"{base_name}_part{part:03d}{ospath.splitext(file_path)[1]}")
                start_byte = part * part_size_target
                end_byte = min((part + 1) * part_size_target, size)

                # Use dd or similar for splitting (async-friendly)
                cmd = ['dd', f'if={file_path}', f'of={output_file}', f'bs={part_size_target}', f'skip={part}', 'count=1', 'status=none']
                _, stderr, rcode = await cmd_exec(cmd)
                if rcode != 0:
                    LOGGER.error(f"Split failed for part {part}: {stderr}")
                    await self._cleanup_files(o_files)
                    return [], []

                if not await aiopath.exists(output_file):
                    LOGGER.error(f"Part {output_file} not created")
                    await self._cleanup_files(o_files)
                    return [], []

                part_size = await get_path_size(output_file)
                if part_size > split_size:
                    LOGGER.warning(f"Part {output_file} size {part_size} exceeds split_size {split_size}, retrying not implemented")
                    await self._cleanup_files(o_files)
                    return [], []

                o_files.append(output_file)
                m_size.append(part_size)

            total_split_size = sum(m_size)
            if abs(total_split_size - size) > 1024 * 1024:  # 1MB tolerance
                LOGGER.error(f"Split size mismatch: original={size}, split_total={total_split_size}")
                await self._cleanup_files(o_files)
                return [], []

            LOGGER.info(f"Successfully split {file_path} into {len(o_files)} parts: {o_files}")
            return o_files, m_size

        except Exception as e:
            LOGGER.error(f"Error splitting {file_path}: {e}", exc_info=True)
            await self._cleanup_files(o_files if 'o_files' in locals() else [])
            return [], []

    async def _cleanup_files(self, files):
        for f in files:
            if await aiopath.exists(f):
                await clean_target(f)
        LOGGER.info(f"Cleaned up files: {files}")

    async def onUploadComplete(self, link, size, files, folders, mime_type):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)

        LOGGER.info(f"Task Done: {self.name} (MID: {self.mid})")
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

        uploadmsg = await sendingMessage(msg, self.message, None, buttons.build_menu(2))
        if self.user_dict.get('enable_pm') and self.isSuperChat:
            await copyMessage(self.user_id, uploadmsg)
        if chat_id := config_dict.get('LEECH_LOG'):
            await copyMessage(chat_id, uploadmsg)

        if self.isSuperChat and (stime := config_dict['AUTO_DELETE_UPLOAD_MESSAGE_DURATION']):
            bot_loop.create_task(auto_delete_message(self.message, uploadmsg, self.message.reply_to_message, stime=stime))

    async def onUploadError(self, error):
        LOGGER.error(f"Upload error for MID: {self.mid}: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Upload failed: {error}", self.message, None)
        await gather(start_from_queued(), clean_download(self.dir))

class TgUploader:
    def __init__(self, listener, path: str, size: int):
        self._listener = listener
        self._path = path
        self._size = size
        self._start_time = time()
        self._processed_bytes = 0
        self._is_cancelled = False
        self._thumb = listener.thumb
        self._msgs_dict = {}
        self._client = None
        self._send_msg = None
        self._corrupted_files = 0

    async def _upload_progress(self, current, _):
        if self._is_cancelled:
            self._client.stop_transmission()
        chunk_size = current - self._processed_bytes
        self._processed_bytes += chunk_size

    async def upload(self, o_files, m_size):
        await self._msg_to_reply()
        total_files = len(o_files)
        uploaded_files = 0
        TELEGRAM_LIMIT = 2097152000  # 2 GB for free users

        for i, file_path in enumerate(o_files):
            if self._is_cancelled:
                LOGGER.info(f"Upload cancelled for MID: {self._listener.mid}")
                return
            if not await aiopath.exists(file_path):
                LOGGER.error(f"File not found for upload: {file_path}")
                self._corrupted_files += 1
                continue

            part_num = i + 1
            caption = f"{ospath.basename(file_path)} (Part {part_num} of {total_files})"
            self._processed_bytes = 0

            try:
                f_size = m_size[i]
                if f_size > TELEGRAM_LIMIT:
                    LOGGER.error(f"File {file_path} size {f_size} exceeds Telegram limit {TELEGRAM_LIMIT}")
                    self._corrupted_files += 1
                    continue
                await self._upload_file(caption, file_path)
                uploaded_files += 1
                self._msgs_dict[self._send_msg.link] = ospath.basename(file_path)
                await sleep(3)
            except Exception as e:
                LOGGER.error(f"Upload failed for {file_path}: {e}")
                self._corrupted_files += 1
                continue
            finally:
                if not self._is_cancelled and await aiopath.exists(file_path) and not self._listener.seed:
                    await clean_target(file_path)

        if self._is_cancelled:
            return
        if uploaded_files == 0:
            await self._listener.onUploadError("No files uploaded successfully!")
            return
        if uploaded_files < total_files:
            await self._listener.onUploadError(f"Only {uploaded_files}/{total_files} files uploaded successfully! Check logs.")
            return

        LOGGER.info(f"Upload completed: {self._listener.name}")
        await self._listener.onUploadComplete(None, self._size, self._msgs_dict, total_files, self._corrupted_files)

    async def _upload_file(self, caption, up_path):
        file_size = await get_path_size(up_path)
        TELEGRAM_LIMIT = 2097152000  # 2 GB for free users
        if file_size > TELEGRAM_LIMIT:
            raise ValueError(f"File {up_path} size {file_size} exceeds Telegram 2 GB limit")

        if not await aiopath.exists(up_path):
            raise FileNotFoundError(f"Upload path missing: {up_path}")

        thumb = self._thumb
        if thumb and not await aiopath.exists(thumb):
            LOGGER.warning(f"Thumbnail {thumb} does not exist, using None")
            thumb = None

        async with bot_lock:
            self._client = bot

        is_video, is_audio, is_image = await get_document_type(up_path)
        LOGGER.debug(f"File type for {up_path}: video={is_video}, audio={is_audio}, image={is_image}")

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
        LOGGER.info(f"Uploaded {up_path} successfully")

    async def _msg_to_reply(self):
        self._send_msg = self._listener.message