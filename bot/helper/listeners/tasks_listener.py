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
        max_upload_size = 4 * 1024**3 if is_premium_user(self.user_id) else 2 * 1024**3
        LOGGER.info(f"File size: {size}, Max upload size: {max_upload_size}, Is premium: {is_premium_user(self.user_id)}")
        if size > max_upload_size and await aiopath.isfile(up_path):
            LOGGER.info(f"Splitting file {self.name} (size: {size}) into parts maximized to {max_upload_size // 1024**2} MB")
            o_files, m_size = await self._split_file(up_path, size, gid)
            if not o_files:
                await self.onUploadError(f"Failed to split {self.name} into parts.")
                return
        else:
            LOGGER.info(f"No split needed for {self.name} (size: {size} <= {max_upload_size})")
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
            if not self._is_cancelled:
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
        """Split file into parts ≤ split_size (2GB basic, 4GB premium) using FFmpeg."""
        split_size = config_dict.get('LEECH_SPLIT_SIZE', DEFAULT_SPLIT_SIZE)
        if is_premium_user(self.user_id):
            split_size = min(max(split_size, 4 * 1024**3), 4 * 1024**3)  # Max 4GB for premium
        else:
            split_size = min(max(split_size, 2 * 1024**3), 2 * 1024**3)  # Max 2GB for basic

        LOGGER.info(f"Splitting {file_path} (size: {size}) with split_size: {split_size}")

        if size <= split_size:
            LOGGER.info(f"File size {size} <= split_size {split_size}, no split required")
            return [file_path], [size]

        o_files, m_size = [], []
        output_dir = ospath.dirname(file_path)
        base_name = ospath.splitext(ospath.basename(file_path))[0]

        # Get total duration
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(file_path)
        ]
        _, stdout, stderr = await cmd_exec(cmd)
        if stderr:
            LOGGER.error(f"ffprobe failed: {stderr}")
            return [], []
        duration = float(stdout.strip())
        LOGGER.info(f"File duration: {duration} seconds")

        # Calculate number of parts
        num_parts = max(1, math.ceil(size / split_size))
        LOGGER.info(f"Total parts needed: {num_parts}")

        start_time = 0
        bytes_per_second = size / duration

        for part_num in range(num_parts):
            part_file = ospath.join(output_dir, f"{base_name}_part{part_num + 1:03d}{ospath.splitext(file_path)[1]}")
            remaining_bytes = size - (part_num * split_size)

            if remaining_bytes <= split_size:
                # Last part: remainder
                cmd = [
                    "ffmpeg", "-i", str(file_path), "-ss", str(start_time),
                    "-c", "copy", "-map", "0", "-y", str(part_file)
                ]
            else:
                # Full part: ≤ split_size
                part_duration = split_size / bytes_per_second
                cmd = [
                    "ffmpeg", "-i", str(file_path), "-ss", str(start_time),
                    "-fs", str(split_size), "-c", "copy", "-map", "0", "-y", str(part_file)
                ]

            LOGGER.info(f"Executing FFmpeg for part {part_num + 1}: {' '.join(cmd)}")
            _, stdout, stderr = await cmd_exec(cmd)
            LOGGER.info(f"FFmpeg stdout (part {part_num + 1}): {stdout}")
            if stderr:
                LOGGER.error(f"FFmpeg stderr (part {part_num + 1}): {stderr}")
            if not await aiopath.exists(part_file):
                LOGGER.error(f"Part {part_file} not created!")
                await self._cleanup_files(o_files)
                return [], []

            part_size = await get_path_size(part_file)
            if part_size > split_size:
                LOGGER.error(f"Part {part_file} size {part_size} exceeds split_size {split_size}")
                await self._cleanup_files(o_files)
                return [], []

            o_files.append(part_file)
            m_size.append(part_size)
            LOGGER.info(f"Created {part_file}, size: {part_size} bytes")

            # Update start_time
            part_duration = await sync_to_async(get_duration, part_file)
            if part_duration is None:
                LOGGER.error(f"Failed to get duration for {part_file}")
                await self._cleanup_files(o_files)
                return [], []
            start_time += part_duration
            LOGGER.info(f"Part {part_num + 1} duration: {part_duration}, New start_time: {start_time}")

        total_split_size = sum(m_size)
        LOGGER.info(f"Split complete. Total size: {total_split_size}, Original: {size}")
        if abs(total_split_size - size) > 1024 * 1024:  # 1MB tolerance
            LOGGER.warning(f"Size mismatch: original={size}, split_total={total_split_size}")
        return o_files, m_size

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
        await start_from_queued()

# TgUploader remains unchanged as provided