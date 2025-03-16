from aiofiles.os import listdir, path as aiopath, makedirs
from aioshutil import move
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError
from html import escape
from os import path as ospath
from random import choice
from requests import utils as rutils
from time import time

from bot import bot_loop, bot_name, task_dict, task_dict_lock, Intervals, aria2, config_dict, non_queued_up, non_queued_dl, queued_up, queued_dl, queue_dict_lock, LOGGER, DATABASE_URL, bot
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import is_premium_user, sync_to_async, cmd_exec
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_download, clean_target, join_files
from bot.helper.ext_utils.links_utils import is_magnet, is_url, get_link, is_gdrive_link
from bot.helper.ext_utils.shortenurl import short_url
from bot.helper.ext_utils.status_utils import action, get_date_time, get_readable_file_size, get_readable_time
from bot.helper.ext_utils.task_manager import start_from_queued
from bot.helper.ext_utils.telegraph_helper import TelePost
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendingMessage, update_status_message, copyMessage, auto_delete_message
from bot.helper.video_utils.executor import VidEcxecutor
from bot.helper.ext_utils.media_utils import get_media_info

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()

    async def onDownloadStart(self):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def onDownloadComplete(self):
        from bot.helper.mirror_utils.upload_utils.telegram_uploader import TgUploader

        multi_links = False
        if self.sameDir and self.mid in self.sameDir['tasks']:
            while not (self.sameDir['total'] in [1, 0] or self.sameDir['total'] > 1 and len(self.sameDir['tasks']) > 1):
                await sleep(0.5)

        async with task_dict_lock:
            if self.sameDir and self.sameDir['total'] > 1 and self.mid in self.sameDir['tasks']:
                self.sameDir['tasks'].remove(self.mid)
                self.sameDir['total'] -= 1
                folder_name = self.sameDir['name']
                spath = ospath.join(self.dir, folder_name)
                des_path = ospath.join(f'{config_dict["DOWNLOAD_DIR"]}{list(self.sameDir["tasks"])[0]}', folder_name)
                await makedirs(des_path, exist_ok=True)
                for item in await listdir(spath):
                    if item.endswith(('.aria2', '.!qB')):
                        continue
                    item_path = ospath.join(spath, item)
                    if item in await listdir(des_path):
                        await move(item_path, ospath.join(des_path, f'{self.mid}-{item}'))
                    else:
                        await move(item_path, ospath.join(des_path, item))
                multi_links = True
            task = task_dict[self.mid]
            self.name = task.name()
            gid = task.gid()
        LOGGER.info(f"Download completed: {self.name} (MID: {self.mid})")
        if multi_links:
            await self.onUploadError('Downloaded! Waiting for other tasks.')
            return

        up_path = ospath.join(self.dir, self.name)
        if not await aiopath.exists(up_path):
            try:
                files = await listdir(self.dir)
                self.name = files[-1]
                if self.name == 'yt-dlp-thumb':
                    self.name = files[0]
            except Exception as e:
                await self.onUploadError(str(e))
                return

        await self.isOneFile(up_path)
        await self.reName()

        up_path = ospath.join(self.dir, self.name)
        size = await get_path_size(up_path)

        if not config_dict['QUEUE_ALL'] and not config_dict['QUEUE_COMPLETE']:
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
            await start_from_queued()

        if self.join and await aiopath.isdir(up_path):
            await join_files(up_path)

        if self.extract:
            up_path = await self.proceedExtract(up_path, size, gid)
            if not up_path:
                return
            up_dir, self.name = ospath.split(up_path)
            size = await get_path_size(up_dir)

        if self.sampleVideo:
            up_path = await self.generateSampleVideo(up_path, gid)
            if not up_path:
                return
            up_dir, self.name = ospath.split(up_path)
            size = await get_path_size(up_dir)

        if self.compress:
            if self.vidMode:
                up_path = await VidEcxecutor(self, up_path, gid).execute()
                if not up_path:
                    return
                self.seed = False
            up_path = await self.proceedCompress(up_path, size, gid)
            if not up_path:
                return

        if not self.compress and self.vidMode:
            LOGGER.info(f"Processing video with VidEcxecutor for MID: {self.mid}")
            up_path = await VidEcxecutor(self, up_path, gid).execute()
            if not up_path:
                return
            self.seed = False
            up_dir, self.name = ospath.split(up_path)
            size = await get_path_size(up_dir)

        o_files, m_size = [], []
        TELEGRAM_LIMIT = 2097152000  # 2,000 MiB
        SPLIT_SIZE = 2048000000  # 1.95GB to stay safely under limit

        if size > TELEGRAM_LIMIT and await aiopath.isfile(up_path):
            LOGGER.info(f"Splitting file {self.name} (size: {size}) into parts of {SPLIT_SIZE} bytes")
            o_files, m_size = await self._split_file(up_path)
            if not o_files:
                await self.onUploadError(f"Failed to split {self.name} into parts.")
                return
            for i, f_size in enumerate(m_size):
                if f_size > TELEGRAM_LIMIT:
                    LOGGER.error(f"Split file {o_files[i]} size {f_size} exceeds Telegram limit of {TELEGRAM_LIMIT} bytes")
                    await self.onUploadError("Split file exceeds Telegram 2,000 MiB limit.")
                    return
        else:
            o_files.append(up_path)
            m_size.append(size)

        LOGGER.info(f"Leeching {self.name} (MID: {self.mid}) with o_files: {o_files}, m_size: {m_size}")
        tg = TgUploader(self, self.dir, size)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')
        try:
            for f in o_files:
                if not await aiopath.exists(f):
                    LOGGER.error(f"File not found before upload: {f}")
                    raise FileNotFoundError(f"Missing split file: {f}")
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
        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
        await start_from_queued()

    async def _split_file(self, file_path):
        try:
            TELEGRAM_LIMIT = 2097152000  # 2,000 MiB
            SPLIT_SIZE = 2048000000  # 1.95GB
            MIN_PART_SIZE = 10 * 1024 * 1024  # 10MB
            file_size = await get_path_size(file_path)
            if file_size <= TELEGRAM_LIMIT:
                LOGGER.info(f"File size {file_size} <= Telegram limit, no splitting needed")
                return [file_path], [file_size]

            base_name = ospath.splitext(ospath.basename(file_path))[0]
            output_dir = ospath.dirname(file_path)
            total_duration = (await get_media_info(file_path))[0]  # Total duration in seconds
            max_parts = (file_size + SPLIT_SIZE - 1) // SPLIT_SIZE  # Ceiling division

            o_files, m_size = [], []
            total_split_size = 0
            start_time = 0
            part_num = 0
            size_tolerance = 1024 * 1024  # 1MB tolerance

            while total_split_size < file_size - size_tolerance and part_num < max_parts:
                output_file = ospath.join(output_dir, f"{base_name}_part{part_num:03d}.mkv")
                cmd_ffmpeg = [
                    'ffmpeg', '-i', file_path, '-ss', str(start_time), '-fs', str(SPLIT_SIZE),
                    '-c', 'copy', '-map', '0', output_file, '-y'
                ]
                LOGGER.info(f"Running split cmd: {' '.join(cmd_ffmpeg)}")
                _, stderr, rcode = await cmd_exec(cmd_ffmpeg)
                if rcode != 0:
                    LOGGER.error(f"FFmpeg split failed for {file_path} part {part_num}: {stderr}")
                    await clean_target(output_file)
                    return [], []

                if await aiopath.exists(output_file):
                    part_size = await get_path_size(output_file)
                    LOGGER.info(f"Generated split file: {output_file} with size: {part_size}")
                    if part_size > TELEGRAM_LIMIT:
                        LOGGER.error(f"Part {output_file} size {part_size} exceeds {TELEGRAM_LIMIT} bytes")
                        await clean_target(output_file)
                        return [], []
                    if part_size < MIN_PART_SIZE and total_split_size > 0:
                        LOGGER.info(f"Part {output_file} size {part_size} below minimum {MIN_PART_SIZE}, discarding")
                        await clean_target(output_file)
                        break
                    o_files.append(output_file)
                    m_size.append(part_size)
                    total_split_size += part_size
                    part_duration = (part_size / file_size) * total_duration if file_size > 0 else 0
                    start_time += part_duration
                    part_num += 1
                    LOGGER.info(f"Total split size: {total_split_size}, remaining: {file_size - total_split_size}")
                else:
                    LOGGER.error(f"Split part {output_file} not created")
                    return [], []

            if file_size - total_split_size > MIN_PART_SIZE:
                output_file = ospath.join(output_dir, f"{base_name}_part{part_num:03d}.mkv")
                cmd_ffmpeg = [
                    'ffmpeg', '-i', file_path, '-ss', str(start_time), '-c', 'copy', '-map', '0', output_file, '-y'
                ]
                LOGGER.info(f"Running final split cmd: {' '.join(cmd_ffmpeg)}")
                _, stderr, rcode = await cmd_exec(cmd_ffmpeg)
                if rcode != 0:
                    LOGGER.error(f"FFmpeg final split failed for {file_path}: {stderr}")
                    await clean_target(output_file)
                    return [], []
                if await aiopath.exists(output_file):
                    part_size = await get_path_size(output_file)
                    if part_size <= TELEGRAM_LIMIT:
                        o_files.append(output_file)
                        m_size.append(part_size)
                        total_split_size += part_size
                        LOGGER.info(f"Final split file: {output_file} with size: {part_size}")
                    else:
                        LOGGER.error(f"Final part {output_file} size {part_size} exceeds {TELEGRAM_LIMIT}")
                        await clean_target(output_file)
                        return [], []

            if not o_files:
                LOGGER.error(f"No valid split files generated for {file_path}")
                return [], []

            LOGGER.info(f"Split {file_path} into {len(o_files)} parts: {o_files}, total size: {total_split_size}")
            return o_files, m_size
        except Exception as e:
            LOGGER.error(f"Split file error for {file_path}: {e}", exc_info=True)
            return [], []

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
        msg += (f'<b>├ Elapsed: </b>{get_readable_time(time() - self.message.date.timestamp())}\n'
                f'<b>├ Cc: </b>{self.tag}\n'
                f'<b>└ Action: </b>{action(self.message)}\n\n')
        if files:
            fmsg = '<b>Leech File(s):</b>\n'
            for index, (tlink, name) in enumerate(files.items(), start=1):
                fmsg += f'{index}. <a href="{tlink}">{name}</a>\n'
            msg += fmsg

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