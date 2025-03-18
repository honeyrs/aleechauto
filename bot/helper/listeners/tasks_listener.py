from aiofiles.os import listdir, path as aiopath, makedirs
from aioshutil import move
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError
from html import escape
from os import path as ospath
from random import choice
from requests import utils as rutils
from time import time
import subprocess
import math

from bot import bot_loop, bot_name, task_dict, task_dict_lock, Intervals, aria2, config_dict, non_queued_up, non_queued_dl, queued_up, queued_dl, queue_dict_lock, LOGGER, DATABASE_URL, bot
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import is_premium_user, UserDaily, default_button, sync_to_async
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_download, clean_target, join_files
from bot.helper.ext_utils.links_utils import is_magnet, is_url, get_link, is_media, is_gdrive_link, get_stream_link, is_gdrive_id
from bot.helper.ext_utils.shortenurl import short_url
from bot.helper.ext_utils.status_utils import action, get_date_time, get_readable_file_size, get_readable_time
from bot.helper.ext_utils.task_manager import start_from_queued, check_running_tasks
from bot.helper.ext_utils.telegraph_helper import TelePost
from bot.helper.mirror_utils.gdrive_utlis.upload import gdUpload
from bot.helper.mirror_utils.rclone_utils.transfer import RcloneTransferHelper
from bot.helper.mirror_utils.status_utils.gdrive_status import GdriveStatus
from bot.helper.mirror_utils.status_utils.gofile_upload_status import GofileUploadStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.mirror_utils.status_utils.rclone_status import RcloneStatus
from bot.helper.mirror_utils.status_utils.telegram_status import TelegramStatus
from bot.helper.mirror_utils.upload_utils.gofile_uploader import GoFileUploader
from bot.helper.mirror_utils.upload_utils.telegram_uploader import TgUploader
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import limit, sendCustom, sendMedia, sendMessage, auto_delete_message, sendSticker, sendFile, copyMessage, sendingMessage, update_status_message, delete_status
from bot.helper.video_utils.executor import VidEcxecutor

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()

    @staticmethod
    async def clean():
        try:
            if st := Intervals['status']:
                for intvl in list(st.values()):
                    intvl.cancel()
            Intervals['status'].clear()
            await gather(sync_to_async(aria2.purge), delete_status())
        except:
            pass

    def removeFromSameDir(self):
        if self.sameDir and self.mid in self.sameDir['tasks']:
            self.sameDir['tasks'].remove(self.mid)
            self.sameDir['total'] -= 1

    async def onDownloadStart(self):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def onDownloadComplete(self):
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
            # Apply splitting here for vidMode before Telegram upload
            if self.isLeech:
                o_files, m_size = [], []
                LOGGER.info(f"Checking split for {self.name} (size: {size / (1024*1024*1024):.2f} GB) MID: {self.mid}")
                result = await self.proceedSplit(up_dir, m_size, o_files, size, gid)
                if not result:
                    return
                LOGGER.info(f"Proceeding to Telegram upload for MID: {self.mid} with o_files: {o_files}")
                tg = TgUploader(self, up_dir, size)
                async with task_dict_lock:
                    task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')
                try:
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
                return

        if one_path := await self.isOneFile(up_path):
            up_path = one_path

        up_dir, self.name = ospath.split(up_path)
        size = await get_path_size(up_dir)

        if self.isLeech:
            o_files, m_size = [], []
            if not self.compress:
                result = await self.proceedSplit(up_dir, m_size, o_files, size, gid)
                if not result:
                    return
            LOGGER.info(f"Leeching with o_files: {o_files}, m_size: {m_size} for MID: {self.mid}")

            add_to_queue, event = await check_running_tasks(self.mid, "up")
            if add_to_queue:
                LOGGER.info(f"Added to Queue/Upload: {self.name} (MID: {self.mid})")
                async with task_dict_lock:
                    task_dict[self.mid] = QueueStatus(self, size, gid, 'Up')
                try:
                    await wait_for(event.wait(), timeout=300)
                except AsyncTimeoutError:
                    LOGGER.error(f"Queue timeout for MID: {self.mid}")
                    await self.onUploadError("Upload queue timeout.")
                    return
                async with task_dict_lock:
                    if self.mid not in task_dict:
                        return
                LOGGER.info(f"Start from Queued/Upload: {self.name} (MID: {self.mid})")
            async with queue_dict_lock:
                non_queued_up.add(self.mid)

            await start_from_queued()

            size = await get_path_size(up_dir)
            for s in m_size:
                size -= s
            LOGGER.info(f"Leech Name: {self.name} (MID: {self.mid})")
            tg = TgUploader(self, up_dir, size)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')
            try:
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
        elif not self.isLeech and self.isGofile:
            LOGGER.info(f"GoFile Uploading: {self.name} (MID: {self.mid})")
            go = GoFileUploader(self)
            async with task_dict_lock:
                task_dict[self.mid] = GofileUploadStatus(self, go, size, gid)
            await gather(update_status_message(self.message.chat.id), go.goUpload())
            if go.is_cancelled:
                return
        elif is_gdrive_id(self.upDest):
            LOGGER.info(f"GDrive Uploading: {self.name} (MID: {self.mid})")
            drive = gdUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GdriveStatus(self, drive, size, gid, 'up')
            await gather(update_status_message(self.message.chat.id), sync_to_async(drive.upload, size))
        elif self.upDest and ':' in self.upDest:
            LOGGER.info(f"RClone Uploading: {self.name} (MID: {self.mid})")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, 'up')
            await gather(update_status_message(self.message.chat.id), RCTransfer.upload(up_path, size))
        else:
            LOGGER.warning(f"No valid upload destination for MID: {self.mid}, assuming upload complete")
            await self.onUploadComplete(None, size, {}, 0, None)

    async def proceedSplit(self, up_dir, m_size, o_files, size, gid):
        """Split files into parts between 1.95GB and 1.99GB if needed."""
        if not self.isLeech or not await aiopath.isdir(up_dir):
            LOGGER.info(f"No split needed: isLeech={self.isLeech}, is_dir={await aiopath.isdir(up_dir)}")
            return True

        target_min_bytes = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
        target_max_bytes = 1.99 * 1024 * 1024 * 1024  # 1.99 GB
        split_dir = ospath.join(up_dir, "splited_files_mltb")
        await makedirs(split_dir, exist_ok=True)

        for dirpath, _, files in await sync_to_async(walk, up_dir):
            for file_ in files:
                input_file = ospath.join(dirpath, file_)
                if not await aiopath.exists(input_file) or file_.endswith(('.aria2', '.!qB')):
                    continue

                file_size = await get_path_size(input_file)
                LOGGER.info(f"Checking file {input_file}: {file_size / (1024*1024*1024):.2f} GB")
                if file_size <= target_max_bytes:
                    LOGGER.info(f"File {file_} under 1.99GB, no split needed")
                    o_files.append(file_)
                    m_size.append(file_size)
                    continue

                try:
                    duration_cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
                                   '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
                    duration_result = await sync_to_async(subprocess.run, duration_cmd, 
                                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                                         text=True, check=True)
                    duration = float(duration_result.stdout.strip())
                    LOGGER.info(f"Duration of {file_}: {duration:.2f}s")
                except Exception as e:
                    LOGGER.error(f"Failed to get duration for {input_file}: {e}")
                    await self.onUploadError(f"Failed to split {file_}: Unable to determine duration")
                    return False

                num_parts = max(1, math.ceil(file_size / target_max_bytes))
                start_time = 0
                base_name = ospath.splitext(file_)[0]
                LOGGER.info(f"Splitting {file_} into {num_parts} parts")

                for part_num in range(1, num_parts + 1):
                    is_last_part = (part_num == num_parts)
                    part_file = ospath.join(split_dir, f"{base_name}.part{part_num}.mkv")

                    if is_last_part:
                        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-c', 'copy', part_file]
                        LOGGER.info(f"Creating last part {part_num}/{num_parts}: {part_file}")
                    else:
                        low = 0
                        high = duration - start_time
                        bytes_per_second = file_size / duration
                        split_duration = target_min_bytes / bytes_per_second
                        low, high = max(0, split_duration * 0.9), min(high, split_duration * 1.1)
                        best_duration = split_duration
                        iteration = 0
                        max_iterations = 5

                        while iteration < max_iterations and high - low > 5.0:
                            mid = (low + high) / 2
                            temp_file = ospath.join(split_dir, "temp_split.mkv")
                            cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(mid), 
                                   '-c', 'copy', temp_file]
                            try:
                                await sync_to_async(subprocess.run, cmd, capture_output=True, text=True, 
                                                   check=True, timeout=300)
                                temp_size = await get_path_size(temp_file)
                                LOGGER.info(f"Test split: {mid:.2f}s, size: {temp_size / (1024*1024*1024):.2f} GB")
                                await clean_target(temp_file)

                                if temp_size > target_max_bytes:
                                    high = mid
                                elif temp_size < target_min_bytes:
                                    low = mid
                                else:
                                    best_duration = mid
                                    break

                                best_duration = mid
                                iteration += 1
                            except Exception as e:
                                LOGGER.error(f"Error testing split for {input_file}: {e}")
                                if await aiopath.exists(temp_file):
                                    await clean_target(temp_file)
                                await self.onUploadError(f"Split failed for {file_}")
                                return False

                        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(best_duration), 
                               '-c', 'copy', part_file]
                        LOGGER.info(f"Creating part {part_num}/{num_parts}: {part_file} ({best_duration:.2f}s)")
                        start_time += best_duration

                    try:
                        await sync_to_async(subprocess.run, cmd, capture_output=True, text=True, check=True, timeout=300)
                        part_size = await get_path_size(part_file)
                        LOGGER.info(f"Part {part_num} size: {part_size / (1024*1024*1024):.2f} GB")
                        o_files.append(ospath.basename(part_file))
                        m_size.append(part_size)
                    except subprocess.TimeoutExpired:
                        LOGGER.error(f"FFmpeg timed out splitting {input_file}")
                        await self.onUploadError(f"Split timeout for {file_}")
                        return False
                    except subprocess.CalledProcessError as e:
                        LOGGER.error(f"FFmpeg error splitting {input_file}: {e.stderr}")
                        await self.onUploadError(f"Split failed for {file_}: {e.stderr}")
                        return False

                await clean_target(input_file)

        return True

    async def onUploadComplete(self, link, size, files, folders, mime_type, rclonePath='', dir_id=''):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)

        LOGGER.info(f"Task Done: {self.name} (MID: {self.mid})")
        dt_date, dt_time = get_date_time(self.message)
        buttons = ButtonMaker()
        buttons_scr = ButtonMaker()
        daily_size = size
        size_str = get_readable_file_size(size)
        reply_to = self.message.reply_to_message
        images = choice(config_dict['IMAGE_COMPLETE'].split())
        TIME_ZONE_TITLE = config_dict['TIME_ZONE_TITLE']

        thumb_path = ospath.join(self.dir, 'thumb.png')
        if not await aiopath.exists(thumb_path):
            LOGGER.info(f"Thumbnail not found at {thumb_path}, using default")
            thumb_path = None

        msg = f'<a href="https://t.me/satyamisme1"><b><i>Bot By satyamisme</b></i></a>\n'
        msg += f'<code>{escape(self.name)}</code>\n'
        msg += f'<b>┌ Size: </b>{size_str}\n'

        if self.isLeech:
            if config_dict['SOURCE_LINK']:
                scr_link = get_link(self.message)
                if is_magnet(scr_link):
                    tele = TelePost(config_dict['SOURCE_LINK_TITLE'])
                    mag_link = await sync_to_async(tele.create_post, f'<code>{escape(self.name)}<br>({size_str})</code><br>{scr_link}')
                    buttons.button_link('Source Link', mag_link)
                    buttons_scr.button_link('Source Link', mag_link)
                elif is_url(scr_link):
                    buttons.button_link('Source Link', scr_link)
                    buttons_scr.button_link('Source Link', scr_link)
            if self.user_dict.get('enable_pm') and self.isSuperChat:
                buttons.button_link('View File(s)', f'http://t.me/{bot_name}')
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
            uploadmsg = await sendingMessage(msg, self.message, images if not thumb_path else thumb_path, buttons.build_menu(2))
        else:
            msg += f'<b>├ Type: </b>{mime_type or "File"}\n'
            if mime_type == 'Folder':
                if folders:
                    msg += f'<b>├ SubFolders: </b>{folders}\n'
                msg += f'<b>├ Files: </b>{files}\n'
            msg += (f'<b>├ Elapsed: </b>{get_readable_time(time() - self.message.date.timestamp())}\n'
                    f'<b>├ Cc: </b>{self.tag}\n'
                    f'<b>└ Action: </b>{action(self.message)}\n')
            if link:
                buttons.button_link('Cloud Link', link)
            elif rclonePath:
                msg += f'\n\n<b>Path:</b> <code>{rclonePath}</code>'
            uploadmsg = await sendingMessage(msg, self.message, images if not thumb_path else thumb_path, buttons.build_menu(2))

        if self.user_dict.get('enable_pm') and self.isSuperChat:
            await copyMessage(self.user_id, uploadmsg, buttons_scr.build_menu(2))
        if chat_id := config_dict.get('LEECH_LOG') if self.isLeech else config_dict.get('MIRROR_LOG'):
            await copyMessage(chat_id, uploadmsg)

        await clean_download(self.dir)
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
            count = len(task_dict)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
        await start_from_queued()

        if self.isSuperChat and (stime := config_dict['AUTO_DELETE_UPLOAD_MESSAGE_DURATION']):
            bot_loop.create_task(auto_delete_message(self.message, uploadmsg, reply_to, stime=stime))

    async def onDownloadError(self, error, listfile=None):
        LOGGER.error(f"Download error for MID: {self.mid}: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Download failed: {error}", self.message, choice(config_dict['IMAGE_COMPLETE'].split()))
        await gather(start_from_queued(), clean_download(self.dir))

    async def onUploadError(self, error):
        LOGGER.error(f"Upload error for MID: {self.mid}: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Upload failed: {error}", self.message, choice(config_dict['IMAGE_COMPLETE'].split()))
        await gather(start_from_queued(), clean_download(self.dir))