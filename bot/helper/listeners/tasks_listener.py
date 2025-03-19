from aiofiles.os import listdir, path as aiopath, makedirs, remove as aioremove
from aioshutil import move
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError
from html import escape
from os import walk, path as ospath
from random import choice
from requests import utils as rutils
from time import time
import subprocess
import math
import logging

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

# Setup logging
logger = logging.getLogger("TaskListener")

def check_dependencies():
    """Check FFmpeg and ffprobe availability."""
    for cmd in ['ffmpeg', 'ffprobe']:
        try:
            subprocess.run([cmd, '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error(f"{cmd} not found. Install FFmpeg and ensure it's in PATH.")
            return False
    return True

def get_file_size(file_path):
    """Get file size in bytes."""
    try:
        return ospath.getsize(file_path)
    except OSError as e:
        logger.error(f"Cannot access file {file_path}: {e}")
        return 0

def get_video_info(file_path):
    """Get video duration and bitrate."""
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        duration = float(result.stdout.strip())
        bitrate = int((get_file_size(file_path) * 8) / duration) if duration > 0 else 0
        return {'duration': duration, 'bitrate': bitrate}
    except Exception as e:
        logger.error(f"Error getting video info for {file_path}: {e}")
        return None

def find_optimal_split_time(input_file, start_time, target_min_bytes, target_max_bytes, total_duration, max_iterations=5):
    """Find split time for 1.95-1.99 GB parts."""
    low = 0
    high = total_duration - start_time
    bytes_per_second = get_file_size(input_file) / total_duration
    initial_guess = target_min_bytes / bytes_per_second
    low, high = max(0, initial_guess * 0.8), min(high, initial_guess * 1.2)  # ±20% range for spikes

    best_time = initial_guess
    best_size = 0
    iteration = 0

    logger.info(f"Searching split: {start_time:.2f}s, target 1.95-1.99 GB, range {low:.2f}s-{high:.2f}s")
    
    while iteration < max_iterations and high - low > 5.0:
        mid = (low + high) / 2
        temp_file = ospath.join(ospath.dirname(input_file), "temp_split.mkv")
        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(mid), '-c', 'copy', temp_file]
        
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
            size = get_file_size(temp_file)
            aioremove(temp_file)
            logger.info(f"Iteration {iteration + 1}: {mid:.2f}s, {size / (1024*1024*1024):.2f} GB")
            
            if size > target_max_bytes:
                high = mid
            elif size < target_min_bytes:
                low = mid
            else:
                best_time = mid
                best_size = size
                break
            
            best_time = mid
            best_size = size
            iteration += 1
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            logger.error(f"FFmpeg failed: {e}")
            if ospath.exists(temp_file):
                aioremove(temp_file)
            return None, None
    
    logger.info(f"Best split: {best_time:.2f}s, {best_size / (1024*1024*1024):.2f} GB")
    return best_time, best_size

def adjust_part_size(input_file, part_file, start_time, current_size, target_min_bytes, target_max_bytes, total_duration):
    """Adjust part to ≤ 1.99 GB if needed."""
    if current_size <= target_max_bytes:
        return True
    
    remaining_duration = total_duration - start_time
    shrink_factor = target_max_bytes / current_size
    new_duration = remaining_duration * shrink_factor * 0.95
    cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(new_duration), '-c', 'copy', part_file]
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        new_size = get_file_size(part_file)
        logger.info(f"Shrunk to {new_size / (1024*1024*1024):.2f} GB")
        return new_size <= target_max_bytes
    except subprocess.CalledProcessError as e:
        logger.error(f"Adjust failed: {e}")
        return False

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        if not check_dependencies():
            raise Exception("FFmpeg/ffprobe missing. TaskListener cannot proceed.")

    @staticmethod
    async def clean():
        try:
            if st := Intervals['status']:
                for intvl in list(st.values()):
                    intvl.cancel()
            Intervals['status'].clear()
            await gather(sync_to_async(aria2.purge), delete_status())
        except Exception as e:
            LOGGER.error(f"Error during cleanup: {e}")

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
                self.name = files[-1] if files[-1] != 'yt-dlp-thumb' else files[0]
                up_path = ospath.join(self.dir, self.name)
            except Exception as e:
                await self.onUploadError(f"Cannot find file: {e}")
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
            LOGGER.info(f"Processing video with VidEcxecutor: {self.name}")
            up_path = await VidEcxecutor(self, up_path, gid).execute()
            if not up_path:
                return
            self.seed = False
            up_dir, self.name = ospath.split(up_path)
            size = await get_path_size(up_dir)

        if one_path := await self.isOneFile(up_path):
            up_path = one_path

        up_dir, self.name = ospath.split(up_path)
        size = await get_path_size(up_path if await aiopath.isfile(up_path) else up_dir)

        if self.isLeech:
            o_files, m_size = [], []
            LOGGER.info(f"Checking split for {self.name}, size: {size / (1024*1024*1024):.2f} GB")
            split_dir = self.dir  # Use MID directory directly
            result = await self.proceedSplit(up_dir, m_size, o_files, size, gid)
            if not result:
                return

            # Validate all parts before upload
            telegram_limit = 2_097_152_000  # 2000 MiB
            target_min_bytes = 1_990_654_771  # 1.95 GB
            target_max_bytes = 2_028_896_563  # 1.99 GB
            for i, (file_, size_) in enumerate(zip(o_files, m_size)):
                full_path = ospath.join(split_dir, file_)
                if size_ > telegram_limit or (i < len(o_files) - 1 and (size_ < target_min_bytes or size_ > target_max_bytes)):
                    LOGGER.error(f"Part {file_} size {size_ / (1024*1024*1024):.2f} GB invalid.")
                    await self.onUploadError("Part size validation failed.")
                    return
                if i == len(o_files) - 1 and size_ > target_max_bytes:
                    LOGGER.error(f"Final part {file_} size {size_ / (1024*1024*1024):.2f} GB exceeds 1.99 GB.")
                    await self.onUploadError("Final part size validation failed.")
                    return

            add_to_queue, event = await check_running_tasks(self.mid, "up")
            if add_to_queue:
                LOGGER.info(f"Added to upload queue: {self.name}")
                async with task_dict_lock:
                    task_dict[self.mid] = QueueStatus(self, size, gid, 'Up')
                try:
                    await wait_for(event.wait(), timeout=300)
                except AsyncTimeoutError:
                    await self.onUploadError("Upload queue timeout.")
                    return
                async with task_dict_lock:
                    if self.mid not in task_dict:
                        return
                LOGGER.info(f"Starting from queue: {self.name}")

            async with queue_dict_lock:
                non_queued_up.add(self.mid)

            await start_from_queued()

            total_size = size
            uploaded_size = 0
            LOGGER.info(f"Leeching: {self.name} with total size: {total_size / (1024*1024*1024):.2f} GB")

            upload_path = split_dir
            tg = TgUploader(self, upload_path, size)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')
            try:
                await wait_for(gather(update_status_message(self.message.chat.id), tg.upload(o_files, m_size)), timeout=600)
                LOGGER.info(f"Leech completed: {self.name}")
            except AsyncTimeoutError:
                await self.onUploadError("Upload timed out after 10 minutes.")
                return
            except Exception as e:
                await self.onUploadError(f"Upload failed: {str(e)}")
                return

            # Cleanup after successful upload
            for file_ in o_files:
                await aioremove(ospath.join(split_dir, file_))
            await aioremove(up_path)  # Delete original file after all parts uploaded
        elif not self.isLeech and self.isGofile:
            LOGGER.info(f"GoFile upload: {self.name}")
            go = GoFileUploader(self)
            async with task_dict_lock:
                task_dict[self.mid] = GofileUploadStatus(self, go, size, gid)
            await gather(update_status_message(self.message.chat.id), go.goUpload())
            if go.is_cancelled:
                return
        elif is_gdrive_id(self.upDest):
            LOGGER.info(f"GDrive upload: {self.name}")
            drive = gdUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GdriveStatus(self, drive, size, gid, 'up')
            await gather(update_status_message(self.message.chat.id), sync_to_async(drive.upload, size))
        elif self.upDest and ':' in self.upDest:
            LOGGER.info(f"RClone upload: {self.name}")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, 'up')
            await gather(update_status_message(self.message.chat.id), RCTransfer.upload(up_path, size))
        else:
            LOGGER.warning(f"No valid upload destination for {self.name}")
            await self.onUploadComplete(None, size, {}, 0, None)

        await clean_download(self.dir)
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
        await start_from_queued()

    async def proceedSplit(self, up_dir, m_size, o_files, size, gid):
        if not self.isLeech or not await aiopath.isfile(up_dir):
            LOGGER.debug(f"No split needed: isLeech={self.isLeech}, is_file={await aiopath.isfile(up_dir)}")
            o_files.append(self.name)
            m_size.append(size)
            return True

        input_file = ospath.join(up_dir, self.name)
        file_size = await get_path_size(input_file)
        telegram_limit = 2_097_152_000  # 2000 MiB
        target_min_bytes = 1_990_654_771  # 1.95 GB
        target_max_bytes = 2_028_896_563  # 1.99 GB

        if file_size <= target_max_bytes:
            o_files.append(self.name)
            m_size.append(file_size)
            return True

        video_info = get_video_info(input_file)
        if not video_info:
            await self.onUploadError("Failed to get video info.")
            return False

        num_parts = math.ceil(file_size / target_max_bytes)
        full_parts = num_parts - 1
        start_time = 0
        parts = []

        logger.info(f"Splitting {self.name}: {file_size / (1024*1024*1024):.2f} GB into {num_parts} parts")

        for i in range(num_parts):
            part_num = i + 1
            is_last_part = (i == num_parts - 1)
            part_file = ospath.join(self.dir, f"{ospath.splitext(self.name)[0]}.part{part_num}.mkv")
            
            if not is_last_part:
                max_iter = 7 if file_size > 10_737_418_240 else 5  # 7 for > 10 GB
                for attempt in range(2):  # 2 retries
                    split_duration, split_size = find_optimal_split_time(input_file, start_time, target_min_bytes, target_max_bytes, video_info['duration'], max_iter)
                    if split_duration is None:
                        if attempt == 1:
                            await self.onUploadError("FFmpeg failed after retries.")
                            return False
                        continue
                    cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(split_duration), '-c', 'copy', part_file]
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                    part_size = get_file_size(part_file)
                    if not adjust_part_size(input_file, part_file, start_time, part_size, target_min_bytes, target_max_bytes, video_info['duration']):
                        if attempt == 1:
                            await self.onUploadError("Failed to adjust part size.")
                            return False
                        aioremove(part_file)
                        continue
                    if target_min_bytes <= part_size <= target_max_bytes:
                        parts.append(part_file)
                        o_files.append(ospath.basename(part_file))
                        m_size.append(part_size)
                        start_time += split_duration
                        break
            else:
                cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-c', 'copy', part_file]
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                part_size = get_file_size(part_file)
                if part_size > target_max_bytes:
                    adjust_part_size(input_file, part_file, start_time, part_size, target_min_bytes, target_max_bytes, video_info['duration'])
                parts.append(part_file)
                o_files.append(ospath.basename(part_file))
                m_size.append(part_size)

        return True

    async def onUploadComplete(self, link, size, files, folders, mime_type, rclonePath='', dir_id=''):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)

        LOGGER.info(f"Task completed: {self.name} (MID: {self.mid})")
        dt_date, dt_time = get_date_time(self.message)
        buttons = ButtonMaker()
        buttons_scr = ButtonMaker()
        size_str = get_readable_file_size(size)
        reply_to = self.message.reply_to_message
        images = choice(config_dict['IMAGE_COMPLETE'].split())

        thumb_path = ospath.join(self.dir, 'thumb.png')
        if not await aiopath.exists(thumb_path):
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
        LOGGER.error(f"Download error: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Download failed: {error}", self.message, choice(config_dict['IMAGE_COMPLETE'].split()))
        await gather(start_from_queued(), clean_download(self.dir))

    async def onUploadError(self, error):
        LOGGER.error(f"Upload error: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Upload failed: {error}", self.message, choice(config_dict['IMAGE_COMPLETE'].split()))
        await gather(start_from_queued(), clean_download(self.dir))

async def start_listener(mid):
    """Start TaskListener for a given MID."""
    listener = TaskListener()
    listener.mid = mid
    listener.dir = f"/usr/src/app/downloads/{mid}"
    listener.message = await bot.get_messages(chat_id=config_dict['CMD_CHAT'], message_ids=int(mid))  # Adjust as per your bot setup
    listener.user_id = listener.message.from_user.id
    listener.isSuperChat = True  # Adjust based on your logic
    listener.tag = "@user"  # Placeholder, adjust as needed
    listener.isLeech = True  # Assuming leech for splitting
    bot_loop.create_task(listener.onDownloadComplete())

if __name__ == "__main__":
    bot_loop.run_until_complete(start_listener("1638"))