from aiofiles.os import listdir, path as aiopath, makedirs, remove as aioremove
from aioshutil import move
from asyncio import sleep, gather, wait_for, TimeoutError as AsyncTimeoutError, Event
from html import escape
from os import walk, path as ospath
import subprocess
import math
import logging

from bot import bot_loop, bot_name, task_dict, task_dict_lock, Intervals, aria2, config_dict, non_queued_up, non_queued_dl, queued_up, queued_dl, queue_dict_lock, LOGGER, DATABASE_URL, bot
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_download, clean_target, join_files
from bot.helper.ext_utils.links_utils import is_magnet, is_url, get_link, is_gdrive_link, is_gdrive_id
from bot.helper.ext_utils.status_utils import get_date_time, get_readable_file_size
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
from bot.helper.telegram_helper.message_utils import sendingMessage, update_status_message, delete_status
from bot.helper.video_utils.executor import VidEcxecutor
from bot.helper.ext_utils.task_manager import check_running_tasks, ffmpeg_queue, ffmpeg_queue_lock, active_ffmpeg, start_from_queued

logger = logging.getLogger("TaskListener")

def check_dependencies():
    for cmd in ['ffmpeg', 'ffprobe']:
        try:
            subprocess.run([cmd, '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error(f"{cmd} not found. Install FFmpeg and ensure it's in PATH.")
            return False
    return True

def get_file_size(file_path):
    try:
        return ospath.getsize(file_path)
    except OSError as e:
        logger.error(f"Cannot access file {file_path}: {e}")
        return 0

def get_video_info(file_path):
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        duration = float(result.stdout.strip())
        bitrate = int((get_file_size(file_path) * 8) / duration) if duration > 0 else 0
        return {'duration': duration, 'bitrate': bitrate}
    except Exception as e:
        logger.error(f"Error getting video info for {file_path}: {e}")
        return None

def smart_guess_split(input_file, start_time, target_min, target_max, total_duration, max_iterations=5):
    bytes_per_second = get_file_size(input_file) / total_duration
    guess = target_max / bytes_per_second
    low, high = guess * 0.95, min(total_duration - start_time, guess * 1.05)
    best_time, best_size = guess, 0
    for i in range(max_iterations):
        mid = (low + high) / 2
        temp_file = ospath.join(ospath.dirname(input_file), f"smart_temp_{i}.mkv")
        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(mid), '-c', 'copy', temp_file]
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
            size = get_file_size(temp_file)
            aioremove(temp_file)
            logger.info(f"Smart Iter {i+1}: {mid:.2f}s, {size / (1024*1024*1024):.2f} GB")
            if 1_931_069_952 <= size <= 2_028_896_563:
                return mid, size
            elif size > 2_028_896_563:
                high = mid
            elif size < 1_931_069_952:
                low = mid
            best_time, best_size = mid, size
            if high - low < 2.0:
                break
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            logger.error(f"Smart guess failed: {e}")
            if ospath.exists(temp_file):
                aioremove(temp_file)
            return None, None
    return best_time if 1_931_069_952 <= best_size <= 2_028_896_563 else None, best_size

class TaskListener(TaskConfig):
    def __init__(self, mid, message, dir, user_id, tag, user_dict=None):
        super().__init__()
        if not check_dependencies():
            raise Exception("FFmpeg/ffprobe missing. TaskListener cannot proceed.")
        self.mid = mid
        self.message = message
        self.dir = dir
        self.user_id = user_id
        self.tag = tag
        self.user_dict = user_dict or {}
        self.name = ""
        self.isSuperChat = False
        self.sameDir = None
        self.isLeech = False
        self.upDest = ""
        self.join = False
        self.extract = False
        self.sampleVideo = False
        self.compress = False
        self.vidMode = None
        self.isGofile = False
        self.seed = True
        LOGGER.info(f"TaskListener initialized for MID: {self.mid}")

    async def clean(self):
        try:
            if st := Intervals.get('status', {}):
                for intvl in list(st.values()):
                    intvl.cancel()
                Intervals['status'].clear()
            await gather(sync_to_async(aria2.purge), delete_status())
            LOGGER.info("Cleanup completed")
        except Exception as e:
            LOGGER.error(f"Error during cleanup: {e}")

    def removeFromSameDir(self):
        if self.sameDir and self.mid in self.sameDir.get('tasks', []):
            self.sameDir['tasks'].remove(self.mid)
            self.sameDir['total'] -= 1
            LOGGER.info(f"Removed MID {self.mid} from sameDir")

    async def isOneFile(self, path):
        if await aiopath.isfile(path):
            return path
        return None

    async def reName(self):
        # Placeholder for renaming logic
        pass

    async def onDownloadStart(self):
        LOGGER.info(f"Download started for MID: {self.mid}")
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def onDownloadComplete(self):
        global active_ffmpeg
        LOGGER.info(f"onDownloadComplete called for MID: {self.mid}")
        multi_links = False
        if self.sameDir and self.mid in self.sameDir.get('tasks', []):
            LOGGER.info(f"Waiting for sameDir tasks for MID: {self.mid}, total: {self.sameDir['total']}")
            while not (self.sameDir['total'] in [1, 0] or (self.sameDir['total'] > 1 and len(self.sameDir['tasks']) > 1)):
                await sleep(0.5)

        async with task_dict_lock:
            if self.mid not in task_dict:
                LOGGER.error(f"Task {self.mid} not found in task_dict")
                return
            if self.sameDir and self.sameDir.get('total', 0) > 1 and self.mid in self.sameDir.get('tasks', []):
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

        if multi_links:
            await self.onUploadError('Downloaded! Waiting for other tasks.')
            return

        up_path = ospath.join(self.dir, self.name)
        if not await aiopath.exists(up_path):
            try:
                files = await listdir(self.dir)
                self.name = next((f for f in files if f != 'yt-dlp-thumb'), files[0])
                up_path = ospath.join(self.dir, self.name)
            except Exception as e:
                await self.onUploadError(f"Cannot find file: {e}")
                return

        await self.reName()
        size = await get_path_size(up_path)

        if not config_dict.get('QUEUE_ALL') and not config_dict.get('QUEUE_COMPLETE'):
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
                    LOGGER.info(f"Removed MID {self.mid} from non_queued_dl")
            await start_from_queued()

        if self.join and await aiopath.isdir(up_path):
            await join_files(up_path)

        o_files, m_size = [], []
        ffmpeg_needed = False
        task_type = None
        if self.extract:
            ffmpeg_needed = True
            task_type = 'extract'
            up_path = await self.proceedExtract(up_path, size, gid)
            if not up_path:
                return
        elif self.sampleVideo:
            ffmpeg_needed = True
            task_type = 'sample'
            up_path = await self.generateSampleVideo(up_path, gid)
            if not up_path:
                return
        elif self.compress:
            ffmpeg_needed = True
            task_type = 'compress'
            if self.vidMode:
                up_path = await VidEcxecutor(self, up_path, gid).execute()
                if not up_path:
                    return
                self.seed = False
            up_path = await self.proceedCompress(up_path, size, gid)
            if not up_path:
                return
        elif not self.compress and self.vidMode:
            ffmpeg_needed = True
            task_type = self.vidMode[0]
            up_path = await VidEcxecutor(self, up_path, gid).execute()
            if not up_path:
                return
            self.seed = False
        elif self.isLeech and size > 1_931_069_952:
            ffmpeg_needed = True
            task_type = 'split'
            split_dir = self.dir
            result = await self.proceedSplit(ospath.dirname(up_path) if await aiopath.isfile(up_path) else up_path, m_size, o_files, size, gid)
            if not result:
                return
            up_path = split_dir

        up_dir, self.name = ospath.split(up_path)
        size = await get_path_size(up_path if await aiopath.isfile(up_path) else up_dir)

        if ffmpeg_needed and task_type not in ['extract', 'sample', 'compress', 'split'] and not self.vidMode:
            event = Event()
            async with ffmpeg_queue_lock:
                ffmpeg_queue[self.mid] = (event, task_type, up_path)
                LOGGER.info(f"Queued FFmpeg for MID: {self.mid}, type: {task_type}")
            await event.wait()
            if active_ffmpeg != self.mid:
                LOGGER.info(f"FFmpeg not active for MID: {self.mid}, cleaning up")
                await self.clean()
                return
            active_ffmpeg = None

        add_to_queue, event = await check_running_tasks(self.mid, "up")
        if add_to_queue:
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, size, gid, 'Up')
            await event.wait()
            async with task_dict_lock:
                if self.mid not in task_dict:
                    LOGGER.info(f"MID {self.mid} removed from task_dict during queue wait")
                    return

        async with queue_dict_lock:
            non_queued_up.add(self.mid)

        if self.isLeech:
            upload_path = split_dir if task_type == 'split' else up_path
            tg = TgUploader(self, upload_path, size)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, size, gid, 'up')
            try:
                files_dict = await wait_for(tg.upload(o_files, m_size) if task_type == 'split' else tg.upload([ospath.basename(up_path)], [size]), timeout=3600)
                if files_dict:
                    await self.onUploadComplete(None, size, files_dict, len(o_files) if task_type == 'split' else 1, 0)
            except AsyncTimeoutError:
                await self.onUploadError("Upload timed out after 10 minutes.")
                return
            except Exception as e:
                await self.onUploadError(f"Upload failed: {str(e)}")
                return
        elif self.isGofile:
            go = GoFileUploader(self)
            async with task_dict_lock:
                task_dict[self.mid] = GofileUploadStatus(self, go, size, gid)
            await gather(update_status_message(self.message.chat.id), go.goUpload())
            if go.is_cancelled:
                return
        elif is_gdrive_id(self.upDest):
            drive = gdUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GdriveStatus(self, drive, size, gid, 'up')
            await gather(update_status_message(self.message.chat.id), sync_to_async(drive.upload, size))
        elif self.upDest and ':' in self.upDest:
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, 'up')
            await gather(update_status_message(self.message.chat.id), RCTransfer.upload(up_path, size))
        else:
            await self.onUploadComplete(None, size, {}, 0, None)

        await clean_download(self.dir)
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        async with queue_dict_lock:
            non_queued_up.discard(self.mid)
            non_queued_dl.discard(self.mid)
        await start_from_queued()

    async def proceedSplit(self, up_dir, m_size, o_files, size, gid):
        if not self.isLeech or not await aiopath.isdir(up_dir):
            return True

        target_min_bytes = 1_931_069_952
        target_max_bytes = 2_028_896_563
        telegram_limit = 2_097_152_000

        for dirpath, _, files in await sync_to_async(walk, up_dir):
            for file_ in files:
                input_file = ospath.join(dirpath, file_)
                if not await aiopath.exists(input_file) or file_.endswith(('.aria2', '.!qB')):
                    continue

                file_size = get_file_size(input_file)
                if file_size <= target_max_bytes:
                    o_files.append(ospath.basename(input_file))
                    m_size.append(file_size)
                    continue

                video_info = get_video_info(input_file)
                if not video_info:
                    await self.onUploadError("Failed to get video info.")
                    return False

                num_parts = math.ceil(file_size / target_max_bytes)
                start_time = 0
                parts = []
                base_name = ospath.splitext(file_)[0]

                for i in range(num_parts):
                    part_num = i + 1
                    is_last_part = (i == num_parts - 1)
                    part_file = ospath.join(self.dir, f"{base_name}.part{part_num}.mkv")

                    if not is_last_part:
                        split_duration, split_size = smart_guess_split(input_file, start_time, target_min_bytes, target_max_bytes, video_info['duration'])
                        if split_duration is None:
                            await self.onUploadError(f"Failed to split part {part_num}.")
                            return False
                        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-t', str(split_duration), '-c', 'copy', part_file]
                        try:
                            subprocess.run(cmd, capture_output=True, text=True, check=True)
                            part_size = get_file_size(part_file)
                            if not (target_min_bytes <= part_size <= target_max_bytes):
                                await self.onUploadError(f"Part {part_num} size out of range.")
                                return False
                            parts.append(part_file)
                            start_time += split_duration
                        except subprocess.CalledProcessError as e:
                            logger.error(f"Split error: {e}")
                            return False
                    else:
                        cmd = ['ffmpeg', '-y', '-i', input_file, '-ss', str(start_time), '-c', 'copy', part_file]
                        subprocess.run(cmd, capture_output=True, text=True, check=True)
                        part_size = get_file_size(part_file)
                        if part_size > telegram_limit:
                            await self.onUploadError(f"Last part exceeds Telegram limit.")
                            return False
                        parts.append(part_file)

                for part in parts:
                    o_files.append(ospath.basename(part))
                    m_size.append(get_file_size(part))
                await aioremove(input_file)

        return True

    async def onUploadComplete(self, link, size, files, folders, mime_type, rclonePath='', dir_id=''):
        if self.isSuperChat and config_dict['INCOMPLETE_TASK_NOTIFIER'] and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)

        LOGGER.info(f"Task completed: {self.name} (MID: {self.mid})")
        buttons = ButtonMaker()
        size_str = get_readable_file_size(size)

        msg = f'<b>Task Completed</b>\n<code>{escape(self.name)}</code>\n<b>Size: </b>{size_str}\n'
        if self.isLeech and files:
            for tlink, name in files.items():
                buttons.button_link(name, tlink)
        elif link:
            buttons.button_link('Cloud Link', link)

        await sendingMessage(msg, self.message, None, buttons.build_menu(2))
        await clean_download(self.dir)
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        async with queue_dict_lock:
            non_queued_up.discard(self.mid)
            non_queued_dl.discard(self.mid)
        await start_from_queued()

    async def onDownloadError(self, error):
        LOGGER.error(f"Download error: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Download failed: {error}", self.message, None)
        await gather(start_from_queued(), clean_download(self.dir))

    async def onUploadError(self, error):
        LOGGER.error(f"Upload error: {error}")
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
        await self.clean()
        if self.isSuperChat and DATABASE_URL:
            await DbManager().rm_complete_task(self.message.link)
        await sendingMessage(f"Upload failed: {error}", self.message, None)
        await gather(start_from_queued(), clean_download(self.dir))

    async def proceedExtract(self, up_path, size, gid):
        LOGGER.info(f"Extracting {up_path}")
        return up_path

    async def generateSampleVideo(self, up_path, gid):
        LOGGER.info(f"Generating sample for {up_path}")
        sample_path = ospath.join(self.dir, f"sample_{ospath.basename(up_path)}")
        cmd = ['ffmpeg', '-i', up_path, '-t', '60', '-c', 'copy', sample_path, '-y']
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            return sample_path
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Sample generation failed: {e}")
            return None

    async def proceedCompress(self, up_path, size, gid):
        LOGGER.info(f"Compressing {up_path}")
        compressed_path = ospath.join(self.dir, f"compressed_{ospath.basename(up_path)}")
        cmd = ['ffmpeg', '-i', up_path, '-vf', 'scale=1280:720', '-crf', '28', compressed_path, '-y']
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            return compressed_path
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Compression failed: {e}")
            return None