from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs, listdir
from aioshutil import rmtree, move
from ast import literal_eval
from asyncio import create_subprocess_exec, gather, Event, Semaphore, wait_for, TimeoutError, sleep
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk
from time import time

# Bot and task management imports from tasks_listener.py
from bot import bot, task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME, config_dict, queue_dict_lock, non_queued_up, non_queued_dl, queued_up, queued_dl, aria2, Intervals, DATABASE_URL
from bot.helper.common import TaskConfig
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.files_utils import get_path_size, clean_target, clean_download, get_url_name
from bot.helper.ext_utils.links_utils import get_document_type, get_media_info
from bot.helper.ext_utils.media_utils import FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage, update_status_message, delete_status

UPLOAD_SEMAPHORE = Semaphore(3)

async def get_metavideo(video_file):
    try:
        stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_streams', '-show_format', video_file])
        if rcode != 0:
            LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
            return [], {}
        metadata = literal_eval(stdout)
        return metadata.get('streams', []), metadata.get('format', {})
    except Exception as e:
        LOGGER.error(f"Error in get_metavideo: {e}")
        return [], {}

class VidEcxecutor(FFProgress, TaskConfig):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        FFProgress.__init__(self)
        TaskConfig.__init__(self)
        self.data = {}
        self.event = Event()
        self.listener = listener
        self.path = path
        self.name = ''
        self.outfile = ''
        self.size = 0
        self._metadata = metadata
        self._up_path = path
        self._gid = gid
        self._files = []
        self.is_cancelled = False
        self._processed_bytes = 0
        self._last_uploaded = 0
        self._start_time = time()
        self.mid = listener.mid
        self._qual = {'1080p': '1920', '720p': '1280', '540p': '960', '480p': '854', '360p': '640'}  # For _vid_convert
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {self.path}")

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

    async def _cleanup(self):
        try:
            for f in self._files:
                if await aiopath.exists(f):
                    await clean_target(f)
            self._files.clear()
            input_file = ospath.join(self.path, f'input_{self._gid}.txt')
            if await aiopath.exists(input_file):
                await clean_target(input_file)
            self.data.clear()
            self.is_cancelled = True
            LOGGER.info(f"Cleanup completed for MID: {self.listener.mid}")
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

    async def _get_files(self):
        file_list = []
        if self._metadata:
            file_list.append(self.path)
        elif await aiopath.isfile(self.path):
            if self.path.lower().endswith('.zip'):
                extract_dir = ospath.join(ospath.dirname(self.path), f"extracted_{self._gid}")
                await makedirs(extract_dir, exist_ok=True)
                cmd = ['7z', 'x', self.path, f'-o{extract_dir}', '-y']
                _, stderr, rcode = await cmd_exec(cmd)
                if rcode != 0:
                    LOGGER.error(f"Failed to extract ZIP: {stderr}")
                    await rmtree(extract_dir, ignore_errors=True)
                    return None
                self._files.append(extract_dir)
                for dirpath, _, files in await sync_to_async(walk, extract_dir):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
            elif (await get_document_type(self.path))[0]:
                file_list.append(self.path)
        else:
            for dirpath, _, files in await sync_to_async(walk, self.path):
                for file in natsorted(files):
                    file_path = ospath.join(dirpath, file)
                    if (await get_document_type(file_path))[0]:
                        file_list.append(file_path)
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
        return file_list

    async def _upload_progress(self, current, _):
        if self.is_cancelled:
            bot.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _upload_file(self, file_path):
        async with UPLOAD_SEMAPHORE:
            try:
                LOGGER.info(f"Uploading file for MID: {self.listener.mid}: {file_path}")
                caption = f"<code>{ospath.basename(file_path)}</code>"
                self._send_msg = await bot.send_document(
                    chat_id=self.listener.message.chat.id,
                    document=file_path,
                    caption=caption,
                    disable_notification=True,
                    progress=self._upload_progress,
                    reply_to_message_id=self.listener.message.id
                )
                if not self._send_msg or not hasattr(self._send_msg, 'link'):
                    raise ValueError("Upload failed: No valid message returned")
                LOGGER.info(f"Upload completed for MID: {self.listener.mid}: {file_path}")
                return self._send_msg.link
            except TimeoutError:
                LOGGER.error(f"Upload timed out for MID: {self.listener.mid}: {file_path}")
                self.is_cancelled = True
                await self.listener.onUploadError("Upload timed out after 10 minutes")
                return None
            except Exception as e:
                LOGGER.error(f"Upload error for MID: {self.listener.mid}: {e}")
                self.is_cancelled = True
                await self.listener.onUploadError(f"Upload failed: {e}")
                return None

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            await self.listener.onUploadError("Invalid video mode configuration.")
            return None

        LOGGER.info(f"Executing {self.mode} for MID: {self.listener.mid}")
        file_list = await self._get_files()
        if not file_list:
            await sendMessage("No valid video files found.", self.listener.message)
            await self._cleanup()
            await self.listener.onUploadError("No files to process or upload.")
            return None

        # Queue management
        add_to_queue, event = False, None
        if config_dict.get('QUEUE_ALL') or config_dict.get('QUEUE_COMPLETE'):
            async with queue_dict_lock:
                if self.mid not in non_queued_up:
                    add_to_queue = True
                    event = Event()
                    queued_up[self.mid] = event
                    LOGGER.info(f"Added to Queue/Upload: {self.name}")
                    async with task_dict_lock:
                        task_dict[self.mid] = QueueStatus(self, self.size, self._gid, 'Up')
            if add_to_queue:
                await event.wait()
                async with task_dict_lock:
                    if self.mid not in task_dict:
                        return None
                LOGGER.info(f"Starting from Queued/Upload: {self.name}")

        async with queue_dict_lock:
            non_queued_up.add(self.mid)

        try:
            if self.mode == 'merge_rmaudio':
                result = await self._merge_and_rmaudio(file_list)
            elif self.mode == 'vid_vid':
                result = await self._merge_vids()
            elif self.mode == 'vid_aud':
                result = await self._merge_auds()
            elif self.mode == 'vid_sub':
                result = await self._merge_subs(**kwargs)
            elif self.mode == 'trim':
                result = await self._vid_trimmer(**kwargs)
            elif self.mode == 'watermark':
                result = await self._vid_marker(**kwargs)
            elif self.mode == 'compress':
                result = await self._vid_compress(**kwargs)
            elif self.mode == 'subsync':
                result = await self._subsync(**kwargs)
            elif self.mode == 'rmstream':
                result = await self._rm_stream()
            elif self.mode == 'extract':
                result = await self._vid_extract()
            else:
                result = await self._vid_convert()

            if result and not self.is_cancelled:
                uploaded_link = await self._upload_file(result)
                if uploaded_link:
                    await self.listener.onUploadComplete(uploaded_link, self.size, {uploaded_link: self.name}, 1, 0)
                else:
                    await self._cleanup()
                    await self.listener.onUploadError("Upload failed after processing.")
                    return None
            else:
                await self._cleanup()
                await self.listener.onUploadError(f"{self.mode} processing failed.")
                return None
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            await self.listener.onUploadError(f"Failed to process {self.mode}.")
            return None
        finally:
            await self._finalize_task()

    async def _finalize_task(self):
        async with task_dict_lock:
            task_dict.pop(self.mid, None)
            count = len(task_dict)
        LOGGER.info(f"Task {self.mid} removed from task_dict")
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.listener.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]

        await clean_download(self.path)
        if config_dict.get('INCOMPLETE_TASK_NOTIFIER') and DATABASE_URL:
            await DbManager().rm_complete_task(self.listener.message.link)

    # UI methods (kept intact)
    @new_task
    async def _start_handler(self, *args):
        from bot.helper.video_utils.extra_selector import ExtraSelect
        selector = ExtraSelect(self)
        await selector.get_buttons(*args)

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update: {status} for MID: {self.listener.mid}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    # Supporting methods
    async def _final_path(self, outfile=''):
        try:
            if self._metadata:
                self._up_path = outfile or self.outfile
            else:
                scan_dir = self._up_path if self._is_dir else ospath.split(self._up_path)[0]
                for dirpath, _, files in await sync_to_async(walk, scan_dir):
                    for file in files:
                        if file != ospath.basename(outfile or self.outfile):
                            await clean_target(ospath.join(dirpath, file))
                all_files = [(dirpath, file) for dirpath, _, files in await sync_to_async(walk, scan_dir) for file in files]
                if len(all_files) == 1:
                    self._up_path = ospath.join(*all_files[0])
            self._files.clear()
            LOGGER.info(f"Final path for MID: {self.listener.mid}: {self._up_path}")
            return self._up_path
        except Exception as e:
            LOGGER.error(f"Final path error: {e}")
            await self._cleanup()
            return None

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        base_dir, file_name = ospath.split(path)
        if not self.name or multi:
            if info:
                if await aiopath.isfile(path):
                    file_name = file_name.rsplit('.', 1)[0]
                file_name += f'_{info}.mkv'
            self.name = file_name
        if not self.name.upper().endswith(('MKV', 'MP4')):
            self.name += '.mkv'
        return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg cmd for MID: {self.listener.mid}: {' '.join(cmd)}")
            process = await create_subprocess_exec(*cmd, stderr=PIPE)
            self.listener.suproc = process
            _, code = await gather(self.progress(status), process.wait())
            if code == 0:
                LOGGER.info(f"FFmpeg succeeded for MID: {self.listener.mid}")
                if not self.listener.seed:
                    await gather(*[clean_target(file) for file in self._files])
                self._files.clear()
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info(f"FFmpeg cancelled for MID: {self.listener.mid}")
            else:
                error_msg = (await process.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error for MID: {self.listener.mid}: {error_msg}")
                self.is_cancelled = True
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}", exc_info=True)
            self.is_cancelled = True
            return False

    # Your FFmpeg method (kept intact)
    async def _merge_and_rmaudio(self, file_list):
        streams = await get_metavideo(file_list[0])[0]  # Adjusted to match signature
        if not streams:
            LOGGER.error(f"No streams found in {file_list[0]} for MID: {self.listener.mid}")
            await sendMessage("No streams found in the video file.", self.listener.message)
            return None

        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', multi=len(file_list) > 1)
        self._files = file_list
        self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0

        await self._start_handler(streams)
        try:
            await wait_for(self.event.wait(), timeout=180)
        except TimeoutError:
            LOGGER.error(f"Stream selection timed out for MID: {self.listener.mid}")
            self.is_cancelled = True
            await self._cleanup()
            await self.listener.onUploadError("Stream selection timed out after 180 seconds.")
            return None

        if self.is_cancelled:
            LOGGER.info(f"Cancelled before processing for MID: {self.listener.mid}")
            return None

        streams_to_remove = self.data.get('streams_to_remove', [])
        self.outfile = ospath.join(base_dir, self.name)
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')

        try:
            if len(file_list) > 1:
                async with aiopen(input_file, 'w') as f:
                    await f.write('\n'.join([f"file '{f}'" for f in file_list]))
                cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file]
            else:
                cmd = [FFMPEG_NAME, '-i', file_list[0]]

            cmd.extend(['-map', '0:v'])
            kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
            for stream in kept_streams:
                cmd.extend(['-map', stream])
            cmd.extend(['-c', 'copy', self.outfile, '-y'])

            if not await self._run_cmd(cmd, 'direct'):
                await sendMessage("Merging failed due to FFmpeg error.", self.listener.message)
                return None
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error in _merge_and_rmaudio: {e}", exc_info=True)
            await sendMessage("Processing failed.", self.listener.message)
            return None
        finally:
            if len(file_list) > 1:
                await clean_target(input_file)

    # Additional FFmpeg methods from provided executor.py
    async def _merge_vids(self):
        list_files = []
        for dirpath, _, files in await sync_to_async(walk, self.path):
            if len(files) == 1:
                return self._up_path
            for file in natsorted(files):
                video_file = ospath.join(dirpath, file)
                if (await get_document_type(video_file))[0]:
                    self.size += await get_path_size(video_file)
                    list_files.append(f"file '{video_file}'")
                    self._files.append(video_file)

        self.outfile = self._up_path
        if len(list_files) > 1:
            await self._name_base_dir(self.path)
            await update_status_message(self.listener.message.chat.id)
            input_file = ospath.join(self.path, 'input.txt')
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join(list_files))

            self.outfile = ospath.join(self.path, self.name)
            cmd = [FFMPEG_NAME, '-ignore_unknown', '-f', 'concat', '-safe', '0', '-i', input_file, '-map', '0', '-c', 'copy', self.outfile, '-y']
            await self._run_cmd(cmd, 'direct')
            await clean_target(input_file)
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _merge_auds(self):
        main_video = False
        for dirpath, _, files in await sync_to_async(walk, self.path):
            if len(files) == 1:
                return self._up_path
            for file in natsorted(files):
                file = ospath.join(dirpath, file)
                is_video, is_audio, _ = await get_document_type(file)
                if is_video:
                    if main_video:
                        continue
                    main_video = file
                if is_audio:
                    self.size += await get_path_size(file)
                    self._files.append(file)

        self._files.insert(0, main_video)
        self.outfile = self._up_path
        if len(self._files) > 1:
            _, size = await gather(self._name_base_dir(self.path), get_path_size(main_video))
            self.size += size
            await update_status_message(self.listener.message.chat.id)
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown']
            for i in self._files:
                cmd.extend(('-i', i))
            cmd.extend(('-map', '0:v:0?', '-map', '0:a:?'))
            for j in range(1, len(self._files)):
                cmd.extend(('-map', f'{j}:a'))

            self.outfile = ospath.join(self.path, self.name)
            streams = (await get_metavideo(main_video))[0]
            audio_track = len([1+i for i in range(len(streams)) if streams[i]['codec_type'] == 'audio'])
            cmd.extend((f'-disposition:s:a:{audio_track if audio_track == 0 else audio_track+1}', 'default', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y'))
            await self._run_cmd(cmd, 'direct')
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _merge_subs(self, **kwargs):
        main_video = False
        for dirpath, _, files in await sync_to_async(walk, self.path):
            if len(files) == 1:
                return self._up_path
            for file in natsorted(files):
                file = ospath.join(dirpath, file)
                is_video, is_sub = (await get_document_type(file))[0], file.endswith(('.ass', '.srt', '.vtt'))
                if is_video:
                    if main_video:
                        continue
                    main_video = file
                if is_sub:
                    self.size += await get_path_size(file)
                    self._files.append(file)

        self._files.insert(0, main_video)
        self.outfile = self._up_path
        if len(self._files) > 1:
            _, size = await gather(self._name_base_dir(self.path), get_path_size(main_video))
            self.size += size
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y']
            self.outfile, status = ospath.join(self.path, self.name), 'direct'
            if kwargs.get('hardsub'):
                self.path, status = self._files[0], 'prog'
                cmd.extend(('-i', self.path, '-vf'))
                fontname = kwargs.get('fontname', '').replace('_', ' ') or config_dict['HARDSUB_FONT_NAME']
                fontsize = f',FontSize={fontsize}' if (fontsize := kwargs.get('fontsize') or config_dict['HARDSUB_FONT_SIZE']) else ''
                fontcolour = f',PrimaryColour=&H{kwargs["fontcolour"]}' if kwargs.get('fontcolour') else ''
                boldstyle = ',Bold=1' if kwargs.get('boldstyle') else ''
                quality = f',scale={self._qual[kwargs["quality"]]}:-2' if kwargs.get('quality') else ''

                cmd.append(f"subtitles='{self._files[1]}':force_style='FontName={fontname},Shadow=1.5{fontsize}{fontcolour}{boldstyle}'{quality},unsharp,eq=contrast=1.07")

                if config_dict['VIDTOOLS_FAST_MODE']:
                    cmd.extend(('-preset', config_dict['LIB264_PRESET'], '-c:v', 'libx264', '-crf', '24'))
                    extra = ['-map', '0:a:?', '-c:a', 'copy']
                else:
                    cmd.extend(('-preset', config_dict['LIB265_PRESET'], '-c:v', 'libx265', '-pix_fmt', 'yuv420p10le', '-crf', '24',
                                '-profile:v', 'main10', '-x265-params', 'no-info=1', '-bsf:v', 'filter_units=remove_types=6'))
                    extra = ['-c:a', 'aac', '-b:a', '160k', '-map', '0:1']
                cmd.extend(['-map', '0:v:0?', '-map', '-0:s'] + extra + [self.outfile])
            else:
                for i in self._files:
                    cmd.extend(('-i', i))
                cmd.extend(('-map', '0:v:0?', '-map', '0:a:?', '-map', '0:s:?'))
                for j in range(1, len(self._files)):
                    cmd.extend(('-map', f'{j}:s'))
                cmd.extend(('-c:v', 'copy', '-c:a', 'copy', '-c:s', 'srt', self.outfile))
            await self._run_cmd(cmd, status)
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _vid_trimmer(self, start_time, end_time):
        for file in (file_list := await self._get_files()):
            self.path = file
            if self._metadata:
                base_dir = self.listener.dir
                await makedirs(base_dir, exist_ok=True)
            else:
                base_dir, self.size = await gather(self._name_base_dir(self.path, 'Trim', len(file_list) > 1), get_path_size(self.path))
            self.outfile = ospath.join(base_dir, self.name)
            self._files.append(self.path)
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-i', self.path, '-ss', start_time, '-to', end_time,
                   '-map', '0:v:0?', '-map', '0:a:?', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y']
            await self._run_cmd(cmd)
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _vid_marker(self, **kwargs):
        wmpath = ospath.join('watermark', f'{self.listener.mid}.png')
        for file in (file_list := await self._get_files()):
            self.path = file
            self._files.append(self.path)
            if self._metadata:
                base_dir, fsize = self.listener.dir, self.size
                await makedirs(base_dir, exist_ok=True)
            else:
                await self._name_base_dir(self.path, 'Marker', len(file_list) > 1)
                base_dir, fsize = await gather(self._name_base_dir(self.path, 'Marker', len(file_list) > 1), get_path_size(self.path))
            self.size = fsize + await get_path_size(wmpath)
            self.outfile = ospath.join(base_dir, self.name)
            wmsize, wmposition, popupwm = kwargs.get('wmsize'), kwargs.get('wmposition'), kwargs.get('popupwm') or ''
            if popupwm:
                duration = (await get_media_info(self.path))[0]
                popupwm = f':enable=lt(mod(t\,{duration}/{popupwm})\,20)'

            hardusb, subfile = kwargs.get('hardsub') or '', kwargs.get('subfile', '')
            if hardusb and await aiopath.exists(subfile):
                fontname = kwargs.get('fontname', '').replace('_', ' ') or config_dict['HARDSUB_FONT_NAME']
                fontsize = f',FontSize={fontsize}' if (fontsize := kwargs.get('fontsize') or config_dict['HARDSUB_FONT_SIZE']) else ''
                fontcolour = f',PrimaryColour=&H{kwargs["fontcolour"]}' if kwargs.get('fontcolour') else ''
                boldstyle = ',Bold=1' if kwargs.get('boldstyle') else ''
                hardusb = f",subtitles='{subfile}':force_style='FontName={fontname},Shadow=1.5{fontsize}{fontcolour}{boldstyle}',unsharp,eq=contrast=1.07"

            quality = f',scale={self._qual[kwargs["quality"]]}:-2' if kwargs.get('quality') else ''
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-i', wmpath, '-filter_complex',
                   f"[1][0]scale2ref=w='iw*{wmsize}/100':h='ow/mdar'[wm][vid];[vid][wm]overlay={wmposition}{popupwm}{quality}{hardusb}"]
            if config_dict['VIDTOOLS_FAST_MODE']:
                cmd.extend(('-c:v', 'libx264', '-preset', config_dict['LIB264_PRESET'], '-crf', '25'))
            cmd.extend(('-map', '0:a:?', '-map', '0:s:?', '-c:a', 'copy', '-c:s', 'copy', self.outfile))
            await self._run_cmd(cmd)
            if self.is_cancelled:
                return
        await gather(clean_target(wmpath), clean_target(subfile))

        return await self._final_path()

    async def _vid_compress(self, quality=None):
        file_list = await self._get_files()
        multi = len(file_list) > 1
        if not file_list:
            return self._up_path

        if self._metadata:
            base_dir = self.listener.dir
            await makedirs(base_dir, exist_ok=True)
            streams = (await get_metavideo(file_list[0]))[0]
        else:
            main_video = file_list[0]
            base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Compress', multi),
                                                             get_metavideo(main_video), get_path_size(main_video))
        await self._start_handler(streams)
        await gather(self._send_status(), self.event.wait())
        if self.is_cancelled or not isinstance(self.data, dict):
            return self._up_path

        self.outfile = self._up_path
        for file in file_list:
            self.path = file
            if not self._metadata:
                _, self.size = await gather(self._name_base_dir(self.path, 'Compress', multi), get_path_size(self.path))
            self.outfile = ospath.join(base_dir, self.name)
            self._files.append(self.path)
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-preset', config_dict['LIB265_PRESET'], '-c:v', 'libx265',
                   '-pix_fmt', 'yuv420p10le', '-crf', '24', '-profile:v', 'main10', '-map', f'0:{self.data["video"]}', '-map', '0:s:?', '-c:s', 'copy']
            if banner := config_dict['COMPRESS_BANNER']:
                sub_file = ospath.join(base_dir, 'subtitle.srt')
                self._files.append(sub_file)
                quality = f',scale={self._qual[quality]}:-2' if quality else ''
                async with aiopen(sub_file, 'w') as f:
                    await f.write(f'1\n00:00:03,000 --> 00:00:08,00\n{banner}')
                cmd.extend(('-vf', f"subtitles='{sub_file}'{quality},unsharp,eq=contrast=1.07", '-metadata', f'title={banner}', '-metadata:s:v',
                            f'title={banner}', '-x265-params', 'no-info=1', '-bsf:v', 'filter_units=remove_types=6'))
            elif quality:
                cmd.extend(('-vf', f'scale={self._qual[quality]}:-2'))

            cmd.extend(('-c:a', 'aac', '-b:a', '160k', '-map', f'0:{self.data["audio"]}?', self.outfile) if self.data else [self.outfile])
            await self._run_cmd(cmd)
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _subsync(self, type: str='sync_manual'):
        if not self._is_dir:
            return self._up_path
        self.size = await get_path_size(self.path)
        list_files = natsorted(await listdir(self.path))
        if len(list_files) <= 1:
            return self._up_path
        sub_files, ref_files = [], []
        if type == 'sync_manual':
            index = 1
            self.data = {'list': {}, 'final': {}}
            for file in list_files:
                if (await get_document_type(ospath.join(self.path, file)))[0] or file.endswith(('.srt', '.ass')):
                    self.data['list'].update({index: file})
                    index += 1
            if not self.data['list']:
                return self._up_path
            await self._start_handler()
            await gather(self._send_status(), self.event.wait())

            if self.is_cancelled or not self.data or not self.data['final']:
                return self._up_path
            for key in self.data['final'].values():
                sub_files.append(ospath.join(self.path, key['file']))
                ref_files.append(ospath.join(self.path, key['ref']))
        else:
            for file in list_files:
                file_ = ospath.join(self.path, file)
                is_video, is_audio, _ = await get_document_type(file_)
                if is_video or is_audio:
                    ref_files.append(file_)
                elif file_.lower().endswith(('.srt', '.ass')):
                    sub_files.append(file_)

            if not sub_files or not ref_files:
                return self._up_path

            if len(sub_files) > 1 and not ref_files:
                ref_files = list(filter(lambda x: (x, sub_files.remove(x)), sub_files))

        for sub_file, ref_file in zip(sub_files, ref_files):
            self._files.extend((sub_file, ref_file))
            self.size = await get_path_size(ref_file)
            self.name = ospath.basename(sub_file)
            name, ext = ospath.splitext(sub_file)
            cmd = ['alass', '--allow-negative-timestamps', ref_file, sub_file, f'{name}_SYNC.{ext}']
            await self._run_cmd(cmd, 'direct')
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _rm_stream(self):
        file_list = await self._get_files()
        multi = len(file_list) > 1
        if not file_list:
            return self._up_path

        if self._metadata:
            base_dir = self.listener.dir
            await makedirs(base_dir, exist_ok=True)
            streams = (await get_metavideo(file_list[0]))[0]
        else:
            main_video = file_list[0]
            base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Remove', multi),
                                                             get_metavideo(main_video), get_path_size(main_video))
        await self._start_handler(streams)
        await gather(self._send_status(), self.event.wait())
        if self.is_cancelled or not self.data:
            return self._up_path

        self.outfile = self._up_path
        for file in file_list:
            self.path = file
            if not self._metadata:
                _, self.size = await gather(self._name_base_dir(self.path, 'Remove', multi), get_path_size(self.path))
            key = self.data.get('key', '')
            self.outfile = ospath.join(base_dir, self.name)
            self._files.append(self.path)
            cmd = [FFMPEG_NAME, '-hide_banner', '-y', '-ignore_unknown', '-i', self.path]
            if key == 'audio':
                cmd.extend(('-map', '0', '-map', '-0:a'))
            elif key == 'subtitle':
                cmd.extend(('-map', '0', '-map', '-0:s'))
            else:
                for x in self.data['stream']:
                    if x not in self.data['sdata']:
                        cmd.extend(('-map', f'0:{x}'))
            cmd.extend(('-c', 'copy', self.outfile))
            await self._run_cmd(cmd)
            if self.is_cancelled:
                return

        return await self._final_path()

    async def _vid_extract(self):
        file_list = await self._get_files()
        if not file_list:
            return self._up_path

        if self._metadata:
            base_dir = ospath.join(self.listener.dir, self.name.split('.', 1)[0])
            await makedirs(base_dir, exist_ok=True)
            streams = (await get_metavideo(file_list[0]))[0]
        else:
            main_video = file_list[0]
            base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Extract', len(file_list) > 1),
                                                             get_metavideo(main_video), get_path_size(main_video))
        await self._start_handler(streams)
        await gather(self._send_status(), self.event.wait())
        if self.is_cancelled or not self.data:
            return self._up_path

        if await aiopath.isfile(self._up_path) or self._metadata:
            base_name = self.name if self._metadata else ospath.basename(self.path)
            self._up_path = ospath.join(base_dir, f'{base_name.rsplit(".", 1)[0]} (EXTRACT)')
            await makedirs(self._up_path, exist_ok=True)
            base_dir = self._up_path

        task_files = []
        for file in file_list:
            self.path = file
            if not self._metadata:
                self.size = await get_path_size(self.path)
            base_name = self.name if self._metadata else ospath.basename(self.path)
            base_name = base_name.rsplit('.', 1)[0]
            extension = dict(zip(['audio', 'subtitle', 'video'], self.data['extension']))

            def _build_command(stream_data):
                cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-i', self.path, '-map', f'0:{stream_data["map"]}']
                if self.data.get('alt_mode'):
                    if stream_data['type'] == 'audio':
                        cmd.extend(('-b:a', '156k'))
                    elif stream_data['type'] == 'video':
                        cmd.extend(('-c', 'copy'))
                else:
                    cmd.extend(('-c', 'copy'))
                cmd.extend((self.outfile, '-y'))
                return cmd

            keys = self.data['key']
            if isinstance(keys, int):
                stream_data = self.data['stream'][keys]
                self.name = f'{base_name}_{stream_data["lang"].upper()}.{extension[stream_data["type"]]}'
                self.outfile = ospath.join(base_dir, self.name)
                cmd = _build_command(stream_data)
                if await self._run_cmd(cmd):
                    task_files.append(file)
                else:
                    await move(file, self._up_path)
                if self.is_cancelled:
                    return
            else:
                ext_all = []
                for stream_data in self.data['stream'].values():
                    for key in keys:
                        if key == stream_data['type']:
                            self.name = f'{base_name}_{stream_data["lang"].upper()}.{extension[key]}'
                            self.outfile = ospath.join(base_dir, self.name)
                            cmd = _build_command(stream_data)
                            if await self._run_cmd(cmd):
                                ext_all.append(file)
                            if self.is_cancelled:
                                return
                if any(ext_all):
                    task_files.append(file)
                else:
                    await move(file, self._up_path)

        await gather(*[clean_target(file) for file in task_files])
        return await self._final_path(self._up_path)

    async def _vid_convert(self):
        file_list = await self._get_files()
        multi = len(file_list) > 1
        if not file_list:
            return self._up_path

        if self._metadata:
            base_dir = self.listener.dir
            await makedirs(base_dir, exist_ok=True)
            streams = (await get_metavideo(file_list[0]))[0]
        else:
            main_video = file_list[0]
            base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Convert', multi),
                                                             get_metavideo(main_video), get_path_size(main_video))
        await self._start_handler(streams)
        await gather(self._send_status(), self.event.wait())
        if self.is_cancelled or not self.data:
            return self._up_path

        self.outfile = self._up_path
        for file in file_list:
            self.path = file
            if not self._metadata:
                _, self.size = await gather(self._name_base_dir(self.path, f'Convert-{self.data}', multi), get_path_size(self.path))
            self.outfile = ospath.join(base_dir, self.name)
            self._files.append(self.path)
            cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-map', '0:v:0',
                   '-vf', f'scale={self._qual[self.data]}:-2', '-map', '0:a:?', '-map', '0:s:?', '-c:a', 'copy', '-c:s', 'copy', self.outfile]
            await self._run_cmd(cmd)
            if self.is_cancelled:
                return

        return await self._final_path()