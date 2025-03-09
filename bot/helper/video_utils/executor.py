from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs, listdir
from aioshutil import rmtree
from ast import literal_eval
from asyncio import create_subprocess_exec, sleep, gather, Event, Lock, wait_for
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk
from time import time

from bot import config_dict, task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.links_utils import get_url_name
from bot.helper.ext_utils.media_utils import get_document_type, FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage

file_lock = Lock()
io_lock = Lock()

async def get_metavideo(video_file):
    async with io_lock:
        try:
            stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_streams', video_file])
            if rcode != 0:
                LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
                return []
            metadata = literal_eval(stdout)
            return metadata.get('streams', [])
        except Exception as e:
            LOGGER.error(f"Error in get_metavideo: {e}")
            return []

class VidEcxecutor(FFProgress):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        super().__init__()
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
        self._qual = {'1080p': '1920', '720p': '1280', '540p': '960', '480p': '854', '360p': '640'}
        self.is_cancelled = False
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {self.path}")

    async def _cleanup(self):
        async with file_lock:
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
                LOGGER.info(f"Cleanup completed for {self.mode}")
            except Exception as e:
                LOGGER.error(f"Cleanup error: {e}")

    async def _extract_zip(self, zip_path):
        extract_dir = ospath.join(ospath.dirname(zip_path), f"extracted_{self._gid}")
        try:
            await makedirs(extract_dir, exist_ok=True)
            cmd = ['7z', 'x', zip_path, f'-o{extract_dir}', '-y']
            _, stderr, rcode = await cmd_exec(cmd)
            if rcode != 0:
                LOGGER.error(f"Failed to extract ZIP: {stderr}")
                await rmtree(extract_dir, ignore_errors=True)
                return None
            LOGGER.info(f"Extracted ZIP to {extract_dir}")
            return extract_dir
        except Exception as e:
            LOGGER.error(f"ZIP extraction error: {e}")
            await rmtree(extract_dir, ignore_errors=True)
            return None

    async def _get_files(self):
        file_list = []
        async with io_lock:
            if self._metadata:
                file_list.append(self.path)
            elif await aiopath.isfile(self.path):
                if self.path.lower().endswith('.zip'):
                    extract_dir = await self._extract_zip(self.path)
                    if extract_dir:
                        self._files.append(extract_dir)
                        for dirpath, _, files in await sync_to_async(walk, extract_dir):
                            for file in natsorted(files):
                                file_path = ospath.join(dirpath, file)
                                if (await get_document_type(file_path))[0]:
                                    file_list.append(file_path)
                                    LOGGER.info(f"Found media file: {file_path}")
                elif (await get_document_type(self.path))[0]:
                    file_list.append(self.path)
            else:
                for dirpath, _, files in await sync_to_async(walk, self.path):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
                            LOGGER.info(f"Found media file: {file_path}")
                        elif file_path.lower().endswith('.zip'):
                            extract_dir = await self._extract_zip(file_path)
                            if extract_dir:
                                self._files.append(extract_dir)
                                for sub_dirpath, _, sub_files in await sync_to_async(walk, extract_dir):
                                    for sub_file in natsorted(sub_files):
                                        if (await get_document_type(ospath.join(sub_dirpath, sub_file)))[0]:
                                            file_list.append(ospath.join(sub_dirpath, sub_file))
                                            LOGGER.info(f"Found media file: {ospath.join(sub_dirpath, sub_file)}")
            if not file_list:
                LOGGER.error(f"No valid media files found in {self.path}")
            self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
            return file_list

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            return self._up_path
        
        if self._metadata and not self.name:
            self.name = get_url_name(self.path)
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'

        LOGGER.info(f"Executing {self.mode} with name: {self.name}")
        file_list = await self._get_files()
        if not file_list:
            await sendMessage("No valid video files found.", self.listener.message)
            return self._up_path

        try:
            match self.mode:
                case 'vid_vid':
                    result = await self._merge_vids(file_list)
                case 'vid_aud':
                    result = await self._merge_auds(file_list)
                case 'vid_sub':
                    result = await self._merge_subs(file_list, **kwargs)
                case 'trim':
                    result = await self._vid_trimmer(file_list, **kwargs)
                case 'watermark':
                    result = await self._vid_marker(file_list, **kwargs)
                case 'compress':
                    result = await self._vid_compress(file_list, **kwargs)
                case 'subsync':
                    result = await self._subsync(file_list, **kwargs)
                case 'rmstream':
                    result = await self._rm_stream(file_list)
                case 'extract':
                    result = await self._vid_extract(file_list)
                case 'merge_rmaudio':
                    result = await self._merge_and_rmaudio(file_list)
                case 'merge_preremove_audio':
                    result = await self._merge_preremove_audio(file_list)
                case _:
                    result = await self._vid_convert(file_list)
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            await self.listener.onUploadError(f"Failed to process {self.mode}.")
            return self._up_path
        finally:
            if self.is_cancelled:
                await self._cleanup()

    @new_task
    async def _start_handler(self, *args):
        await sleep(1)
        from bot.helper.video_utils.extra_selector import ExtraSelect
        selector = ExtraSelect(self)
        await selector.get_buttons(*args)

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update: {status}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    async def _final_path(self, outfile=''):
        async with file_lock:
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
                LOGGER.info(f"Final path: {self._up_path}")
                return self._up_path
            except Exception as e:
                LOGGER.error(f"Final path error: {e}")
                await self._cleanup()
                return self._up_path

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        async with file_lock:
            base_dir, file_name = ospath.split(path)
            if not self.name or multi:
                if info:
                    if await aiopath.isfile(path):
                        file_name = file_name.rsplit('.', 1)[0]
                    file_name += f'_{info}.mkv'
                    LOGGER.info(f"Generated name: {file_name}")
                self.name = file_name
            if not self.name.upper().endswith(('MKV', 'MP4')):
                self.name += '.mkv'
            LOGGER.info(f"Set name: {self.name} with base_dir: {base_dir}")
            return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg cmd: {' '.join(cmd)}")
            self.listener.suproc = await create_subprocess_exec(*cmd, stderr=PIPE)
            _, code = await gather(self.progress(status), self.listener.suproc.wait())
            if code == 0:
                LOGGER.info(f"FFmpeg succeeded")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info(f"FFmpeg cancelled")
            else:
                error_msg = (await self.listener.suproc.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error: {error_msg}")
                self.is_cancelled = True
            await self._cleanup()
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}")
            self.is_cancelled = True
            await self._cleanup()
            return False

    async def _merge_and_rmaudio(self, file_list):
        if len(file_list) == 1:
            self.path = file_list[0]
            return await self._rm_audio_single(file_list[0])

        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', True)
        streams = await get_metavideo(file_list[0])
        self._files = file_list
        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))

        await self._start_handler(streams)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data.get('streams_to_remove'):
            LOGGER.info("No streams selected for removal or cancelled.")
            return self._up_path

        self.outfile = ospath.join(base_dir, self.name)
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in file_list]))
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-map', '0:v']
            streams_to_remove = self.data.get('streams_to_remove', [])
            kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and s['index'] not in streams_to_remove]
            cmd.extend(['-map'] + kept_streams if kept_streams else [])
            cmd.extend(('-c', 'copy', self.outfile, '-y'))
            if not await self._run_cmd(cmd, 'direct'):
                await sendMessage("Merging failed.", self.listener.message)
                return self._up_path
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error in _merge_and_rmaudio: {e}")
            await sendMessage("Processing failed.", self.listener.message)
            return self._up_path
        finally:
            await clean_target(input_file)

    async def _merge_preremove_audio(self, file_list):
        if len(file_list) == 1:
            self.path = file_list[0]
            return await self._rm_audio_single(file_list[0])

        stream_data = await gather(*[get_metavideo(f) for f in file_list])
        streams_per_file = {f: streams for f, streams in zip(file_list, stream_data)}
        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))
        base_dir = await self._name_base_dir(file_list[0], 'Merge-PreRemoveAudio', True)

        await self._start_handler(streams_per_file)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data:
            LOGGER.info("Cancelled or no selections made.")
            return self._up_path

        temp_files = []
        for file in file_list:
            outfile = ospath.join(base_dir, f"temp_{ospath.basename(file)}_{self._gid}")
            selections = self.data.get('streams_to_remove', [])
            streams = streams_per_file[file]
            cmd = [FFMPEG_NAME, '-i', file, '-map', '0:v']
            kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and f"{file}_{s['index']}" not in selections]
            cmd.extend(['-map'] + kept_streams if kept_streams else [])
            cmd.extend(('-c', 'copy', outfile, '-y'))
            if await self._run_cmd(cmd):
                temp_files.append(outfile)
            else:
                await gather(*[clean_target(f) for f in temp_files])
                await sendMessage("Processing failed.", self.listener.message)
                return self._up_path

        self.outfile = ospath.join(base_dir, self.name)
        self._files = temp_files
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in temp_files]))
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                await sendMessage("Merging failed.", self.listener.message)
                return self._up_path
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error in _merge_preremove_audio: {e}")
            await sendMessage("Processing failed.", self.listener.message)
            return self._up_path
        finally:
            await gather(*[clean_target(f) for f in temp_files])
            await clean_target(input_file)

    async def _rm_audio_single(self, single_file):
        streams = await get_metavideo(single_file)
        if not streams:
            LOGGER.error(f"No streams found in {single_file}")
            await sendMessage("No streams found.", self.listener.message)
            return self._up_path
        
        await self._start_handler(streams)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data:
            LOGGER.info(f"Cancelled or no selections for {single_file}")
            return self._up_path

        base_dir = await self._name_base_dir(single_file, 'RemoveAudio')
        self.outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', single_file, '-map', '0:v']
        selections = self.data.get('streams_to_remove', [])
        kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and s['index'] not in selections]
        cmd.extend(['-map'] + kept_streams if kept_streams else [])
        cmd.extend(('-c', 'copy', self.outfile, '-y'))
        if not await self._run_cmd(cmd):
            await sendMessage("Processing failed.", self.listener.message)
            return self._up_path
        return await self._final_path()

    async def _merge_vids(self, file_list):
        if len(file_list) == 1:
            return file_list[0]
        
        base_dir = await self._name_base_dir(file_list[0], 'MergeVideos', True)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in file_list]))
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                return self._up_path
            return await self._final_path()
        finally:
            await clean_target(input_file)

    async def _merge_auds(self, file_list):
        main_video = None
        audio_files = []
        for file in file_list:
            is_video, is_audio, _ = await get_document_type(file)
            if is_video and not main_video:
                main_video = file
            if is_audio:
                audio_files.append(file)
        
        if not main_video or not audio_files:
            return self._up_path
        
        base_dir = await self._name_base_dir(main_video, 'MergeAudio', True)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = [main_video] + audio_files
        cmd = [FFMPEG_NAME]
        for i, f in enumerate(self._files):
            cmd.extend(('-i', f))
        cmd.extend(('-map', '0:v', '-map', '0:a?'))
        for i in range(1, len(audio_files) + 1):
            cmd.extend(('-map', f'{i}:a'))
        cmd.extend(('-c', 'copy', self.outfile, '-y'))
        if not await self._run_cmd(cmd, 'direct'):
            return self._up_path
        return await self._final_path()

    async def _merge_subs(self, file_list, **kwargs):
        main_video = None
        sub_files = []
        for file in file_list:
            is_video = (await get_document_type(file))[0]
            if is_video and not main_video:
                main_video = file
            elif file.lower().endswith(('.srt', '.ass', '.vtt')):
                sub_files.append(file)
        
        if not main_video or not sub_files:
            return self._up_path
        
        base_dir = await self._name_base_dir(main_video, 'MergeSubs', True)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = [main_video] + sub_files
        cmd = [FFMPEG_NAME]
        for f in self._files:
            cmd.extend(('-i', f))
        cmd.extend(('-map', '0:v', '-map', '0:a?'))
        for i in range(1, len(sub_files) + 1):
            cmd.extend(('-map', f'{i}:s'))
        cmd.extend(('-c', 'copy', '-c:s', 'srt', self.outfile, '-y'))
        if not await self._run_cmd(cmd, 'direct'):
            return self._up_path
        return await self._final_path()

    async def _vid_trimmer(self, file_list, **kwargs):
        start_time = kwargs.get('start_time', '00:00:00')
        end_time = kwargs.get('end_time', '00:01:00')
        base_dir = await self._name_base_dir(file_list[0], 'Trim', len(file_list) > 1)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list
        cmd = [FFMPEG_NAME, '-i', file_list[0], '-ss', start_time, '-to', end_time, '-c', 'copy', self.outfile, '-y']
        if not await self._run_cmd(cmd):
            return self._up_path
        return await self._final_path()

    async def _vid_marker(self, file_list, **kwargs):
        wmpath = ospath.join('watermark', f'{self.listener.mid}.png')
        if not await aiopath.exists(wmpath):
            return self._up_path
        
        base_dir = await self._name_base_dir(file_list[0], 'Watermark', len(file_list) > 1)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list + [wmpath]
        cmd = [FFMPEG_NAME, '-i', file_list[0], '-i', wmpath, '-filter_complex',
               f'[1][0]scale2ref=w=iw*{kwargs.get("wmsize", 10)}/100:h=ow/mdar[wm][vid];[vid][wm]overlay={kwargs.get("wmposition", "5:5")}',
               '-c:v', 'libx264', '-c:a', 'copy', self.outfile, '-y']
        if not await self._run_cmd(cmd):
            return self._up_path
        return await self._final_path()

    async def _vid_compress(self, file_list, **kwargs):
        quality = kwargs.get('quality', '720p')
        base_dir = await self._name_base_dir(file_list[0], 'Compress', len(file_list) > 1)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list
        streams = await get_metavideo(file_list[0])
        await self._start_handler(streams)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data:
            return self._up_path
        
        cmd = [FFMPEG_NAME, '-i', file_list[0], '-c:v', 'libx265', '-vf', f'scale={self._qual[quality]}:-2',
               '-c:a', 'aac', '-b:a', '128k', self.outfile, '-y']
        if not await self._run_cmd(cmd):
            return self._up_path
        return await self._final_path()

    async def _subsync(self, file_list, **kwargs):
        # Placeholder: Implement subsync logic as needed
        return self._up_path

    async def _rm_stream(self, file_list):
        streams = await get_metavideo(file_list[0])
        await self._start_handler(streams)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data:
            return self._up_path
        
        base_dir = await self._name_base_dir(file_list[0], 'RemoveStream', len(file_list) > 1)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list
        cmd = [FFMPEG_NAME, '-i', file_list[0]]
        selections = self.data.get('streams_to_remove', [])
        kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in selections]
        cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
        cmd.extend(('-c', 'copy', self.outfile, '-y'))
        if not await self._run_cmd(cmd):
            return self._up_path
        return await self._final_path()

    async def _vid_extract(self, file_list):
        # Placeholder: Implement extract logic as needed
        return self._up_path

    async def _vid_convert(self, file_list):
        streams = await get_metavideo(file_list[0])
        await self._start_handler(streams)
        await wait_for(self.event.wait(), timeout=180)
        if self.is_cancelled or not self.data:
            return self._up_path
        
        quality = self.data.get('quality', '720p')
        base_dir = await self._name_base_dir(file_list[0], 'Convert', len(file_list) > 1)
        self.outfile = ospath.join(base_dir, self.name)
        self._files = file_list
        cmd = [FFMPEG_NAME, '-i', file_list[0], '-vf', f'scale={self._qual[quality]}:-2', '-c:a', 'copy', self.outfile, '-y']
        if not await self._run_cmd(cmd):
            return self._up_path
        return await self._final_path()