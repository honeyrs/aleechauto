from asyncio import sleep

from bot import LOGGER, config_dict, task_dict_lock, task_dict
from bot.helper.ext_utils.bot_utils import new_task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import editMessage, sendMessage
from bot.helper.video_utils.executor import get_metavideo

class ExtraSelect:
    def __init__(self, executor):
        self.executor = executor
        self.streams_select = []

    @new_task
    async def get_buttons(self, streams):
        buttons = ButtonMaker()
        msg = "**Select streams to keep:**\n"
        SUPPORTED_LANGUAGES = config_dict.get('SUPPORTED_LANGUAGES', ['eng']).copy()
        ALWAYS_REMOVE_LANGUAGES = config_dict.get('ALWAYS_REMOVE_LANGUAGES', []).copy()
        if not streams:
            msg += "No streams found in the video file."
            await sendMessage(msg, self.executor.listener.message)
            self.executor.event.set()
            return
        for stream in streams:
            if stream['codec_type'] == 'video':
                msg += f"\n**Video** (Stream {stream['index']}): {stream.get('codec_name', 'Unknown')}"
                continue
            lang = stream.get('tags', {}).get('language', 'und')
            if stream['codec_type'] == 'audio':
                if lang in SUPPORTED_LANGUAGES:
                    msg += f"\n**Audio** (Stream {stream['index']}): {lang} ({stream.get('codec_name', 'Unknown')}) - **Keeping**"
                    self.streams_select.append(stream['index'])
                elif lang in ALWAYS_REMOVE_LANGUAGES:
                    msg += f"\n**Audio** (Stream {stream['index']}): {lang} ({stream.get('codec_name', 'Unknown')}) - **Removing**"
                else:
                    msg += f"\n**Audio** (Stream {stream['index']}): {lang} ({stream.get('codec_name', 'Unknown')})"
                    buttons.cb_buildbutton(f"Keep {stream['index']}", f"videx {self.executor._gid} keep {stream['index']}")
                    buttons.cb_buildbutton(f"Remove {stream['index']}", f"videx {self.executor._gid} remove {stream['index']}")
            elif stream['codec_type'] == 'subtitle':
                if lang in SUPPORTED_LANGUAGES:
                    msg += f"\n**Subtitle** (Stream {stream['index']}): {lang} ({stream.get('codec_name', 'Unknown')}) - **Keeping**"
                    self.streams_select.append(stream['index'])
                else:
                    msg += f"\n**Subtitle** (Stream {stream['index']}): {lang} ({stream.get('codec_name', 'Unknown')}) - **Removing**"
        buttons.cb_buildbutton("Done", f"videx {self.executor._gid} done")
        LOGGER.info(f"Stream selection message: {msg}")
        await sendMessage(msg, self.executor.listener.message, buttons.build_menu(2))

    async def update_streams(self, mode, index):
        LOGGER.debug(f"Updating streams: {mode} {index}")
        if mode == 'keep':
            self.streams_select.append(int(index))
        elif mode == 'remove' and int(index) in self.streams_select:
            self.streams_select.remove(int(index))
        async with task_dict_lock:
            if self.executor._gid in task_dict:
                task_dict[self.executor._gid] = FFMpegStatus(self.executor.listener, self.executor, self.executor._gid, 'wait')
        await sendStatusMessage(self.executor.listener.message)

    async def finish_selection(self):
        LOGGER.info(f"Finished stream selection: Keeping streams {self.streams_select}")
        streams = await get_metavideo(self.executor.path if self.executor._metadata else self.executor._files[0])
        self.executor.data['streams_to_remove'] = [s['index'] for s in streams if s['index'] not in self.streams_select and s['codec_type'] != 'video']
        self.executor.event.set()