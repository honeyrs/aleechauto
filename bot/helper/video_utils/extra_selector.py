from __future__ import annotations
from time import time

from bot import LOGGER, VID_MODE
from bot.helper.ext_utils.status_utils import get_readable_file_size
from bot.helper.telegram_helper.message_utils import sendMessage, deleteMessage
from bot.helper.video_utils import executor as exc

class ExtraSelect:
    def __init__(self, executor: exc.VidEcxecutor):
        self._listener = executor.listener
        self._time = time()
        self._reply = None
        self.executor = executor
        self.is_cancelled = False
        LOGGER.info(f"Initialized ExtraSelect for {self.executor.mode} (MID: {self.executor.listener.mid})")

    async def _send_message(self, text: str):
        try:
            if not self._reply:
                LOGGER.info(f"Sending initial ExtraSelect message for {self.executor.mode}")
                self._reply = await sendMessage(text, self._listener.message)
        except Exception as e:
            LOGGER.error(f"Failed to send message: {e}")
            self.is_cancelled = True

    def _format_stream_name(self, stream):
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    def _is_telugu(self, lang):
        """Check if a language tag indicates Telugu."""
        if not lang:
            return False
        lang = lang.lower()
        telugu_tags = ['tel', 'te', 'తెలుగు']  # Standard tags and Telugu script
        return any(tag in lang for tag in telugu_tags)

    async def streams_select(self, streams=None):
        if 'streams' not in self.executor.data:
            if not streams or not isinstance(streams, list):
                LOGGER.warning(f"No valid streams provided for {self.executor.mode}")
                return "No streams available."
            LOGGER.info(f"Initializing stream data for {self.executor.mode}")
            self.executor.data = {'streams': {}, 'streams_to_remove': []}
            for stream in streams:
                if isinstance(stream, dict) and 'codec_type' in stream:
                    index = stream['index']
                    self.executor.data['streams'][index] = stream
                    self.executor.data['streams'][index]['info'] = self._format_stream_name(stream)

        streams_dict = self.executor.data['streams']
        if not streams_dict:
            LOGGER.warning(f"No streams found in data for {self.executor.mode}")
            return "No streams to process."

        text = (f'<b>{VID_MODE[self.executor.mode].upper()} ~ {self._listener.tag}</b>\n'
                f'<code>{self.executor.name}</code>\n'
                f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n'
                f'\n<b>Streams:</b>\n')

        has_telugu_audio = any(
            stream.get('codec_type') == 'audio' and self._is_telugu(stream.get('tags', {}).get('language', ''))
            for stream in streams_dict.values()
        )

        for key, value in streams_dict.items():
            codec_type = value.get('codec_type', 'unknown')
            lang = value.get('tags', {}).get('language', '')
            is_metadata = codec_type == 'data' or (codec_type == 'unknown' and 'metadata' in value.get('tags', {}).get('title', '').lower())

            if has_telugu_audio:
                if (codec_type == 'audio' and self._is_telugu(lang)) or codec_type == 'video' or is_metadata:
                    text += f"{value['info']}\n"
                else:
                    self.executor.data['streams_to_remove'].append(key)
                    text += f"🚫 {value['info']} (Removed)\n"
            else:
                if codec_type in ['audio', 'video'] or is_metadata:
                    text += f"{value['info']}\n"
                else:
                    self.executor.data['streams_to_remove'].append(key)
                    text += f"🚫 {value['info']} (Removed)\n"

        if self.executor.data['streams_to_remove']:
            text += '\n<b>Removed Streams:</b>\n'
            for i, key in enumerate(self.executor.data['streams_to_remove'], start=1):
                text += f"{i}. {self.executor.data['streams'][key]['info']}\n"

        LOGGER.info(f"Prepared streams_select text for {self.executor.mode}")
        return text

    async def get_buttons(self, *args):
        LOGGER.info(f"Starting get_buttons for {self.executor.mode}")
        if not args or not args[0]:
            LOGGER.error(f"No valid streams passed for {self.executor.mode}")
            self.is_cancelled = True
            self.executor.is_cancelled = True
            self.executor.event.set()
            return

        message_text = await self.streams_select(*args)
        await self._send_message(message_text)
        if self._reply:
            await deleteMessage(self._reply)

        if self.is_cancelled:
            self._listener.suproc = 'cancelled'
            await self._listener.onUploadError(f'{VID_MODE[self.executor.mode]} stopped due to error!')
        else:
            LOGGER.info(f"Selections completed: {self.executor.data}")
            self.executor.event.set()