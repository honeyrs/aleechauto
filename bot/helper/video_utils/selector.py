from __future__ import annotations
from asyncio import Event, wait_for, wrap_future, gather
from functools import partial
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import CallbackQuery
from time import time

from bot import config_dict, VID_MODE, LOGGER
from bot.helper.ext_utils.bot_utils import new_thread
from bot.helper.ext_utils.status_utils import get_readable_time
from bot.helper.listeners import tasks_listener as task
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, deleteMessage

class SelectMode:
    def __init__(self, listener: task.TaskListener, isLink=False):
        self._isLink = isLink
        self._time = time()
        self._reply = None
        self.listener = listener
        self.mode = ''
        self.newname = ''
        self.extra_data = {}
        self.event = Event()
        self.is_cancelled = False
        LOGGER.info(f"Initialized SelectMode for user {self.listener.user_id}, isLink: {isLink}")

    @new_thread
    async def _event_handler(self):
        pfunc = partial(cb_vidtools, obj=self)
        handler = self.listener.client.add_handler(
            CallbackQueryHandler(pfunc, filters=regex('^vidtool') & user(self.listener.user_id)), group=-1)
        try:
            await wait_for(self.event.wait(), timeout=180)
            LOGGER.info(f"SelectMode event completed for user {self.listener.user_id}")
        except TimeoutError:
            self.mode = 'Task cancelled due to timeout!'
            self.is_cancelled = True
            self.event.set()
            LOGGER.warning(f"SelectMode timed out for user {self.listener.user_id}")
        except Exception as e:
            LOGGER.error(f"Event handler error: {e}", exc_info=True)
            self.is_cancelled = True
            self.event.set()
        finally:
            self.listener.client.remove_handler(*handler)
            if self.is_cancelled and self._reply:
                await editMessage(self.mode, self._reply)

    async def _send_message(self, text: str, buttons):
        try:
            if not self._reply:
                self._reply = await sendMessage(text, self.listener.message, buttons)
                LOGGER.info(f"Sent initial message for mode selection to user {self.listener.user_id}")
            else:
                await editMessage(text, self._reply, buttons)
                LOGGER.info(f"Updated message for mode selection for user {self.listener.user_id}")
        except Exception as e:
            LOGGER.error(f"Failed to send message: {e}")

    def _captions(self):
        msg = (f'<b>VIDEO TOOLS SETTINGS</b>\n'
               f'Mode: <b>{VID_MODE.get(self.mode, "Not Selected")}</b>\n'
               f'Output Name: <b>{self.newname or "Default"}</b>')
        if self.extra_data:
            if self.mode == 'trim':
                msg += f'\nTrim Duration: <b>{self.extra_data.get("start_time", "00:00:00")} - {self.extra_data.get("end_time", "00:01:00")}</b>'
            elif self.mode in ('vid_sub', 'watermark'):
                hardsub = self.extra_data.get('hardsub', False)
                msg += f'\nHardsub: <b>{"Enabled" if hardsub else "Disabled"}</b>'
            if quality := self.extra_data.get('quality'):
                msg += f'\nQuality: <b>{quality}</b>'
            if self.mode == 'watermark':
                if wmsize := self.extra_data.get('wmsize'):
                    msg += f'\nWatermark Size: <b>{wmsize}%</b>'
                if wmposition := self.extra_data.get('wmposition'):
                    pos_dict = {'5:5': 'Top Left', 'main_w-overlay_w-5:5': 'Top Right', 
                                '5:main_h-overlay_h': 'Bottom Left', 'main_w-overlay_w-5:main_h-overlay_h-5': 'Bottom Right'}
                    msg += f'\nWatermark Position: <b>{pos_dict.get(wmposition, wmposition)}</b>'
        elapsed_time = int(time() - self._time)
        remaining_time = max(0, 180 - elapsed_time)
        msg += f'\n\n<i>Time Remaining: {get_readable_time(remaining_time)}</i>'
        return msg

    async def list_buttons(self, sub_menu=''):
        buttons = ButtonMaker()
        if not sub_menu:
            modes = {
                'merge_rmaudio': 'Merge & Remove Audio',
                'merge_preremove_audio': 'Merge Pre-Remove Audio',
                'convert': 'Convert Resolution'
            }
            if not self._isLink:
                modes['subsync'] = 'Subtitle Sync'
                modes['extract'] = 'Extract Streams'
            
            for key, value in modes.items():
                if key not in config_dict.get('DISABLE_VIDTOOLS', []):
                    buttons.button_data(f"{'🔵 ' if self.mode == key else ''}{value}", f'vidtool {key}')
            buttons.button_data('Cancel', 'vidtool cancel', 'footer')
            if self.mode:
                buttons.button_data('Done', 'vidtool done', 'footer')
            if self.mode in ('trim', 'watermark', 'compress'):
                buttons.button_data('Settings', f'vidtool settings_{self.mode}', 'header')
        elif sub_menu.startswith('settings_'):
            mode = sub_menu.split('_')[1]
            if mode == 'trim':
                buttons.button_data(f"{'🔵 ' if 'start_time' in self.extra_data else ''}Set Trim", 'vidtool trim_set', 'header')
            elif mode == 'watermark':
                buttons.button_data('Size', 'vidtool wm_size', 'header')
                buttons.button_data('Position', 'vidtool wm_pos', 'header')
                buttons.button_data(f"{'🔵 ' if self.extra_data.get('hardsub') else ''}Hardsub", 'vidtool wm_hardsub', 'header')
            elif mode == 'compress':
                buttons.button_data('Quality', 'vidtool compress_quality', 'header')
            buttons.button_data('Back', 'vidtool back', 'footer')
            buttons.button_data('Done', 'vidtool done', 'footer')
        elif sub_menu == 'wm_size':
            for size in [5, 10, 15, 20, 25, 30]:
                buttons.button_data(f"{'🔵 ' if self.extra_data.get('wmsize') == size else ''}{size}%", f'vidtool wm_size {size}')
            buttons.button_data('Back', 'vidtool settings_watermark', 'footer')
        elif sub_menu == 'wm_pos':
            positions = {'5:5': 'Top Left', 'main_w-overlay_w-5:5': 'Top Right', 
                         '5:main_h-overlay_h': 'Bottom Left', 'main_w-overlay_w-5:main_h-overlay_h-5': 'Bottom Right'}
            for key, value in positions.items():
                buttons.button_data(f"{'🔵 ' if self.extra_data.get('wmposition') == key else ''}{value}", f'vidtool wm_pos {key}')
            buttons.button_data('Back', 'vidtool settings_watermark', 'footer')
        elif sub_menu == 'compress_quality':
            qualities = ['1080p', '720p', '540p', '480p', '360p']
            for q in qualities:
                buttons.button_data(f"{'🔵 ' if self.extra_data.get('quality') == q else ''}{q}", f'vidtool compress_quality {q}')
            buttons.button_data('Back', 'vidtool settings_compress', 'footer')

        await self._send_message(self._captions(), buttons.build_menu(2))

    async def get_buttons(self):
        LOGGER.info(f"Starting get_buttons for user {self.listener.user_id}")
        future = self._event_handler()
        try:
            await gather(self.list_buttons(), wrap_future(future))
            if self.is_cancelled:
                LOGGER.info(f"Task cancelled for user {self.listener.user_id}")
                return None
            await deleteMessage(self._reply)
            LOGGER.info(f"Mode selected: {self.mode}, name: {self.newname}, extra: {self.extra_data}")
            return [self.mode, self.newname, self.extra_data]
        except Exception as e:
            LOGGER.error(f"Error in get_buttons: {e}", exc_info=True)
            self.is_cancelled = True
            return None

@new_thread
async def cb_vidtools(_, query: CallbackQuery, obj: SelectMode):
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        return
    await query.answer()
    LOGGER.info(f"Callback received: {query.data}")

    if data[1] in config_dict.get('DISABLE_VIDTOOLS', []):
        await query.answer(f"{VID_MODE[data[1]]} is disabled!", show_alert=True)
        return

    match data[1]:
        case 'done':
            obj.event.set()
            LOGGER.info(f"Done triggered by user {obj.listener.user_id}")
        case 'cancel':
            obj.mode = 'Task has been cancelled!'
            obj.is_cancelled = True
            obj.event.set()
            LOGGER.info(f"Cancel triggered by user {obj.listener.user_id}")
        case 'back':
            await obj.list_buttons()
        case 'settings_trim' | 'settings_watermark' | 'settings_compress' as submenu:
            await obj.list_buttons(submenu)
        case 'trim_set':
            obj.extra_data['start_time'] = '00:00:00'  # Placeholder, expand with message handler if needed
            obj.extra_data['end_time'] = '00:01:00'
            await obj.list_buttons('settings_trim')
        case 'wm_size':
            if len(data) > 2:
                obj.extra_data['wmsize'] = int(data[2])
            await obj.list_buttons('wm_size')
        case 'wm_pos':
            if len(data) > 2:
                obj.extra_data['wmposition'] = data[2]
            await obj.list_buttons('wm_pos')
        case 'wm_hardsub':
            obj.extra_data['hardsub'] = not obj.extra_data.get('hardsub', False)
            await obj.list_buttons('settings_watermark')
        case 'compress_quality':
            if len(data) > 2:
                obj.extra_data['quality'] = data[2]
            await obj.list_buttons('compress_quality')
        case value:
            obj.mode = value
            obj.extra_data.clear()
            await obj.list_buttons()