from bot import LOGGER, VID_MODE
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage

class SelectMode:
    def __init__(self, listener):
        self.listener = listener
        self.mode = ''
        self.newname = ''
        self.extra_data = {}

    async def get_buttons(self):
        buttons = ButtonMaker()
        self.mode = 'merge_rmaudio'
        msg = f"Selected mode: **{VID_MODE.get(self.mode, self.mode)}**\nEnter new name (or leave blank for default):"
        buttons.cb_buildbutton("Confirm", f"vidmode {self.listener.mid} confirm")
        buttons.cb_buildbutton("Cancel", f"vidmode {self.listener.mid} cancel")
        LOGGER.info(f"Sending mode selection message: {msg}")
        await sendMessage(msg, self.listener.message, buttons.build_menu(1))

    async def set_values(self, newname):
        LOGGER.info(f"Setting mode values: mode={self.mode}, newname={newname}")
        self.newname = newname.strip() if newname.strip() else ''
        return [self.mode, self.newname, self.extra_data]