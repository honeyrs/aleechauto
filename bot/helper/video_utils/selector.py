from __future__ import annotations
from asyncio import sleep
from bot import LOGGER
from bot.helper.telegram_helper.message_utils import sendMessage, deleteMessage

class SelectMode:
    def __init__(self, listener):
        self.listener = listener
        self._reply = None
        self.is_cancelled = False
        self.mode = None
        self.newname = None
        self.extra_data = {}

    async def _send_message(self, text: str):
        try:
            if not self._reply:
                self._reply = await sendMessage(text, self.listener.message)
        except Exception as e:
            LOGGER.error(f"Failed to send message: {e}")
            self.is_cancelled = True

    async def list_buttons(self):
        # Simplified auto-selection logic
        self.mode = 'merge_rmaudio'  # Default mode
        self.newname = self.listener.name or "processed_video"
        text = f"Selected mode: {self.mode}\nName: {self.newname}"
        await self._send_message(text)
        await sleep(1)  # Simulate user interaction delay
        if self._reply:
            await deleteMessage(self._reply)

    async def get_buttons(self):
        LOGGER.info(f"Starting mode selection for MID: {self.listener.mid}")
        try:
            await self.list_buttons()
            if self.is_cancelled:
                await self.listener.onUploadError("Mode selection cancelled")
                return None
            LOGGER.info(f"Mode selected: {self.mode}, name: {self.newname}")
            return [self.mode, self.newname, self.extra_data]
        except Exception as e:
            LOGGER.error(f"Mode selection error: {e}", exc_info=True)
            await self.listener.onUploadError(f"Mode selection failed: {e}")
            return None

    async def choose_mode(self, path):
        result = await self.get_buttons()
        if result:
            from bot.helper.video_utils.executor import VidEcxecutor
            executor = VidEcxecutor(self.listener, path, f"task_{self.listener.mid}")
            self.listener.vidMode = result
            await executor.execute()