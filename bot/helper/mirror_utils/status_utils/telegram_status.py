from time import time

from bot.helper.ext_utils.status_utils import MirrorStatus, get_readable_file_size, get_readable_time


class TelegramStatus:
    def __init__(self, listener, obj, size, gid, status):
        self._obj = obj
        self._size = size
        self._gid = gid
        self._status = status
        self._elapsed = time()
        self.listener = listener

    @staticmethod
    def engine():
        return 'Pyrofork'

    def elapsed(self):
        return get_readable_time(time() - self._elapsed)

    def processed_bytes(self):
        return get_readable_file_size(self._obj._processed_bytes)  # Use _processed_bytes from TgUploader

    def size(self):
        return get_readable_file_size(self._size)

    def status(self):
        return MirrorStatus.STATUS_DOWNLOADING if self._status == 'dl' else MirrorStatus.STATUS_UPLOADING

    def name(self):
        return self.listener.name

    def progress(self):
        try:
            progress_raw = self._obj._processed_bytes / self._size * 100
        except:
            progress_raw = 0
        return f'{round(progress_raw, 2)}%'

    def speed(self):
        try:
            elapsed = time() - self._obj._start_time
            if elapsed > 0:
                return f'{get_readable_file_size(self._obj._processed_bytes / elapsed)}/s'
            return '0 B/s'
        except:
            return '0 B/s'

    def eta(self):
        try:
            elapsed = time() - self._obj._start_time
            if elapsed > 0:
                speed = self._obj._processed_bytes / elapsed
                remaining_bytes = self._size - self._obj._processed_bytes
                if speed > 0:
                    return get_readable_time(remaining_bytes / speed)
            return '~'
        except:
            return '~'

    def gid(self):
        return self._gid

    def task(self):
        return self._obj