from html import escape
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from pyrogram.types import Message
from time import time
from pytz import timezone

from bot import bot_name, task_dict, task_dict_lock, botStartTime, config_dict
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.button_build import ButtonMaker


SIZE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']


class MirrorStatus:
    STATUS_ARCHIVING = 'Archiving'
    STATUS_CHECKING = 'CheckingUp'
    STATUS_CLONING = 'Cloning'
    STATUS_COMPRESS = 'Compressing'
    STATUS_CONVERT = 'Converting'
    STATUS_SUBSYNC = 'Syncing'
    STATUS_DOWNLOADING = 'Downloading'
    STATUS_EXTRACTING = 'Extracting'
    STATUS_MERGING = 'Merging'
    STATUS_PAUSED = 'Paused'
    STATUS_QUEUEDL = 'QueueDl'
    STATUS_QUEUEUP = 'QueueUl'
    STATUS_RMSTREAM = 'Removing'
    STATUS_SAMVID = 'SamVid'
    STATUS_METADATA = 'Metadata'
    STATUS_SEEDING = 'Seeding'
    STATUS_SPLITTING = 'Splitting'
    STATUS_TRIM = 'Trimming'
    STATUS_UPLOADING = 'Uploading'
    STATUS_UPLOADINGTOGO = 'Uploading'
    STATUS_WAIT = 'Waiting'
    STATUS_WATERMARK = 'Watermarking'


STATUS_VALUES = [('ALL', 'All'),
                 ('DL', MirrorStatus.STATUS_DOWNLOADING),
                 ('UP', MirrorStatus.STATUS_UPLOADING),
                 ('QD', MirrorStatus.STATUS_QUEUEDL),
                 ('QU', MirrorStatus.STATUS_QUEUEUP),
                 ('AR', MirrorStatus.STATUS_ARCHIVING),
                 ('EX', MirrorStatus.STATUS_EXTRACTING),
                 ('CL', MirrorStatus.STATUS_CLONING),
                 ('MG', MirrorStatus.STATUS_MERGING),
                 ('CP', MirrorStatus.STATUS_COMPRESS),
                 ('CV', MirrorStatus.STATUS_CONVERT),
                 ('WM', MirrorStatus.STATUS_WATERMARK),
                 ('SD', MirrorStatus.STATUS_SEEDING)]


async def getTaskByGid(gid: str):
    async with task_dict_lock:
        return next((tk for tk in task_dict.values() if tk.gid() == gid), None)


async def getAllTasks(req_status: str):
    async with task_dict_lock:
        if req_status == 'all':
            return list(task_dict.values())
        return [tk for tk in task_dict.values() if tk.status() == req_status]


def get_readable_file_size(size_in_bytes: int | str):
    if isinstance(size_in_bytes, str):
        size_in_bytes = int(size_in_bytes)
    if not size_in_bytes:
        return '0B'
    index = 0
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1
    return f'{size_in_bytes:.2f}{SIZE_UNITS[index]}' if index > 0 else f'{size_in_bytes:.2f}B'


def get_date_time(message: Message):
    dt = message.date.astimezone(timezone(config_dict['TIME_ZONE']))
    return dt.strftime('%B %d, %Y'), dt.strftime('%H:%M:%S')


def get_readable_time(seconds):
    periods = [('d', 86400), ('h', 3600), ('m', 60), ('s', 1)]
    result = ''
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f'{int(period_value)}{period_name} '
    return result.strip()


def speed_string_to_bytes(size_text: str):
    size = 0
    size_text = size_text.lower()
    if 'k' in size_text:
        size += float(size_text.split('k')[0]) * 1024
    elif 'm' in size_text:
        size += float(size_text.split('m')[0]) * 1048576
    elif 'g' in size_text:
        size += float(size_text.split('g')[0]) * 1073741824
    elif 't' in size_text:
        size += float(size_text.split('t')[0]) * 1099511627776
    elif 'b' in size_text:
        size += float(size_text.split('b')[0])
    return size


def get_progress_bar_string(pct: str):
    pct = float(pct.strip('%'))
    p = min(max(pct, 0), 100)
    cFull = int(p // 8)
    cPart = int(p % 8 - 1)
    p_str = '⬤' * cFull
    if cPart >= 0:
        p_str += ['○', '○', '◔', '◔', '◑', '◑', '◕', '◕'][cPart]
    p_str += '○' * (12 - cFull)
    return f"[{p_str}]"


def action(message: Message):
    acts = message.text.split(maxsplit=1)[0]
    return acts.replace('/', '#').replace(f'@{bot_name}', '').replace(str(config_dict['CMD_SUFFIX']), '').lower()


def get_readable_message(sid: int, is_user: bool, page_no: int=1, status : str='All', page_step: int=1):
    msg = f'<a href="https://t.me/maheshsirop"><b><i>Bot By Mahesh Kadali</b></i></a>\n\n'
    dl_speed = up_speed = 0

    if status == 'All':
        tasks = [tk for tk in task_dict.values() if tk.listener.user_id == sid] if is_user else list(task_dict.values())
    elif is_user:
        tasks = [tk for tk in task_dict.values() if tk.status() == status and tk.listener.user_id == sid]
    else:
        tasks = [tk for tk in task_dict.values() if tk.status() == status]

    STATUS_LIMIT = config_dict['STATUS_LIMIT']
    tasks_no = len(tasks)
    pages = (max(tasks_no, 1) + STATUS_LIMIT - 1) // STATUS_LIMIT
    if page_no > pages:
        page_no = (page_no - 1) % pages + 1
    elif page_no < 1:
        page_no = pages - (abs(page_no) % pages)
    start_position = (page_no - 1) * STATUS_LIMIT
    for index, task in enumerate(tasks[start_position:STATUS_LIMIT + start_position], start=1):
        tstatus = task.status()
        msg += f'<b>{index+start_position}.</b> <code>{escape(str(task.name())) or "N/A"}</code>'
        if task.listener.isSuperChat:
            reply_to = task.listener.message.reply_to_message
            link = task.listener.message.link if not reply_to or getattr(reply_to.from_user, 'is_bot', None) else reply_to.link
            msg += f'\n\n<b>┌ <a href="{link}"><i>{tstatus}...</i></a></b>'
        else:
            msg += f'\n<b>┌ <i>{tstatus}...</i></b>'
        ext_msg = (f'\n<b>├ Engine:<i> {task.engine()}</i></b>'
                   f'\n<b>├ By:</b> <a href="https://t.me/{task.listener.message.from_user.username}">{task.listener.message.from_user.first_name}</a>' if task.listener.isSuperChat else ''
                   f'\n<b>├ Action:</b> {action(task.listener.message)}')
        if tstatus not in [MirrorStatus.STATUS_SEEDING, MirrorStatus.STATUS_METADATA, MirrorStatus.STATUS_SUBSYNC]:
            msg += (f'\n<b>├ </b>{get_progress_bar_string(task.progress())}'
                    f'\n<b>├ Progress:</b> {task.progress()}')
            if tstatus == MirrorStatus.STATUS_SPLITTING and task.listener.isLeech:
                msg += f'\n<b>├ Split Size:</b> {get_readable_file_size(task.listener.splitSize)}'
            msg += (f'\n<b>├ Processed:</b> {task.processed_bytes()}'
                    f'\n<b>├ Total Size:</b> {task.size()}'
                    f'\n<b>├ Speed:</b> {task.speed()}'
                    f'\n<b>├ ETA:</b> {task.eta() or "~"}'
                    f'\n<b>├ Elapsed: </b>{task.elapsed() or "~"}')
            if tstatus == MirrorStatus.STATUS_WAIT:
                msg += f'\n<b>├ Timeout: </b>{task.timeout()}'
            if hasattr(task, 'seeders_num'):
                try:
                    msg += f'\n<b>├ S/L:</b> {task.seeders_num()}/{task.leechers_num()}'
                except:
                    pass
        elif tstatus == MirrorStatus.STATUS_SEEDING:
            msg += (f'\n<b>├ Size:</b> {task.size()}'
                    f'\n<b>├ Speed:</b> {task.upload_speed()}'
                    f'\n<b>├ Uploaded:</b> {task.uploaded_bytes()}'
                    f'\n<b>├ Ratio:</b> {task.ratio()}'
                    f'\n<b>├ Time:</b> {task.seeding_time()}'
                    f'\n<b>├ S/L:</b> {task.seeders_num()}/{task.leechers_num()}')
        else:
            msg += (f'\n<b>├ Size:</b> {task.size()}'
                    f'\n<b>├ Elapsed:</b> {task.elapsed() or "~"}')
        msg += f'{ext_msg}\n<b>└ </b><code>/{BotCommands.CancelTaskCommand} {task.gid()}</code>\n\n'

    if not msg:
        if status == 'All':
            return None, None
        msg = f'No Active {status} Task!\n'

    for task in tasks:
        tstatus = task.status()
        if tstatus == MirrorStatus.STATUS_DOWNLOADING or task.engine() == 'JDownloader':
            dl_speed += speed_string_to_bytes(task.speed())
        elif tstatus == MirrorStatus.STATUS_UPLOADING:
            up_speed += speed_string_to_bytes(task.speed())
        elif tstatus == MirrorStatus.STATUS_SEEDING:
            up_speed += speed_string_to_bytes(task.upload_speed())

    buttons = ButtonMaker()
    if not is_user:
        buttons.button_data('☲', 'status 0 ov', 'header')

    if len(tasks) > STATUS_LIMIT:
        msg += f'<b>Page:</b> {page_no}/{pages} | <b>Tasks:</b> {tasks_no} | <b>Step:</b> {page_step}\n'
        buttons.button_data('Back', f'status {sid} pre', 'header')
        buttons.button_data('Next', f'status {sid} nex', 'header')
        if tasks_no > 30:
            for i in [1, 2, 4, 6, 8, 10, 15, 20]:
                buttons.button_data(i, f'status {sid} ps {i}', 'footer')
    if len(task_dict) > STATUS_LIMIT or status != 'All':
        for label, status_value in STATUS_VALUES:
            if status_value != status:
                buttons.button_data(label, f'status {sid} st {status_value}')
    buttons.button_data('♻️', f'status {sid} ref', 'header')
    if is_user:
        buttons.button_data('✘', f'status {sid} cls', 'header')
    msg += ('▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n'
            f'<b>CPU:</b> {cpu_percent()}% <b>| RAM:</b> {virtual_memory().percent}% <b>| FREE:</b> {get_readable_file_size(disk_usage(config_dict["DOWNLOAD_DIR"]).free)}\n'
            f'<b>IN:</b> {get_readable_file_size(net_io_counters().bytes_recv)}<b> | OUT:</b> {get_readable_file_size(net_io_counters().bytes_sent)}\n'
            f'<b>DL:</b> {get_readable_file_size(dl_speed)}/s<b> | UL:</b> {get_readable_file_size(up_speed)}/s <b>|</b> {get_readable_time(time() - botStartTime)}')
    return msg, buttons.build_menu(6)
