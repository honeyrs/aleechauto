from aiofiles.os import path as aiopath
from asyncio import Event, Lock, sleep
from os import path as ospath

from bot import config_dict, bot_loop, LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async, presuf_remname_name, is_premium_user
from bot.helper.ext_utils.files_utils import get_base_name, check_storage_threshold
from bot.helper.ext_utils.links_utils import is_gdrive_id, is_mega_link
from bot.helper.mirror_utils.gdrive_utlis.search import gdSearch

# Queue structures
non_queued_dl = set()  # Active downloads
queued_dl = {}        # Waiting downloads (MID -> Event)
non_queued_up = set()  # Active uploads
queued_up = {}        # Waiting uploads (MID -> Event)
ffmpeg_queue = {}     # Waiting FFmpeg tasks (MID -> (Event, task_type, file_details))
active_ffmpeg = None  # Current FFmpeg task MID

# Locks for each queue
queue_dict_lock = Lock()      # Downloads
upload_queue_lock = Lock()    # Uploads
ffmpeg_queue_lock = Lock()    # FFmpeg

async def stop_duplicate_check(listener):
    """Check for duplicate files in Google Drive."""
    if (isinstance(listener.upDest, int) or listener.isLeech or listener.select or listener.sameDir
        or not is_gdrive_id(listener.upDest) or not listener.stopDuplicate):
        return None, ''
    name = listener.name
    LOGGER.info(f'Checking File/Folder if already in Drive: {name}')
    if listener.compress:
        name = f'{name}.zip'
    elif listener.extract:
        try:
            name = get_base_name(name)
        except Exception:
            name = None
    if name:
        if not listener.isRename and await aiopath.isfile(ospath.join(listener.dir, name)):
            name = presuf_remname_name(listener.user_dict, name)
        count, file = await sync_to_async(gdSearch(stopDup=True, noMulti=listener.isClone).drive_list, name, listener.upDest, listener.user_id)
        if count:
            LOGGER.info(f"Duplicate found: {name}")
            return file, name
    LOGGER.info('Checking duplicate is passed...')
    return None, ''

async def check_limits_size(listener, size, playlist=False, play_count=False):
    """Check size and playlist limits."""
    msgerr = None
    max_pyt, megadl, torddl, zuzdl, leechdl, storage = (
        config_dict['MAX_YTPLAYLIST'], config_dict['MEGA_LIMIT'], config_dict['TORRENT_DIRECT_LIMIT'],
        config_dict['ZIP_UNZIP_LIMIT'], config_dict['LEECH_LIMIT'], config_dict['STORAGE_THRESHOLD']
    )
    if config_dict.get('PREMIUM_MODE') and not is_premium_user(listener.user_id):
        mdl = torddl = zuzdl = leechdl = config_dict.get('NONPREMIUM_LIMIT', 0)
        megadl = min(megadl, mdl)
        max_pyt = 10

    arch = any([listener.compress, listener.isLeech, listener.extract])
    if torddl and not arch and size >= torddl * 1024**3:
        msgerr = f'Torrent/direct limit is {torddl}GB'
    elif zuzdl and any([listener.compress, listener.extract]) and size >= zuzdl * 1024**3:
        msgerr = f'Zip/Unzip limit is {zuzdl}GB'
    elif leechdl and listener.isLeech and size >= leechdl * 1024**3:
        msgerr = f'Leech limit is {leechdl}GB'
    elif is_mega_link(listener.link) and megadl and size >= megadl * 1024**3:
        msgerr = f'Mega limit is {megadl}GB'
    elif max_pyt and playlist and (play_count > max_pyt):
        msgerr = f'Only {max_pyt} playlist allowed. Current playlist is {play_count}.'
    elif storage and not await check_storage_threshold(size, arch):
        msgerr = f'Need {storage}GB free storage'
    if msgerr:
        LOGGER.info(f"Limit check failed: {msgerr}")
    return msgerr

async def check_running_tasks(mid: int, state='dl'):
    """Check if task can start; queue if limits hit."""
    all_limit = config_dict.get('QUEUE_ALL', 0)  # Overall limit
    state_limit = 0 if state == 'dl' else 1  # Downloads unlimited, uploads 1
    event = None
    is_over_limit = False
    target_lock = queue_dict_lock if state == 'dl' else upload_queue_lock
    target_non_queued = non_queued_dl if state == 'dl' else non_queued_up
    target_queued = queued_dl if state == 'dl' else queued_up
    
    async with target_lock:
        if state == 'up' and mid in non_queued_dl:
            non_queued_dl.remove(mid)
            LOGGER.info(f"Removed MID {mid} from non_queued_dl for upload")
        dl_count, up_count = len(non_queued_dl), len(non_queued_up)
        is_over_limit = (all_limit > 0 and dl_count + up_count >= all_limit) or \
                        (state_limit > 0 and len(target_non_queued) >= state_limit)
        if is_over_limit:
            event = Event()
            target_queued[mid] = event
        else:
            target_non_queued.add(mid)
        LOGGER.info(f"Check {state} - MID: {mid}, dl_count: {dl_count}, up_count: {up_count}, all_limit: {all_limit}, state_limit: {state_limit}, queued: {is_over_limit}")
    return is_over_limit, event

async def start_dl_from_queued(mid: int):
    """Release a download from queue."""
    async with queue_dict_lock:
        if mid in queued_dl:
            LOGGER.info(f"Releasing queued download task MID: {mid}")
            queued_dl[mid].set()
            del queued_dl[mid]
            non_queued_dl.add(mid)
        else:
            LOGGER.warning(f"MID {mid} not found in queued_dl")
    await sleep(0.5)

async def start_up_from_queued(mid: int):
    """Release an upload from queue."""
    async with upload_queue_lock:
        if mid in queued_up:
            LOGGER.info(f"Releasing queued upload task MID: {mid}")
            queued_up[mid].set()
            del queued_up[mid]
            non_queued_up.add(mid)
        else:
            LOGGER.warning(f"MID {mid} not found in queued_up")
    await sleep(0.5)

async def start_task_from_queued(task_type, limit, non_queued, queued):
    """Start tasks from queue based on limit."""
    target_lock = queue_dict_lock if task_type == 'dl' else upload_queue_lock
    async with target_lock:
        count = len(non_queued)
        if not queued:
            LOGGER.info(f"No {task_type} tasks in queue to start")
            return
        if limit == 0 or count < limit:
            to_start = len(queued) if limit == 0 else min(limit - count, len(queued))
            LOGGER.info(f"Starting {task_type} tasks - count: {count}, limit: {limit}, to_start: {to_start}")
            mids = list(queued.keys())[:to_start]
            for mid in mids:
                if task_type == 'up':
                    await start_up_from_queued(mid)
                else:
                    await start_dl_from_queued(mid)
            LOGGER.info(f"Released {task_type} tasks: {mids}")
        else:
            LOGGER.info(f"{task_type} limit reached: {count}/{limit}")

async def run_ffmpeg_manager():
    """Manage FFmpeg queue - 1 task at a time."""
    global active_ffmpeg
    while True:
        try:
            async with ffmpeg_queue_lock:
                if active_ffmpeg is None and ffmpeg_queue:
                    mid, (event, task_type, file_details) = next(iter(ffmpeg_queue.items()))
                    active_ffmpeg = mid
                    LOGGER.info(f"Starting FFmpeg for MID: {mid}, type: {task_type}")
                    del ffmpeg_queue[mid]
                    event.set()  # Signal VidEcxecutor or TaskListener to proceed
            await sleep(1)
        except Exception as e:
            LOGGER.error(f"FFmpeg manager error: {e}")
            await sleep(5)  # Prevent tight loop on error

async def run_upload_manager():
    """Manage upload queue - 1 task at a time."""
    while True:
        try:
            async with upload_queue_lock:
                up_count = len(non_queued_up)
                if up_count < 1 and queued_up:
                    mid = next(iter(queued_up))
                    await start_up_from_queued(mid)
                    LOGGER.info(f"Started upload for MID: {mid}")
            await sleep(1)
        except Exception as e:
            LOGGER.error(f"Upload manager error: {e}")
            await sleep(5)

async def start_from_queued():
    """Start tasks from download and upload queues."""
    all_limit = config_dict.get('QUEUE_ALL', 0)
    dl_limit = 0  # Unlimited downloads
    up_limit = 1  # 1 upload at a time
    LOGGER.info(f"start_from_queued called - all_limit: {all_limit}, dl_limit: {dl_limit}, up_limit: {up_limit}")
    async with queue_dict_lock:
        dl_count, up_count = len(non_queued_dl), len(non_queued_up)
        all_count = dl_count + up_count
        LOGGER.info(f"Queue stats - dl: {dl_count}, up: {up_count}, all: {all_count}")
    if all_limit > 0 and all_count >= all_limit:
        LOGGER.info("All limit reached, no tasks started")
        return
    await start_task_from_queued('up', up_limit, non_queued_up, queued_up)
    await start_task_from_queued('dl', dl_limit, non_queued_dl, queued_dl)

# Start managers
bot_loop.create_task(run_ffmpeg_manager())
bot_loop.create_task(run_upload_manager())