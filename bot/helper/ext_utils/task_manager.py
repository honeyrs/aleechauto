from aiofiles.os import path as aiopath
from asyncio import Event, sleep
from os import path as ospath

from bot import config_dict, queued_dl, queued_up, non_queued_up, non_queued_dl, queue_dict_lock, LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async, presuf_remname_name, is_premium_user
from bot.helper.ext_utils.files_utils import get_base_name, check_storage_threshold
from bot.helper.ext_utils.links_utils import is_gdrive_id, is_mega_link
from bot.helper.mirror_utils.gdrive_utlis.search import gdSearch


async def stop_duplicate_check(listener):
    if (isinstance(listener.upDest, int) or listener.isLeech or listener.select or listener.sameDir
        or not is_gdrive_id(listener.upDest) or not listener.stopDuplicate):
        return None, ''
    name = listener.name
    LOGGER.info('Checking File/Folder if already in Drive: %s', name)
    if listener.compress:
        name = f'{name}.zip'
    elif listener.extract:
        try:
            name = get_base_name(name)
        except:
            name = None
    if name:
        if not listener.isRename and await aiopath.isfile(ospath.join(listener.dir, name)):
            name = presuf_remname_name(listener.user_dict, name)
        count, file = await sync_to_async(gdSearch(stopDup=True, noMulti=listener.isClone).drive_list, name, listener.upDest, listener.user_id)
        if count:
            return file, name
    LOGGER.info('Checking duplicate is passed...')
    return None, ''


async def check_limits_size(listener, size, playlist=False, play_count=False):
    msgerr = None
    max_pyt, megadl, torddl, zuzdl, leechdl, storage = (config_dict['MAX_YTPLAYLIST'], config_dict['MEGA_LIMIT'], config_dict['TORRENT_DIRECT_LIMIT'],
                                                        config_dict['ZIP_UNZIP_LIMIT'], config_dict['LEECH_LIMIT'], config_dict['STORAGE_THRESHOLD'])
    if config_dict['PREMIUM_MODE'] and not is_premium_user(listener.user_id):
        mdl = torddl = zuzdl = leechdl = config_dict['NONPREMIUM_LIMIT']
        megadl = min(megadl, mdl)
        max_pyt = 10

    arch = any([listener.compress, listener.isLeech, listener.extract])
    if torddl and not arch and size >= torddl * 1024**3:
        msgerr = f'Torrent/direct limit is {torddl}GB'
    elif zuzdl and any([listener.compress, listener.extract]) and size >= zuzdl * 1024**3:
        msgerr = f'Zip/Unzip limit is {zuzdl}GB'
    elif leechdl and listener.isLeech and size >= leechdl * 1024**3:
        msgerr = f'Leech limit is {leechdl}GB'
    if is_mega_link(listener.link) and megadl and size >= megadl * 1024**3:
        msgerr = f'Mega limit is {megadl}GB'
    if max_pyt and playlist and (play_count > max_pyt):
        msgerr = f'Only {max_pyt} playlist allowed. Current playlist is {play_count}.'
    if storage and not await check_storage_threshold(size, arch):
        msgerr = f'Need {storage}GB free storage'
    return msgerr


async def check_running_tasks(mid: int, state='dl'):
    all_limit = config_dict['QUEUE_ALL']
    state_limit = (config_dict['QUEUE_DOWNLOAD'] if state == 'dl' else config_dict['QUEUE_UPLOAD'])
    event = None
    is_over_limit = False
    if all_limit or state_limit:
        async with queue_dict_lock:
            if state == 'up' and mid in non_queued_dl:
                non_queued_dl.remove(mid)
            dl_count, up_count = len(non_queued_dl), len(non_queued_up)
            is_over_limit = (all_limit and dl_count + up_count >= all_limit and (not state_limit or dl_count >= state_limit)) or (state_limit and dl_count >= state_limit)
            if is_over_limit:
                event = Event()
                if state == 'dl':
                    queued_dl[mid] = event
                else:
                    queued_up[mid] = event

    return is_over_limit, event


async def start_dl_from_queued(mid: int):
    queued_dl[mid].set()
    del queued_dl[mid]
    await sleep(0.7)


async def start_up_from_queued(mid: int):
    queued_up[mid].set()
    del queued_up[mid]
    await sleep(0.7)


async def start_task_from_queued(task_type, limit, non_queued, queued):
    async with queue_dict_lock:
        count = len(non_queued)
        if queued and count < limit:
            f_tasks = limit - count
            for index, mid in enumerate(list(queued), start=1):
                if task_type == 'up':
                    await start_up_from_queued(mid)
                else:
                    await start_dl_from_queued(mid)
                if index == f_tasks:
                    break

async def start_from_queued():
    all_limit, dl_limit, up_limit = config_dict['QUEUE_ALL'], config_dict['QUEUE_DOWNLOAD'], config_dict['QUEUE_UPLOAD']

    if all_limit:
        async with queue_dict_lock:
            dl, up = len(non_queued_dl), len(non_queued_up)
            all_ = dl + up
            if all_ < all_limit:
                if queued_up and (not up_limit or up < up_limit):
                    await start_task_from_queued('up', min(all_limit - all_, up_limit or all_limit), non_queued_up, queued_up)
                if queued_dl and (not dl_limit or dl < dl_limit):
                    await start_task_from_queued('dl', min(all_limit - all_, dl_limit or all_limit), non_queued_dl, queued_dl)
        return

    if up_limit:
        await start_task_from_queued('up', up_limit, non_queued_up, queued_up)
    else:
        await start_task_from_queued('up', float('inf'), non_queued_up, queued_up)

    if dl_limit:
        await start_task_from_queued('dl', dl_limit, non_queued_dl, queued_dl)
    else:
        await start_task_from_queued('dl', float('inf'), non_queued_dl, queued_dl)
