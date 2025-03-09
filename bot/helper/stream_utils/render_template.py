from aiofiles import open as aiopen
from aiohttp import ClientSession
from os import path as ospath
from time import time
from urllib.parse import urljoin

from bot import config_dict, botStartTime, LOGGER
from bot.helper.ext_utils.exceptions import InvalidHash, FIleNotFound
from bot.helper.ext_utils.links_utils import get_url_name, is_url
from bot.helper.ext_utils.status_utils import get_readable_file_size, get_readable_time
from bot.helper.stream_utils.file_properties import get_file_ids


async def render_page(message_id, secure_hash, is_home=False, ddl=''):
    tpath = ospath.join('bot', 'helper', 'stream_utils', 'template')
    if is_home:
        channel = config_dict['CHANNEL_USERNAME']
        info = (f"<h1 style='text-align: center'><a href='https://t.me/{channel}'>@{channel}</a></h1><br>"
                f"<h2 style='text-align: center'>Up Time: {get_readable_time(time() - botStartTime)}</h2>")
        async with aiopen(ospath.join(tpath, 'home.html'), 'r') as f:
            html = (await f.read()).replace("<!-- Print -->", info)
    else:
        if ddl:
            file_data = type('file_id', (object, ), {'file_name': get_url_name(ddl), 'mime_type': secure_hash or 'video'})
            src = ddl if is_url(ddl) else urljoin(config_dict['RCLONE_SERVE_URL'], ddl)
        else:
            file_data = await get_file_ids(int(message_id))
            if file_data.unique_id[:6] != secure_hash:
                LOGGER.info('Link hash: %s - %s', secure_hash, file_data.unique_id[:6])
                LOGGER.info('Invalid hash for message with - ID %s', message_id)
                raise InvalidHash
            src = urljoin(config_dict['STREAM_BASE_URL'], f'{secure_hash}{message_id}')

        filename = file_data.file_name
        match file_data.mime_type.split('/')[0].strip():
            case 'video' as tag:
                async with aiopen(ospath.join(tpath, 'req.html')) as r:
                    heading = f'Watch: {filename}'
                    html = (await r.read()).replace('tag', tag) % (heading, filename, src)
            case 'audio' as tag:
                async with aiopen(ospath.join(tpath, 'req.html')) as r:
                    heading = f'Listen {filename}'
                    html = (await r.read()).replace('tag', tag) % (heading, filename, src)
            case _:
                if ddl:
                    raise FIleNotFound
                async with aiopen(ospath.join(tpath, 'dl.html')) as r, ClientSession() as s, s.get(src) as u:
                    heading = f'Download: {filename}'
                    file_size = get_readable_file_size(int(u.headers.get('Content-Length')))
                    html = (await r.read()) % (heading, filename, src, file_size)
    return html
