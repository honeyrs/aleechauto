from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from base64 import b64decode
from math import ceil, floor
from mimetypes import guess_type
from re import search as re_search

from bot import LOGGER
from bot.helper.ext_utils.exceptions import FIleNotFound, InvalidHash
from bot.helper.stream_utils.custom_dl import ByteStreamer
from bot.helper.stream_utils.render_template import render_page

client_cache = {}

routes = web.RouteTableDef()


@routes.get('/', allow_head=True)
async def root_handler(_):
    try:
        return web.Response(text=await render_page(None, None, True), content_type='text/html')
    except Exception as e:
        LOGGER.error(e, exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r'/stream/{path:\S+}', allow_head=True)
async def ddls_handler(request: web.Request):
    try:
        if path := request.match_info['path']:
            mime_type = request.rel_url.query.get('type')
            try:
                path = b64decode(path).decode('utf8')
            except:
                pass
            return web.Response(text=await render_page(None, mime_type, ddl=path), content_type='text/html')
        raise FIleNotFound
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except Exception as e:
        LOGGER.error(e, exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r'/watch/{path:\S+}', allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info['path']
        if match := re_search(r'^([a-zA-Z0-9_-]{6})(\d+)$', path):
            secure_hash = match.group(1)
            message_id = int(match.group(2))
        else:
            message_id = int(re_search(r'(\d+)(?:\/\S+)?', path).group(1))
            secure_hash = request.rel_url.query.get('hash')
        return web.Response(text=await render_page(message_id, secure_hash), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        LOGGER.error(e, exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r'/{path:\S+}', allow_head=True)
async def download_handler(request: web.Request):
    try:
        path = request.match_info['path']
        if match := re_search(r'^([a-zA-Z0-9_-]{6})(\d+)$', path):
            secure_hash = match.group(1)
            message_id = int(match.group(2))
        else:
            message_id = int(re_search(r'(\d+)(?:\/\S+)?', path).group(1))
            secure_hash = request.rel_url.query.get('hash')
        return await media_streamer(request, message_id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        LOGGER.error(e, exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))


async def media_streamer(request: web.Request, message_id: int, secure_hash: str):
    if not client_cache.get('client'):
        client_cache['client'] = ByteStreamer()
    stream = client_cache['client']
    file_id = await stream.get_file_properties(message_id)
    if file_id.unique_id[:6] != secure_hash:
        LOGGER.debug('Invalid hash for message with ID %s', message_id)
        raise InvalidHash
    range_header = request.headers.get('Range', 0)
    file_size = file_id.file_size
    if range_header:
        from_bytes, until_bytes = range_header.replace('bytes=', '').split('-')
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = request.http_range.stop or file_size - 1
    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(status=416,
                            body='416: Range not satisfiable',
                            headers={'Content-Range': f'bytes */{file_size}'})

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1
    req_length = until_bytes - from_bytes + 1
    part_count = ceil(until_bytes / chunk_size) - floor(offset / chunk_size)
    body = stream.yield_file(file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size)
    mime_type, file_name = file_id.mime_type, file_id.file_name
    disposition = 'attachment'

    if not mime_type:
        mime_type = guess_type(file_name)[0] or 'application/octet-stream'

    if any(x in mime_type for x in ['video/', 'audio/', '/html']):
        disposition = 'inline'

    return web.Response(status=206 if range_header else 200,
                        body=body,
                        headers={'Content-Type': mime_type,
                                 'Content-Range': f'bytes {from_bytes}-{until_bytes}/{file_size}',
                                 'Content-Length': str(req_length),
                                 'Content-Disposition': f'{disposition}; filename="{file_name}"',
                                 'Accept-Ranges': 'bytes'})
