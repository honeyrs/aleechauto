from aiohttp import web

from bot import config_dict, LOGGER
from bot.helper.stream_utils.stream_routes import routes


def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(routes)
    return web_app


server = web.AppRunner(web_server())


async def start_server():
    if config_dict['ENABLE_STREAM_LINK'] and config_dict['STREAM_BASE_URL'] and config_dict['STREAM_PORT'] and config_dict['LEECH_LOG']:
        port = config_dict['STREAM_PORT']
        await server.cleanup()
        LOGGER.info('Initalizing web stream with %s', port)
        await server.setup()
        await web.TCPSite(server, '0.0.0.0', 40065).start()
