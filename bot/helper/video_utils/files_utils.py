import os
from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from bot import LOGGER
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec
from aioshutil import rmtree

async def split_file(path, size):
    """Splits a file into parts smaller than the specified size."""
    LOGGER.info(f"Splitting file: {path} with size limit: {size}")
    if not await sync_to_async(os.path.isfile, path):
        LOGGER.error(f"Cannot split {path}: not a file")
        return [path]
    file_size = await sync_to_async(os.path.getsize, path)
    if file_size <= size:
        return [path]
    cmd = ['ffmpeg', '-i', path, '-fs', str(size), '-map', '0', '-c', 'copy', '-y', f'{path}.split-%03d.mkv']
    process = await create_subprocess_exec(*cmd, stderr=PIPE)
    await process.wait()
    if process.returncode != 0:
        LOGGER.error(f"Split failed for {path}: {(await process.stderr.read()).decode()}")
        return [path]
    parts = [f'{path}.split-{i:03d}.mkv' for i in range(1000) if await sync_to_async(os.path.exists, f'{path}.split-{i:03d}.mkv')]
    LOGGER.info(f"Split {path} into {len(parts)} parts")
    return parts

async def createThumb(path):
    """Creates a thumbnail from a video file."""
    LOGGER.info(f"Creating thumbnail for: {path}")
    output = os.path.splitext(path)[0] + "_thumb.jpg"
    cmd = ['ffmpeg', '-i', path, '-vf', 'thumbnail', '-frames:v', '1', output, '-y']
    process = await create_subprocess_exec(*cmd, stderr=PIPE)
    await process.wait()
    if process.returncode == 0 and await sync_to_async(os.path.exists, output):
        LOGGER.info(f"Thumbnail created: {output}")
        return output
    LOGGER.error(f"Thumbnail creation failed for {path}: {(await process.stderr.read()).decode()}")
    return None

async def get_path_size(path):
    """Calculates total size of a path (file or directory)."""
    if await sync_to_async(os.path.isfile, path):
        return await sync_to_async(os.path.getsize, path)
    total_size = 0
    for dirpath, _, filenames in await sync_to_async(os.walk, path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += await sync_to_async(os.path.getsize, file_path)
    LOGGER.debug(f"Path size for {path}: {total_size}")
    return total_size

async def clean_target(path):
    """Removes a file or directory."""
    try:
        if await sync_to_async(os.path.isfile, path):
            await sync_to_async(os.remove, path)
        elif await sync_to_async(os.path.isdir, path):
            await sync_to_async(os.rmdir, path) if not os.listdir(path) else await rmtree(path)
        LOGGER.debug(f"Cleaned target: {path}")
    except Exception as e:
        LOGGER.error(f"Error cleaning target {path}: {e}")