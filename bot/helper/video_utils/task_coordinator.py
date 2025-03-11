from bot import LOGGER, task_dict_lock, queue_dict_lock, queue_dl, queue_up, multi_tags, bot_loop
from bot.helper.mirror_utils.status_utils.mirror_status import MirrorStatus
from bot.helper.ext_utils.bot_utils import new_task
from asyncio import sleep

class TaskCoordinator:
    @staticmethod
    @new_task
    async def coordinate_error(listener, error_message):
        """Coordinates TaskListener on error without direct modification."""
        LOGGER.error(f"Task {listener.mid} error: {error_message}")
        async with task_dict_lock:
            if listener.mid in task_dict:
                task_dict[listener.mid].status = MirrorStatus.STATUS_FAILED
                task_dict[listener.mid]._set_status(error_message)
        listener.event.set()  # Signal completion to TaskListener
        await TaskCoordinator._trigger_next_task(listener)

    @staticmethod
    @new_task
    async def coordinate_success(listener, up_path):
        """Coordinates TaskListener on success to ensure upload proceeds."""
        LOGGER.info(f"Task {listener.mid} completed processing, path: {up_path}")
        async with task_dict_lock:
            if listener.mid in task_dict:
                task_dict[listener.mid].status = MirrorStatus.STATUS_UPLOADING
        listener._path = up_path  # Update path for upload
        listener.event.set()  # Signal TaskListener to proceed
        await TaskCoordinator._trigger_next_task(listener)

    @staticmethod
    async def _trigger_next_task(listener):
        """Triggers the next task in the queue using TaskListener's existing logic."""
        async with queue_dict_lock:
            LOGGER.debug(f"Checking queue for task {listener.mid}: dl={len(queue_dl)}, up={len(queue_up)}")
            if listener.mid in queue_dl:
                dl = queue_dl[listener.mid]
                del queue_dl[listener.mid]
                await dl.event.wait()
                if dl.error_message:
                    LOGGER.error(f"Queued download failed: {dl.error_message}")
                    async with task_dict_lock:
                        if listener.mid in task_dict:
                            task_dict[listener.mid].status = MirrorStatus.STATUS_FAILED
                            task_dict[listener.mid]._set_status(dl.error_message)
                else:
                    listener._path = dl.path()
                    # TaskListener's onDownloadComplete handles naturally
            elif listener.mid in queue_up:
                ul = queue_up[listener.mid]
                del queue_up[listener.mid]
                await ul.event.wait()
                if ul.error_message:
                    LOGGER.error(f"Queued upload failed: {ul.error_message}")
                    async with task_dict_lock:
                        if listener.mid in task_dict:  # Fixed typo
                            task_dict[listener.mid].status = MirrorStatus.STATUS_FAILED
                            task_dict[listener.mid]._set_status(ul.error_message)
                # TaskListener's onUploadComplete handles naturally
            LOGGER.debug(f"Queue state after task {listener.mid}: dl={len(queue_dl)}, up={len(queue_up)}")

    @staticmethod
    @new_task
    async def monitor_queue_health():
        """Monitors and cleans queue to prevent stalling, runs continuously."""
        LOGGER.info("Starting queue health monitor within TaskCoordinator")
        check_count = 0
        while True:
            async with queue_dict_lock:
                if check_count % 5 == 0:  # Log every 5th check (5 minutes)
                    LOGGER.info(f"Queue health check: dl={len(queue_dl)}, up={len(queue_up)}")
                check_count += 1
                for mid, dl_task in list(queue_dl.items()):
                    if dl_task.error_message:
                        LOGGER.error(f"Removing stalled download task {mid}: {dl_task.error_message}")
                        del queue_dl[mid]
                        async with task_dict_lock:
                            if mid in task_dict:
                                task_dict[mid].status = MirrorStatus.STATUS_FAILED
                                task_dict[mid]._set_status(dl_task.error_message)
                        dl_task.event.set()
                for mid, up_task in list(queue_up.items()):
                    if up_task.error_message:
                        LOGGER.error(f"Removing stalled upload task {mid}: {up_task.error_message}")
                        del queue_up[mid]
                        async with task_dict_lock:
                            if mid in task_dict:
                                task_dict[mid].status = MirrorStatus.STATUS_FAILED
                                task_dict[mid]._set_status(up_task.error_message)
                        up_task.event.set()
            await sleep(60)  # Check every 60 seconds

# Start the queue health monitor when the module is imported
bot_loop.create_task(monitor_queue_health())