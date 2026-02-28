import fcntl
from contextlib import contextmanager
from pathlib import Path


def should_run_background_jobs(app):
    cached = app.extensions.get("background_jobs_leader")
    if cached is not None:
        return bool(cached)

    lock_path = Path(app.config["BACKGROUND_JOBS_LOCK_FILE"])
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        app.extensions["background_jobs_leader"] = False
        app.logger.info("Background jobs desativados neste processo; outro worker possui o lock.")
        return False

    app.extensions["background_jobs_leader"] = True
    app.extensions["background_jobs_lock_handle"] = handle
    app.logger.info("Background jobs habilitados neste processo.")
    return True


@contextmanager
def exclusive_file_lock(lock_path, blocking=True):
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    flags = fcntl.LOCK_EX
    if not blocking:
        flags |= fcntl.LOCK_NB
    try:
        fcntl.flock(handle.fileno(), flags)
        yield handle
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
