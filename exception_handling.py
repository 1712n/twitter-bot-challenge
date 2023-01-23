from functools import wraps
from pymongo.errors import PyMongoError, AutoReconnect
from WorkFlowRecorder import WorkFlowRecorder
from time import sleep
import os
from datetime import datetime, timedelta


def handle_mongo_exception(func: object) -> None:
    """Handles mongodb exceptions.\n
    In case AutoReconnect exception performs reconnect attempts
    every `attempt_interval` seconds during `reconnect_duration` minutes.
    """
    recorder = WorkFlowRecorder()
    attempt_interval = os.environ.get("MONGODB_RECONNECT_INTERVAL", 60)
    reconnect_duration = os.environ.get("MONGODB_RECONNECT_DURATION", 5)

    @wraps(func)
    def cover_mongo(*args, **kwargs) -> None:
        stop_time = datetime.utcnow() + timedelta(minutes=reconnect_duration)

        while datetime.utcnow() < stop_time:
            try:
                func(*args, **kwargs)
                break
            except AutoReconnect as ex:
                recorder.log_exception(ex)
                recorder.get_logged(
                    f"next attempt in {attempt_interval} sec\n"
                    f"reconnect attempts will be stopped at {stop_time.isoformat()}"
                )
                sleep(attempt_interval)

            except PyMongoError as ex:
                recorder.log_exception(ex)
                exit(1)

    return cover_mongo
