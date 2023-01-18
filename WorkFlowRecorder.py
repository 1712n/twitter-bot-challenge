from datetime import datetime

class WorkFlowRecorder():
    """Makes logs in the meantime of bot execution"""

    def __init__(self) -> None:
        pass

    def get_logged(self, message: str) -> None:
        """Prints passed messages with a timestamp at that moment to stdout."""

        print(f"{datetime.utcnow().isoformat()} | {message}")