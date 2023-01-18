from datetime import datetime

class WorkFlowRecorder():
    """Makes logs during a run"""

    def __init__(self, message_separator: str="\n") -> None:
        """"""
        self.indent = " "*(len(datetime.utcnow().isoformat())) + " | "
        self.message_separator = message_separator

    def get_logged(self, message: str) -> None:
        """Prints passed messages with a timestamp at that moment to stdout."""

        call_timestamp = datetime.utcnow()
        splited_message = message.split(sep=self.message_separator)

        print(f"{call_timestamp.isoformat()} | {splited_message[0]}")

        if len(splited_message) > 1:
            for i in range(1, len(splited_message)):
                print(f"{self.indent}{splited_message[i]}")
