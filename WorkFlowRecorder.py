from datetime import datetime

class WorkFlowRecorder():
    """Makes logs during a run"""

    def __init__(self, message_separator: str="\n") -> None:
        """"""
        self.delimiter = " | "
        self.indent = " "*(len(datetime.utcnow().isoformat())) + self.delimiter
        self.message_separator = message_separator

    def get_logged(self, message: str, error_flag: bool=False) -> None:
        """Prints passed messages with a timestamp at that moment to stdout."""

        call_timestamp = datetime.utcnow()
        indent = self.indent
        delimiter = self.delimiter
        splited_message = message.split(sep=self.message_separator)

        if error_flag:
            delimiter = " -> "
            indent = " "*(len(datetime.utcnow().isoformat())) + delimiter
            print(f"{call_timestamp.isoformat()} ! ERROR")

        if len(splited_message) > 1:
            print(f"{call_timestamp.isoformat()}{delimiter}{splited_message[0]}")
            for i in range(1, len(splited_message)):
                print(f"{indent}{splited_message[i]}")
        else:
            print(f"{call_timestamp.isoformat()}{delimiter}{splited_message[0]}")
