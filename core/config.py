# For environment settings
import os
import pprint
from pathlib import Path
from dotenv import load_dotenv
# For mongodb connection string
from urllib.parse import quote_plus

# For settings from config.yaml
import confuse

# Logging
from core.log import log


class Settings:
    # Variables that defined by Customer/...
    __ATTRS = [
        "MONGODB_USER",
        "MONGODB_PASSWORD",
        "MONGO_DB_ADDRESS",
        "TW_ACCESS_TOKEN",
        "TW_ACCESS_TOKEN_SECRET",
        "TW_CONSUMER_KEY",
        "TW_CONSUMER_KEY_SECRET",
    ]
    # Local configuration file name
    CONFIG_FILE: str = 'config.yaml'

    # Make a connection string
    def make_connection_string(self) -> str:
        return (
            f"mongodb+srv://{quote_plus(self.MONGODB_USER)}:"
            f"{quote_plus(self.MONGODB_PASSWORD)}@"
            f"{self.MONGO_DB_ADDRESS}"
        )

    def __init__(self):
        # Check if environment variables exist
        try:
            # Setting parameters for mongodb
            log.logger.debug(f"Trying to getenv from environment")
            for attr in self.__ATTRS:
                attr_value = os.getenv(attr)
                if attr_value:
                    log.logger.debug(f"Found env variable: {attr}")
                    setattr(self, attr, attr_value)
                else:
                    log.logger.debug(f"Couldn't getenv variable: {attr}")
                    raise Exception(f"Couldn't getenv environment")
        except:
            # Not less than one variable doesn't exist in environment
            # Going to get variables from .env
            log.logger.debug('Environment variables not found. Setting env from .env file')
            env_path = Path(".") / ".env"
            load_dotenv(dotenv_path=env_path, verbose=True)

            for attr in self.__ATTRS:
                attr_value = os.getenv(attr)
                if attr_value:
                    log.logger.debug(f"Read from file, variable: {attr}")
                    setattr(self, attr, attr_value)
                else:
                    log.logger.critical(f"Couldn't getenv variable: {attr}")
                    raise Exception(f"Couldn't getenv environment")
        finally:
            # Building connection string for mongodb
            self.MONGODB_URI: str = self.make_connection_string()

        try:
            # Setting other params from local configuration file for the project
            conf = confuse.Configuration(__name__)
            conf.set_file(self.CONFIG_FILE)
            # Database name inside mongodb
            self.MONGODB_DBNAME = conf['mongodb']['dbname'].get()
            if self.MONGODB_DBNAME:
                log.logger.debug(f"Found: MONGODB_DBNAME: {self.MONGODB_DBNAME}")
            # Mongodb collections names
            self.PAIRS_NAME = conf['mongodb']['pairs'].get()
            self.POSTS_NAME = conf['mongodb']['posts'].get()

            # Mongodb timeout serverSelectionTimeoutMS
            self.SERVERSELECTIONTIMEOUTMS: str = \
                conf['mongodb']['serverselectiontimeoutms'].get()
        except Exception as e:
                log.logger.critical(
                    f"Failed configure from file: {self.CONFIG_FILE}: {e}"
                )


settings = Settings()
