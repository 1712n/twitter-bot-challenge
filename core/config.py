# For environment settings
import os
from pathlib import Path
from dotenv import load_dotenv
# For mongodb connection string
from urllib.parse import quote_plus

# Logging
from core.log import log


class Settings:
    ATTRS = [
        "MONGODB_USER",
        "MONGODB_PASSWORD",
        "MONGO_DB_ADDRESS",
        "TW_ACCESS_TOKEN",
        "TW_ACCESS_TOKEN_SECRET",
        "TW_CONSUMER_KEY",
        "TW_CONSUMER_KEY_SECRET",
    ]

    def __init__(self):
        # Check if environment variables exist
        try:
            # Setting parameters for mongodb
            log.logger.debug(f"Trying to getenv from environment")
            for attr in self.ATTRS:
                attr_value = os.getenv(attr)
                if attr_value:
                    log.logger.debug(f"Found env variable: {attr}")
                    setattr(self, attr, attr_value)
                else:
                    log.logger.debug(f"Couldn't getenv variable: {attr}")
                    raise Exception(f"Couldn't getenv environment")
        except:
            log.logger.debug('Environment variables not found. Setting env from .env')
            env_path = Path(".") / ".env"
            load_dotenv(dotenv_path=env_path)

            for attr in self.ATTRS:
                attr_value = os.getenv(attr)
                if attr_value:
                    log.logger.debug(f"Read from file, variable: {attr}")
                    setattr(self, attr, attr_value)
                else:
                    log.logger.debug(f"Couldn't getenv variable: {attr}")
                    raise Exception(f"Couldn't getenv environment")

            self.MONGODB_URI: str = (
                f"mongodb+srv://{quote_plus(self.MONGODB_USER)}:"
                f"{quote_plus(self.MONGODB_PASSWORD)}@"
                f"{self.MONGO_DB_ADDRESS}"
            )


settings = Settings()
