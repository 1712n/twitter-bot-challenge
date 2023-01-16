# For logging
import logging
import logging.config
# For loading logging config
import yaml


CONFIG_FILE = 'logging_config.yaml'
DEFAULT_LEVEL = logging.WARNING


class Logs:
    def __init__(self):
        # Trying to load yaml config for loggin
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            self.logger = logging.getLogger(__name__)
            self.logger.debug('Logging config loaded')
        # Failed configuration from file, setting defaults
        except Exception as e:
            logging.basicConfig(level=DEFAULT_LEVEL)
            self.logger = logging.getLogger(__name__)
            self.logger.warning(f"Logging set from default. Failed to load logging config: {e}")

        self.logger.debug('Logging started')


log = Logs()
