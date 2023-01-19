# For logging
import logging
import logging.config
# For loading logging config
import yaml

from core.config import APP_NAME
from core.config import CONFIG_FILE
from core.config import DEFAULT_LEVEL

try:
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(APP_NAME)
    local_logger = logging.getLogger(f"{APP_NAME}.{__name__}")
    local_logger.debug('Logging config loaded')
# Failed configuration from file, setting defaults
except Exception as e:
    logging.basicConfig(level=DEFAULT_LEVEL)
    logger = logging.getLogger(APP_NAME)
    local_logger = logging.getLogger(f"{APP_NAME}.{__name__}")
    local_logger.warning(f"Logging set from default. Failed to load logging config: {e}")

local_logger.debug('Logging started')
