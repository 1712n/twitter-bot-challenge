import sys
from loguru import logger

LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SS}</green> | <level>{level: <8}</level> | {message}"

logger.remove()
logger.add(sys.stdout, colorize=True, format=LOG_FORMAT)
