from pathlib import Path
from logging import config, getLogger, Logger


def get_logger(config_path: Path = Path(__file__).parent / 'logging.conf') -> Logger:
    '''Return a Logger instance appropriate for using in a Tap or a Target.'''
    # See
    # https://docs.python.org/3.5/library/logging.config.html#logging.config.fileConfig
    # for a discussion of why or why not to set disable_existing_loggers
    # to False. The long and short of it is that if you don't set it to
    # False it ruins external module's abilities to use the logging
    # facility.
    config.fileConfig(config_path, disable_existing_loggers=False)
    return getLogger()
