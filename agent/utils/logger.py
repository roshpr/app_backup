import logging
logging.basicConfig()
loggers = {}

class BackupLogger:

    def __init__(self, name=None):
        logger = logging.getLogger(name)
        self._logger = logger

    def info(self, message, *args, **kwargs):
        self._logger.info("%s %s %s", message, args, kwargs)

    def debug(self, message, *args, **kwargs):
        self._logger.debug("%s %s %s", message, args, kwargs)

    def warn(self,  message, *args, **kwargs):
        self._logger.warning("%s %s %s", message, args, kwargs)

    def severe(self,  message, *args, **kwargs):
        self._logger.error("%s %s %s", message, args, kwargs)

    def exception(self, e, *args,**kwargs):
        self._logger.exception("%s %s %s", e, args, kwargs)





def get_logger(name=None):

    if name not in loggers:
        loggers[name] = BackupLogger(name)

    return loggers[name]