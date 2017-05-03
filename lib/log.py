"""offer log interface according Baidu log level"""
import logging
import logging.handlers
import os
import sys

# Baidu defined log level & name
BD_LOGNAMELVL = {
    'DEBUG': logging.DEBUG,
    'TRACE': logging.INFO,
    'NOTICE': logging.WARNING,
    'WARNING': logging.ERROR,
    'FATAL': logging.CRITICAL
}
# Global logging produced a unique bd_logger
bd_logger = logging.getLogger('bd_log')


class WfLogFilter(logging.Filter):
    """
    Filter out WARNING and FATAL log
    """
    def filter(self, record):
        return 0 if record.levelno >= logging.ERROR else 1


def init(level, normal_log_path, wf_log_path=None, auto_rotate=False, backup_days=7):
    """
    initialization function
    level: the level of log, it can be:
        'DEBUG'
        'TRACE'
        'NOTICE'
        'WARNING'
        'FATAL'
    normal_log_path: the path of log file
    wf_log_path: the path of warning and fatal log file
    """
    # Set log level
    for name in BD_LOGNAMELVL:
        logging.addLevelName(BD_LOGNAMELVL[name], name)
    levelno = BD_LOGNAMELVL[level]
    bd_logger.setLevel(levelno)
    bd_fmt = logging.Formatter('%(levelname)s: %(asctime)s: %(thread)d [%(bd_filename)s:%(bd_lineno)d]'
                               '[%(bd_funcName)s] %(message)s')
    # Set normal log file
    if auto_rotate:
        backup_count = backup_days * 24
        normal_hdlr = logging.handlers.TimedRotatingFileHandler(normal_log_path, when='h', interval=1,
                                                                backupCount=backup_count)
    else:
        normal_hdlr = logging.handlers.WatchedFileHandler(normal_log_path)
    normal_hdlr.setFormatter(bd_fmt)
    normal_hdlr.setLevel(levelno)

    # Make WF log emit to a separate file
    if wf_log_path is not None:
        normal_hdlr.addFilter(WfLogFilter())
        # set wf log file
        if auto_rotate:
            wf_hdlr = logging.handlers.TimedRotatingFileHandler(wf_log_path, when='h', interval=1,
                                                                backupCount=backup_count)
        else:
            wf_hdlr = logging.handlers.WatchedFileHandler(wf_log_path)
        wf_hdlr.setFormatter(bd_fmt)
        wf_hdlr.setLevel(levelno if levelno > logging.ERROR else logging.ERROR)
        bd_logger.addHandler(wf_hdlr)
    bd_logger.addHandler(normal_hdlr)


def debug(msg, *args):
    """
    DEBUG level log
    """
    bd_logger.debug(msg, *args, extra=_extra())


def trace(msg, *args):
    """
    TRACE level log
    """
    bd_logger.info(msg, *args, extra=_extra())


def notice(msg, *args):
    """
    NOTICE level log
    """
    bd_logger.warning(msg, *args, extra=_extra())


def warning(msg, *args):
    """
    WARNING level log
    """
    bd_logger.error(msg, *args, extra=_extra())


def fatal(msg, *args):
    """
    FATAL level log
    """
    bd_logger.critical(msg, *args, extra=_extra())


def _extra():
    """
    add file name, file number and function name
    """
    file_path = sys._getframe(2).f_code.co_filename
    dir_path, base_name = os.path.split(file_path)
    super_dir, current_dir = os.path.split(dir_path)
    file_name = os.path.join(current_dir, base_name)
    return {'bd_filename': file_name,
            'bd_lineno': sys._getframe(2).f_lineno,
            'bd_funcName': sys._getframe(2).f_code.co_name}
