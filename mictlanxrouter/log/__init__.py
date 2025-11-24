import os, sys, logging, json, threading
from mictlanxrouter import config
from logging.handlers import TimedRotatingFileHandler
from option import NONE, Option

class DumbLogger(object):
    """
    A dummy logger class that provides no-op (no operation) methods for debug, info, and error logging.

    This is useful as a fallback logger to avoid breaking code that expects a logger but doesn't need actual logging.
    """

    def debug(self, **kargs):
        """
        Dummy debug method that performs no action.
        """
        return

    def info(self, **kargs):
        """
        Dummy info method that performs no action.
        """
        return

    def error(self, **kargs):
        """
        Dummy error method that performs no action.
        """
        return


class JsonFormatter(logging.Formatter):
    """
    Custom JSON formatter for log records.

    Formats each log record into a JSON object, including metadata like timestamp, log level,
    logger name, and thread name. If the message is a dictionary, it merges it into the log record.
    """

    def format(self, record):
        """
        Format the log record as a JSON string.

        Args:
            record (LogRecord): The log record instance.

        Returns:
            str: A JSON-formatted log string.
        """
        thread_id = threading.current_thread().getName()
        log_data = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'logger_name': record.name,
            "thread_name": thread_id
        }
        if isinstance(record.msg, dict):
            log_data.update(record.msg)
        else:
            log_data['message'] = record.getMessage()

        return json.dumps(log_data, indent=4) + "\n"

def _normalize_event_payload(msg, default_event: str) -> dict:
    """
    Ensure that the payload:

    - Always has an EVENT key.
    - EVENT is uppercase.
    - If 'event' exists, it's normalized to 'EVENT'.
    """

    # Si no es dict, lo envolvemos
    if not isinstance(msg, dict):
        return {
            "EVENT": default_event,
            "message": str(msg),
        }

    data = dict(msg)

    if "EVENT" in data:
        data["EVENT"] = str(data["EVENT"]).upper()
    elif "event" in data:
        data["EVENT"] = str(data.pop("event")).upper()
    else:
        data["EVENT"] = default_event

    return data


class Log(logging.Logger):
    """
    Custom logger class that supports JSON formatting, stream output to console, rotating file output,
    and error-level file separation. Uses filters for log level control.

    Inherits from the built-in `logging.Logger`.
    """

    def __init__(self,
                formatter:               logging.Formatter = JsonFormatter(),
                name:                    str = "mictlanx-router",
                level:                   int = getattr(logging, config.MICTLANX_ROUTER_LOG_LEVEL.upper(), logging.DEBUG),
                path:                    str =  config.MICTLANX_ROUTER_LOG_PATH,
                disabled:                bool = False,
                console_handler_filter = lambda record: record.levelno == logging.DEBUG,
                file_handler_filter =    lambda record: record.levelno == logging.INFO,
                console_handler_level:   int = logging.DEBUG,
                file_handler_level:      int = logging.INFO,
                error_log:               bool = config.MICTLANX_ROUTER_LOG_ERROR_FILE,
                filename:                Option[str] = NONE,
                output_path:             Option[str] = NONE,
                error_output_path:       Option[str] = NONE,
                to_file:                 bool = config.MICTLANX_ROUTER_LOG_TO_FILE,
                when:                    str =  config.MICTLANX_ROUTER_LOG_ROTATION_WHEN,
                interval:                int =  config.MICTLANX_ROUTER_LOG_ROTATION_INTERVAL,
                ):
        """
        Initialize the logger with optional console and file handlers.

        Args:
            formatter (logging.Formatter): Formatter to use for all handlers.
            name (str): Name of the logger.
            level (int): Logging level for the logger.
            path (str): Directory path where logs will be stored.
            disabled (bool): If True, disables logging handlers.
            console_handler_filter (callable): Filter for console logs.
            file_handler_filter (callable): Filter for file logs.
            console_handler_level (int): Minimum level for console logs.
            file_handler_level (int): Minimum level for file logs.
            error_log (bool): Whether to enable separate error log file.
            filename (Option[str]): Optional filename base for logs.
            output_path (Option[str]): Path for general log output.
            error_output_path (Option[str]): Path for error log output.
            create_folder (bool): If True, creates path if it does not exist.
            to_file (bool): If True, enables file logging.
            when (str): TimedRotatingFileHandler `when` parameter (e.g., "m" for minutes).
            interval (int): TimedRotatingFileHandler `interval` parameter.
        """
        super().__init__(name, level)

        if not os.path.exists(path):
            os.makedirs(path)

        if not disabled:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(console_handler_level)
            console_handler.addFilter(console_handler_filter)
            self.addHandler(console_handler)

            if to_file:
                # Rotating file handler
                file_handler = TimedRotatingFileHandler(
                    filename=output_path.unwrap_or(f"{path}/{filename.unwrap_or(name)}"),
                    when=when,
                    interval=interval
                )
                file_handler.setFormatter(formatter)
                file_handler.setLevel(file_handler_level)
                file_handler.addFilter(file_handler_filter)
                self.addHandler(file_handler)

            if error_log:
                # Error file handler
                error_file_handler = logging.FileHandler(
                    filename=error_output_path.unwrap_or(f"{path}/{filename.unwrap_or(name)}.error")
                )
                error_file_handler.setFormatter(formatter)
                error_file_handler.setLevel(logging.ERROR)
                error_file_handler.addFilter(lambda record: record.levelno == logging.ERROR)
                self.addHandler(error_file_handler)


    def debug(self, msg, *args, **kwargs):
        payload = _normalize_event_payload(msg, default_event="LOG.DEBUG")
        super().debug(payload, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        payload = _normalize_event_payload(msg, default_event="LOG.INFO")
        super().info(payload, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        payload = _normalize_event_payload(msg, default_event="LOG.ERROR")
        super().error(payload, *args, **kwargs)