import logging
import sys


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configures a centralized logger for the anaplan_orm namespace.

    Args:
        level (int): The logging severity level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: The configured root logger for the library.
    """
    logger = logging.getLogger("anaplan_orm")
    logger.setLevel(level)

    # Guardrail: Only add a handler if one doesn't already exist.
    if not logger.handlers:
        # Create a StreamHandler to push logs to standard output
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
