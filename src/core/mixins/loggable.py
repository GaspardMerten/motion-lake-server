#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.
import logging
import uuid


class LoggableComponent:
    """
    A component that provides logging capabilities to its subclasses
    """

    def __init__(self):
        """
        Initialize the LoggableComponent with a unique internal UUID and a logger
        """
        self.internal_uuid = str(uuid.uuid4())[0:8]
        self._logger = logging.getLogger(
            self.__class__.__name__ + " - " + self.internal_uuid
        )

    def log(self, message: str) -> None:
        """
        Log a message with the logger
        :param message: The message to log
        :return: None
        """
        self._logger.info(message)

    def log_error(self, message: str) -> None:
        """
        Log an error message with the logger
        :param message:  The error message to log
        :return: None
        """
        self._logger.error(message)

    def log_warning(self, message: str) -> None:
        """
        Log a warning message with the logger
        :param message: The warning message to log
        :return: None
        """
        self._logger.warning(message)
