""" Logging configuration for the project. """

import logging
from logging import LogRecord
from typing import Optional
import click
import irods
import rich
from rich.logging import RichHandler
from rich.console import Console, ConsoleRenderable
from rich.panel import Panel
from rich.theme import Theme

# Define a new logging level
VERBOSE = 15
logging.addLevelName(VERBOSE, "VERBOSE")


# Define new Logger method
def verbose(self, message, *args, **kws):
    """Log 'message % args' with log level 'VERBOSE'."""
    if self.isEnabledFor(VERBOSE):
        self._log(VERBOSE, message, args, **kws)  # pylint: disable=protected-access


# Add new method to the logger class
logging.Logger.verbose = verbose


class CustomRichHandler(RichHandler):
    """Customized RichHandler"""

    def __init__(self, *args, **kwargs):
        self.style = kwargs.pop("style", None)
        self.message_panel = kwargs.pop("message_panel", {})
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))

    def render_message(self, record: LogRecord, message: str) -> ConsoleRenderable:
        """Render the message with custom styling"""
        style = getattr(record, "style", self.style)
        if style and self.markup:
            message = f"[{style}]{message}[/{style}]"
        message_renderable = super().render_message(record, message)
        if panel := getattr(record, "panel", {}):
            panel = {**self.message_panel, **panel}
            return Panel(message_renderable, **panel)
        return message_renderable


def set_verbosity(logger: logging.Logger, verbosity: int) -> None:
    """Set the log level based on verbosity"""
    verbosities = {
        0: logging.WARNING,
        1: logging.INFO,
        2: VERBOSE,
        3: logging.DEBUG,
    }
    logger.setLevel(verbosities.get(verbosity, logging.ERROR))


def configure_console_logging(
    logger: logging.Logger, verbosity: int = -1, console: Optional[Console] = None
) -> None:
    """Configure the logger with rich handler"""
    if verbosity > -1:
        set_verbosity(logger, verbosity)
    logger.propagate = False
    logger.handlers.clear()
    if not console:
        console = rich.get_console()
    # Add custom theme for verbose level
    console.push_theme(Theme({"logging.level.verbose": "magenta"}))
    handler = CustomRichHandler(
        rich_tracebacks=True,
        tracebacks_suppress=[click, irods],
        console=console,
        markup=True,
        show_path=False,
        message_panel={"expand": True},
    )
    logger.addHandler(handler)
