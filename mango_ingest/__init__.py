"""
Mango Ingest Package
"""
import logging
from .utils import logging as logging_extension

_ = logging_extension  # noqa: F401

# Get logger instance for the package
logger = logging.getLogger(__name__)
