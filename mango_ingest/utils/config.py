"""Configuration utilities"""
import os
import pathlib
import yaml
from . import logger


def load_config():
    """Load configuration from yaml file"""
    config = {}
    if config_file := os.getenv("MANGO_INGEST_CONFIG"):
        try:
            config = yaml.safe_load(pathlib.Path(config_file).read_text(encoding='utf-8'))
        except Exception:   # pylint: disable=broad-except
            logger.exception("Problem loading config file %s", config_file)
    return config
