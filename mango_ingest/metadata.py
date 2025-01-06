""" Utility functions for metadata extraction and manipulation. """

import pathlib
import re
import datetime
from typing import Dict, List, Union, Optional


# Copied from ManGO Flow: path based metadata extraction
# For now disregard mapper and splitter though
def extract_metadata_from_path(
    path: str,
    path_regex: str,
    mapper: Optional[Dict[str, str]] = None,
    split_metadata: Optional[Dict[str, str]] = None
) -> Dict[str, Union[str, List[str]]]:
    """
    Metadata handler to extract metadata from a path using regex patterns.

    Both path and path_regex path may be a partial path expression, meaning from
    the end of a string mapper converts the restricted metadata names into a more
    general form (irods accepts almost anything).

    Args:
        path: The path to extract metadata from
        path_regex: Regular expression pattern with named groups
        mapper: Maps restricted metadata names into a more general form
        split_metadata: Used to further split a value into a list of values. Contains
            the metadata name (before mapping) and the regex to split on
    """
    if mapper is None:
        mapper = {}
    if split_metadata is None:
        split_metadata = {}

    matches = re.search(path_regex, path)
    extracted_metadata = {}

    if matches:
        extracted_metadata_raw = matches.groupdict()
        for key, value in extracted_metadata_raw.items():
            extracted_metadata[mapper.get(key, key)] = (
                value if not split_metadata.get(key, False)
                else re.split(split_metadata[key], value)
            )

    return extracted_metadata


def iso8601_format_timestamp(
    timestamp: float,
    timespec: str = "seconds"
) -> str:
    """
    Format a timestamp as ISO 8601 datetime string.
    """
    formatted_timestamp = (
        datetime.datetime.fromtimestamp(timestamp)
        .astimezone(datetime.timezone.utc)
        .isoformat(timespec=timespec)
    )
    return formatted_timestamp


def extract_system_metadata_from_file(
    path: str,
    system_attributes: Optional[List[str]] = None
) -> Dict[str, Union[str, float]]:
    """
    Metadata handler to extract system metadata from a file.

    Args:
        path: Path to the file
        system_attributes: List of system attributes to extract
    """
    if system_attributes is None:
        system_attributes = []
    metadata_dict = {}
    mapping = {"original_modify_time": "st_mtime"}
    stats = pathlib.Path(path).stat()

    for attribute in system_attributes:
        try:
            value = getattr(stats, mapping[attribute])
            if attribute.endswith("time"):
                value = iso8601_format_timestamp(value)
            metadata_dict[attribute] = value
        except Exception:   # pylint: disable=broad-except
            pass

    return metadata_dict


def extract_metadata(path: pathlib.Path, metadata_handlers: List[tuple]) -> Dict:
    """Extract metadata from a file using a list of metadata handlers."""
    metadata = {}
    for metadata_handler, kwargs in metadata_handlers:
        metadata |= metadata_handler(str(path), **kwargs)
    return metadata
