"""ManGO Ingest Exceptions"""


class ManGOIngestException(Exception):
    """Base Exception for ManGO Ingest Errors."""


class ManGOMetadataError(ManGOIngestException):
    """ManGO Metadata Errors."""
    def __init__(self, message="ManGO metadata error", **params):
        super().__init__(f"{message} : {params}")
