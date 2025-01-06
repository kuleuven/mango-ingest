"""Utility functions for tracking and writing results of ingest operations"""

import json
import pathlib
import datetime
import time
import threading
from typing import Union
from irods.data_object import iRODSDataObject
from . import logger

# Global results dictionary to track operations
_results = {
    "matched": [],
    "success": [],
    "failed": [],
    "ignored": [],
    "locked": [],
}

# Global variable to track the latest results refresh time
_results_modified_at = datetime.datetime.now(datetime.timezone.utc)

# Global variable to track the running report timestamp
_running_report_timestamp: Union[str, None] = None

# Reference to the reporting thread and stop event
_reporting_thread: Union[threading.Thread, None] = None
_stop_event: threading.Event = threading.Event()

# Report file written by the reporting thread
_report_file: Union[pathlib.Path, None] = None


def get_results() -> dict:
    """Get the current results dictionary"""
    return _results


def get_upload_status_record(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> dict:
    """Get a status record for an upload operation"""
    if isinstance(path, pathlib.Path):
        return {
            "path": str(path),
            "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(
                timespec="seconds"
            ),
            "st_size": path.stat().st_size,
            "st_mtime": path.stat().st_mtime,
            "checksum": checksum,
        }
    if isinstance(path, str):
        return {
            "path": str(path),
            "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(
                timespec="seconds"
            ),
            "checksum": checksum,
        }
    if isinstance(path, iRODSDataObject):
        return {
            "path": path.path,
            "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(
                timespec="seconds"
            ),
            "checksum": checksum,
        }
    return {
        "path": str(path),
        "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(
            timespec="seconds"
        ),
        "checksum": checksum,
    }


def refresh_report(report_file: Union[str, pathlib.Path]) -> None:
    """Refresh report file with current results"""
    report_file = pathlib.Path(report_file)
    if not report_file.exists() or (
        report_file.exists()
        and (
            _results_modified_at
            > datetime.datetime.fromtimestamp(
                report_file.stat().st_mtime, datetime.timezone.utc
            )
        )
    ):
        logger.debug(
            "Refreshing report file %s", report_file, extra={"style": "orange1"}
        )
        write_report(report_file)


def write_report(report_file: Union[str, pathlib.Path]) -> None:
    """Write running results to JSON report file"""
    report_file = pathlib.Path(report_file)
    report_file.write_text(json.dumps(_results, indent=2), encoding="utf-8")
    logger.verbose("Write report file %s", report_file, extra={"style": "orange1"})


def delete_report(
    report_file: Union[str, pathlib.Path], delete_all: bool = False
) -> None:
    """Delete report file"""
    report_file = pathlib.Path(report_file)
    report_files = sorted(
        [p for p in report_file.glob(get_report_file_glob())],
        key=lambda t: t.stat().st_mtime,
    )

    if not delete_all:
        report_files = report_files[:-1]

    for report_file in report_files:
        logger.info("Deleting report file %s", report_file)
        report_file.unlink()


def read_report(report_file: Union[str, pathlib.Path]) -> dict:
    """Read report file"""
    report_file = pathlib.Path(report_file)
    if report_file.exists():
        return json.loads(report_file.read_text(encoding="utf-8"))
    logger.warning("No report file found at %s", report_file)
    return {
        "matched": [],
        "success": [],
        "failed": [],
        "ignored": [],
        "locked": [],
    }


def get_report_file(
    path: Union[str, pathlib.Path, None] = None, refresh: bool = False
) -> pathlib.Path:
    """Get report file for given path

    Path is the location of the report file. If path is None, the report file
    currently used by the reporting thread is returned. If refresh is True, a new
    report file is created with a new timestamp.

    Args:
        path: Location of the report file
        new: Refresh the report timestamp
    """
    if path is None:
        if _report_file:
            return _report_file
        raise ValueError(
            "No path specified for report file and no running report file found"
        )

    global _running_report_timestamp  # pylint: disable=global-statement
    if refresh or _running_report_timestamp is None:
        _running_report_timestamp = (
            datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds")
            .replace(":", "")
        )
    return pathlib.Path(path, f"mango_ingest_results-{_running_report_timestamp}.json")


def get_report_file_glob() -> str:
    """Get glob pattern for results report files"""
    return "mango_ingest_results-*.json"


def refresh_results_modified_at() -> None:
    """Update the results modified timestamp"""
    global _results_modified_at  # pylint: disable=global-statement
    _results_modified_at = datetime.datetime.now(datetime.timezone.utc)


def register_success(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> None:
    """Register a successful upload operation"""
    _results["success"].append(get_upload_status_record(path, checksum))
    refresh_results_modified_at()


def register_failed(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> None:
    """Register a failed upload operation"""
    _results["failed"].append(get_upload_status_record(path, checksum))
    refresh_results_modified_at()


def register_matched(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> None:
    """Register a matched file"""
    _results["matched"].append(get_upload_status_record(path, checksum))
    refresh_results_modified_at()


def register_ignored(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> None:
    """Register an ignored file"""
    _results["ignored"].append(get_upload_status_record(path, checksum))
    refresh_results_modified_at()


def register_locked(
    path: Union[pathlib.Path, str, iRODSDataObject], checksum: str = ""
) -> None:
    """Register a locked file"""
    _results["locked"].append(get_upload_status_record(path, checksum))
    refresh_results_modified_at()


def start_reporting(path: pathlib.Path) -> pathlib.Path:
    """Start a thread to save results to report file"""

    global _report_file  # pylint: disable=global-statement
    _report_file = get_report_file(path, refresh=True)

    def do_refresh_report():
        # TODO: decide to make this an option or not
        refresh_secs = 30
        secs_running = 0
        while not _stop_event.is_set():
            if secs_running % refresh_secs == 0:
                refresh_report(_report_file)
            time.sleep(1)
            secs_running += 1

    _stop_event.clear()
    global _reporting_thread  # pylint: disable=global-statement
    _reporting_thread = threading.Thread(target=do_refresh_report, daemon=True)
    _reporting_thread.start()
    logger.verbose("Started reporting thread", extra={"style": "orange1"})
    return _report_file


def stop_reporting() -> None:
    """Stop the reporting thread"""
    global _reporting_thread  # pylint: disable=global-statement
    if _reporting_thread:
        logger.debug("Sending stop signal to reporting thread", extra={"style": "orange1"})
        _stop_event.set()
        logger.debug("Waiting for reporting thread to stop", extra={"style": "orange1"})
        _reporting_thread.join()
        _reporting_thread = None
        write_report(_report_file)
        logger.verbose("Reporting thread stopped", extra={"style": "orange1"})
