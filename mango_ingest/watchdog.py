""" Event handler for the ManGO Ingest module """

import threading
import pathlib
import datetime
import os
import time
import pprint
from watchdog.events import RegexMatchingEventHandler, FileSystemEvent
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from rich import pretty
from . import irods
from . import logger


def _now_as_utc_timestamp() -> float:
    """Helper to get the current time as a UTC timestamp"""
    return datetime.datetime.now(tz=datetime.timezone.utc).timestamp()


class ManGOIngestHandler(
    RegexMatchingEventHandler
):  # pylint: disable=too-many-instance-attributes
    """File system event handler for ManGO Ingest"""

    def __init__(
        self,
        path: str,
        irods_destination: str,
        observer_type: str,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        self.path = path
        self.irods_destination = irods_destination
        self.observer_type = observer_type
        self.dry_run = dry_run
        self.filter = kwargs.pop("filter", None)
        self.filter_kwargs = kwargs.pop("filter_kwargs", None)
        self.verify_checksum = kwargs.pop("verify_checksum", False)
        self.metadata_handlers = kwargs.pop("metadata_handlers", [])

        # delay queue
        self.delay_queue = {}
        self.delay_queue_last_visit = None
        self.delay_queue_lock = threading.Lock()

        # setup the delay queue handler thread
        queue_interval = kwargs.pop("queue_interval", 10)
        time_at_rest = kwargs.pop("time_at_rest_criterion", 4)
        queue_thread = threading.Thread(
            target=self.process_delay_queue,
            kwargs={
                "interval": queue_interval,
                "time_at_rest_criterion": time_at_rest,
            },
            daemon=True,
        )
        queue_thread.start()
        super().__init__(**kwargs)

    def delay_event(self, event: FileSystemEvent):
        """Add an event to the delay queue"""
        with self.delay_queue_lock:
            self.delay_queue[event.src_path] = {
                "event": event,
                "event_timestamp": _now_as_utc_timestamp(),
                "event_path_mtime": pathlib.Path(event.src_path).stat().st_mtime,
            }
        logger.verbose("Added event to delay queue: %s", event.src_path)

    def remove_delay_event_via_path(self, path: str):
        """Remove an event from the delay queue"""
        with self.delay_queue_lock:
            self.delay_queue.pop(path, None)

    def process_delay_queue(
        self, interval: float = 10, time_at_rest_criterion: float = 30
    ):
        """Process the delay queue"""
        logger.debug(
            "Delay queue started with interval %s and time at rest %s",
            interval,
            time_at_rest_criterion,
        )

        while True:
            path_list_to_treat = []
            self.delay_queue_last_visit = _now_as_utc_timestamp()
            with self.delay_queue_lock:
                logger.debug("Processing %s items in delay queue", len(self.delay_queue))

                for path, item in self.delay_queue.items():
                    # check if mtime has changed since the recorded mtime
                    # and set the delay_queue value of it to the new one if it has changed
                    # if it has not changed, look up the event_timestamp, this should
                    # also be older than the time_at_rest_criterion
                    current_path_mtime = pathlib.Path(path).stat().st_mtime
                    now_as_timestamp = _now_as_utc_timestamp()
                    if current_path_mtime != item["event_path_mtime"]:
                        # Set to the reported value, which for whatever reason
                        # can be far different from now()
                        item["event_path_mtime"] = current_path_mtime
                        item["event_timestamp"] = now_as_timestamp
                        continue
                    if (
                        now_as_timestamp
                        < item["event_timestamp"] + time_at_rest_criterion
                    ):
                        continue
                    # found an eligible path !
                    path_list_to_treat.append(path)
                    logger.debug(
                        "Delay queue: added an eligible path to process: %s", path
                    )

            # now handle the path event(s)
            if path_list_to_treat:
                for path_to_treat in path_list_to_treat:
                    item_to_treat = self.delay_queue.pop(path_to_treat)
                    try:
                        self.handle_event(event=item_to_treat["event"])
                    except Exception:  # pylint: disable=broad-except
                        logger.exception("Error while handling %s", path_to_treat)

            # sleep if we need to, usually not too much sleep needed unless
            # native or other signals trigger a direct long running action
            use_interval = interval if irods.busy_uploading else 1
            if (
                elapsed := (_now_as_utc_timestamp() - self.delay_queue_last_visit)
            ) < use_interval:
                time.sleep(use_interval - elapsed)

    def dispatch(self, event: FileSystemEvent) -> None:
        """Dispatch an event

        Override dispatch to use re.search instead of re.match.
        """
        if self.ignore_directories and event.is_directory:
            return

        logger.debug("ManGO Ingest dispatch: received file event %s", event)

        paths = []
        if hasattr(event, "dest_path"):
            paths.append(os.fsdecode(event.dest_path))
        if event.src_path:
            paths.append(os.fsdecode(event.src_path))

        if any(r.search(p) for r in self.ignore_regexes for p in paths):
            return

        if any(r.search(p) for r in self.regexes for p in paths):
            super().dispatch(event)

    def handle_event(self, event: FileSystemEvent):
        """Handle an event"""

        # exclude directory creation, we are ony interested in files (for now)
        if not event.is_directory:
            logger.debug("Handling file: %s", event.src_path)
            file_path = pathlib.Path(event.src_path)

            # run external filter and return if it returns False or raises an
            # exception, otherwise continue
            if self.filter:
                logger.verbose(
                    "validating against external rule with %s",
                    self.filter_kwargs,
                    extra={"style": "blue"},
                )

                try:
                    if not self.filter(file_path, **self.filter_kwargs):
                        logger.debug(
                            "External rule returned False",
                            extra={"style": "red"},
                        )
                        super().on_closed(event)
                        return
                except Exception:  # pylint: disable=broad-except
                    logger.exception("An error occurred with the filter")
                    super().on_closed(event)
                    return

            if self.dry_run:
                logger.info("dry-run: would upload %s", file_path)
                return super().on_closed(event)

            irods_session = irods.get_irods_session()
            file_path = file_path.absolute()
            irods.upload_to_irods(
                irods_session=irods_session,
                local_path=file_path,
                irods_collection=self.irods_destination,
                local_base_path=self.path,
                verify_checksum=self.verify_checksum,
                metadata_handlers=self.metadata_handlers,
            )

    def on_closed(self, event: FileSystemEvent) -> None:
        """Handle a closed event

        Is called when writing to a file has finished and the handler is closed
        (native for linux).
        """
        # remove delay queue entry for this path, otherwise it may be uploaded twice
        self.remove_delay_event_via_path(event.src_path)
        try:
            self.handle_event(event=event)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Exception in on_closed handling for %s", event.src_path)
        return super().on_closed(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self.delay_event(event=event)
        # in case of polling observer, print out a debug message
        # for native observer, its an avalanche with larger files, so
        # print nothing
        if self.observer_type == "polling":
            logger.debug(
                "Sent modified event to the delay queue for %s", event.src_path
            )
        return super().on_modified(event)

    def on_created(self, event: FileSystemEvent) -> None:
        self.delay_event(event=event)
        logger.debug("Sent created event to the delay queue for %s", event.src_path)
        return super().on_created(event)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n {pprint.pformat(self.__dict__)}"


def monitor_and_sync_changes(
    path: str,
    destination: str,
    recursive: bool = False,
    observer_type: str = "polling",
    dry_run: bool = False,
    **kwargs,
):
    """Start watching a directory for changes and sync them to iRODS"""
    path = pathlib.Path(path).absolute()
    polling_interval = kwargs.pop("polling_interval", 5)
    handler = ManGOIngestHandler(
        path=path,
        irods_destination=destination,
        observer_type=observer_type,
        dry_run=dry_run,
        **kwargs,
    )
    observer = (
        PollingObserver(timeout=polling_interval)
        if observer_type == "polling"
        else Observer()
    )
    observer.schedule(handler, path, recursive=recursive)
    observer.start()
    logger.info(
        "ManGO Ingest is now monitoring %s\n"
        "Recursive: %s\n"
        "Observer: %s\n"
        "Polling interval: %s sec\n"
        "Handler applied: %s",
        os.path.abspath(path),
        recursive,
        type(observer),
        polling_interval if observer_type == 'polling' else 'NA',
        pretty.pretty_repr(handler),
        extra={"panel": {"style": "green bold"}, "highlighter": None},
    )

    try:
        while observer.is_alive():
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("")  # send newline to stdout after ^C
        observer.stop()
    observer.join()
    logger.info("Watcher terminated, have a nice day :waving_hand:", extra={"style": "red bold"})
