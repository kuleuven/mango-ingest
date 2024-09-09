#!/bin/env python

import base64
import binascii
import datetime
import fnmatch
import importlib
import json
import os
import pathlib
import pprint
import re
import ssl
import threading
import time
from hashlib import sha256
from typing import Callable

import cachetools
import cachetools.func
import click
import rich
import rich.panel
import rich.pretty
import rich.progress
import yaml
from cachetools import TTLCache
from cachetools.keys import hashkey
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.meta import AVUOperation, iRODSMeta
from irods.session import iRODSSession
from rich.console import Console
from rich.markup import escape
from rich.pretty import Pretty
from watchdog.events import (
    FileSystemEvent,
    FileSystemEventHandler,
    RegexMatchingEventHandler,
)
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver


class MangoIngestException(Exception):
    def __init__(self, message="Mango Flow Metadata Extraction Error", **params):
        super().__init__(f"{message} : {params}")


##### global variables / objects
print_output = False
dry_run = False
console = Console()

## global result variables
result = {
    "matched": [],
    "success": [],
    "failed": [],
    "ignored": [],
    "locked": [],
}

# tick tick, first tick is launch time of this script
latest_result_time = datetime.datetime.now(datetime.timezone.utc)

result_file_timestring = datetime.datetime.now(datetime.timezone.utc).isoformat(
    timespec="seconds"
)
result_filename = f"mango_ingest_results-{result_file_timestring}.json"
# for use in ignoring the results file for ingestion
result_filename_glob = "mango_ingest_results-*.json"
######


## python print() override using Rich
def print(*args, **kwargs):
    """Override the Python built in print function with the rich library version and only really print when asked"""
    if print_output:
        console.log(*args, **kwargs)


## simple caching and re-use, to expand like mango flow/ mango portal with expiry checks?
irods_session: iRODSSession | None = None


## helper
def get_upload_status_record(path: pathlib.Path) -> dict:
    return {
        "path": str(path),
        "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(
            timespec="seconds"
        ),
        "size": path.stat().st_size,
    }


## session init
def get_irods_session() -> iRODSSession:

    # need to check session timeout for long running operations, copy management from mango portal?
    # although its likely used using a long valid ingress account when deployed in production

    if irods_session:
        return irods_session

    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment.json")
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None
    )
    ssl_settings = {"ssl_context": ssl_context}

    return iRODSSession(irods_env_file=env_file, **ssl_settings)


## cache helper for irods_mkdir_p below
def cache_key_path_only(irods_sesion, collection_path):
    return hashkey(collection_path)


## force irods collection (tree) to exist, but only once during cache lifetime (see ttl parameter, in seconds)
@cachetools.cached(
    cache=TTLCache(maxsize=500, ttl=1200), key=cache_key_path_only, info=True
)
def irods_mkdir_p(irods_session: iRODSSession, collection_path: str):
    try:
        irods_session.collections.create(collection_path)
    except Exception as e:
        # should ideally be more specific, if the collection alreay exists, fine,
        # if any other exception, should exit()
        print(e)
    return collection_path


# This function is copied from ManGO Flow: safely add or replace AVU triplets
# based on a dict {'name': 'value', ...} value can be lists
def bulk_add_metadata(
    item: iRODSDataObject | iRODSCollection,
    metadata_items: dict,
    unit_text: str = "analysis/mango_ingest",
    as_admin=False,
    prefix="",
):
    if metadata_items:
        metadata_names = metadata_items.keys()
        avu_operations = [
            AVUOperation(operation="remove", avu=avu)
            for avu in item.metadata.items()
            if avu.name in metadata_names
        ]
        for m_name, m_value in metadata_items.items():
            m_name = prefix + m_name
            if type(m_value) == list:
                avu_operations.extend(
                    [
                        AVUOperation(
                            "add",
                            iRODSMeta(name=m_name, value=sub_value, units=unit_text),
                        )
                        for sub_value in m_value
                    ]
                )

            elif type(m_value) == str:
                avu_operations.append(
                    AVUOperation(
                        operation="add",
                        avu=iRODSMeta(name=m_name, value=m_value, units=unit_text),
                    )
                )
            else:
                raise (
                    MangoIngestException(
                        {"unknown_field_type for AVU operation": type(m_value)}
                    )
                )
        if len(avu_operations):
            print(f"Adding metadata to {item.name}: {metadata_items}")
            item.metadata(admin=as_admin).apply_atomic_operations(*avu_operations)


# Copied from ManGO Flow: path based metadata extraction
# For now disregard mapper and splitter though
def extract_metadata_from_path(
    path: str, path_regex: str, mapper: dict = {}, split_metadata: dict = {}
) -> dict:
    """
    both path and path_regex path may be a partial path expression, meaning from the end of a string
    mapper converts the restricted metadata names into a more general form (irods accepts almost anything)
    split_metadata is used to further split a value into a list of values
    and contains the metadata name (before mapping) and the regex to split on
    """
    matches = re.search(path_regex, path)

    extracted_metadata = {}
    if matches:
        extracted_metadata_raw = matches.groupdict()

        for key, value in extracted_metadata_raw.items():
            extracted_metadata[mapper.get(key, key)] = (
                value
                if not split_metadata.get(key, False)
                else re.split(split_metadata[key], value)
            )

    return extracted_metadata


## for use in the do_initial_sync function
def check_filters(
    file_path: pathlib.Path, regexes=None, filter=None, filter_kwargs=None
) -> bool:

    if regexes and any(re.search(pattern, str(file_path)) for pattern in regexes):
        return True

    if filter:
        print(f"validating against custom rule with {filter_kwargs}", style="bold blue")
        try:
            if not filter(file_path, **filter_kwargs):
                print("external rule returned False", style="red bold")
                return False
        except Exception as e:
            print(
                f"An error occurred with external validation: {e} .. Continuing though"
            )
            return False

    return True


## watcher class
class ManGOIngestWatcher(object):
    """ """

    def __init__(
        self,
        path: str = ".",
        handler=FileSystemEventHandler(),
        recursive: bool = False,
        observer: str = "native",
    ) -> None:
        self.path = pathlib.Path(path).resolve()  # get full path
        self.handler = handler
        self.recursive = recursive
        self.observer = (
            PollingObserver() if observer == "polling" else Observer()
        )  # so "native" by default uses Observer() which itself will adapt according to the client platform

    def run(self):
        self.observer.schedule(self.handler, self.path, recursive=self.recursive)
        # start the watcher thread, let it sail :-)
        self.observer.start()

        print(
            rich.panel.Panel(
                escape(
                    f"ManGO Ingest is now monitoring {os.path.abspath(self.path)}\n"
                    f"Recursive: {self.recursive}\n"
                    f"Observer: {type(self.observer)}\n"
                    f"Handler applied: {rich.pretty.pretty_repr(self.handler)}"
                ),
                style="green bold",
                expand=True,
            )
        )
        ## make it interruptable from the terminal, with a clean exit
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
        # lower the sail, absorb the watcher thread
        self.observer.join()
        # write out the report file before exiting
        report_file = pathlib.Path(self.path, result_filename)
        report_file.write_text(json.dumps(result, indent=2))
        print(f"Updated report file {report_file}", style="orange1 bold")

        irods_session.cleanup()
        print("\n:waving_hand: Watcher terminated, have a nice day!", style="red bold")


class ManGOIngestHandler(RegexMatchingEventHandler):
    def __init__(
        self, path: str, irods_destination: str = "/set/home/u0123318", **kwargs
    ) -> None:
        self.path = path
        self.irods_destination = irods_destination
        self.filter = kwargs.pop("filter", None)
        self.filter_kwargs = kwargs.pop("filter_kwargs", None)
        self.verify_checksum = kwargs.pop("verify_checksum", False)
        self.metadata_handlers = kwargs.pop("metadata_handlers", [])
        super().__init__(**kwargs)

    ## Override dispatch to use re.search instead of re.match
    def dispatch(self, event: FileSystemEvent) -> None:
        """Dispatches events to the appropriate methods.

        :param event:
            The event object representing the file system event.
        :type event:
            :class:`FileSystemEvent`
        """
        if self.ignore_directories and event.is_directory:
            return

        paths = []
        if hasattr(event, "dest_path"):
            paths.append(os.fsdecode(event.dest_path))
        if event.src_path:
            paths.append(os.fsdecode(event.src_path))

        if any(r.search(p) for r in self.ignore_regexes for p in paths):
            return

        if any(r.search(p) for r in self.regexes for p in paths):
            super().dispatch(event)

    # on_closed is called when writing to a file has finished and the handler is closed
    def on_closed(self, event: FileSystemEvent) -> None:
        # exclude directory creation, we are ony interested in files (for now)
        if not event.is_directory:
            print(f"Added file {event.src_path}")
            file_path = pathlib.Path(event.src_path)
            print(f"And is interpreted by pathlib as {file_path}")
            print(f"destination rel path is {file_path.relative_to(self.path)}")

            ## run external filter and return if it returns False or raises an exception, otherwise continue
            if self.filter:
                print(
                    f"validating against external rule with {self.filter_kwargs}",
                    style="bold blue",
                )
                try:
                    if not self.filter(file_path, **self.filter_kwargs):
                        print("external rule returned False", style="red bold")
                        return super().on_closed(event)
                except Exception as e:
                    print(f"An error occurred with external validation: {e}")
                    return super().on_closed(event)
            if dry_run:
                print(f"dry-run: would upload {file_path}")
                return super().on_closed(event)
            irods_session = get_irods_session()
            file_path = file_path.resolve()
            upload_result = upload_to_irods(
                irods_session=irods_session,
                local_path=file_path,
                irods_collection=self.irods_destination,
                local_base_path=self.path,
                verify_checksum=self.verify_checksum,
                metadata_handlers=self.metadata_handlers,
            )
            if upload_result:
                result["success"].append(get_upload_status_record(file_path))
            else:
                result["failed"].append(get_upload_status_record(file_path))
            global latest_result_time
            latest_result_time = datetime.datetime.now(datetime.timezone.utc)

        return super().on_closed(event)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n {pprint.pformat(self.__dict__)}"


def check_data_object_replica_status(data_object: iRODSDataObject | None) -> bool:
    """
    Check the data object status via its replica status, for all available replicas
    """
    return (
        data_object
        and data_object.replicas
        and all([int(replica.status) == 1 for replica in data_object.replicas])
    )


def irods_to_sha256_checksum(irods_checksum):
    if irods_checksum is None or not irods_checksum.startswith("sha2:"):
        return None
    return binascii.hexlify(base64.b64decode(irods_checksum[5:])).decode("utf-8")


# From Jef's code
def compare_checksums(session, file_path, data_object_path):
    """Check whether the checksum of a local file matches its iRODS equivalent


    Arguments
    ---------
    session: obj
        An iRODSSession object

    file_path: str
        The path of a local file
        Please provide a full path

    data_object_path: str
        The path to a data object in iRODS
        Please provide a full path.

    Returns
    -------

    do_checksums_match: bool
        True if sizes match, False in case of a mismatch.
    """

    try:
        # get checksum from iRODS
        # put first so function fails early if data object does not exist
        obj = session.data_objects.get(data_object_path)
        try:
            irods_checksum = obj.chksum()
        except Exception as e:
            if -1803000 in e.args:
                print("Object is locked", style="red bold")
                result["locked"].append(get_upload_status_record(data_object_path))
            return False
        irods_checksum_sha256 = irods_to_sha256_checksum(irods_checksum)
        BUFFER = 32 * 1024 * 1024
        # get local checksum
        hash_sha256 = sha256()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(BUFFER), b""):
                hash_sha256.update(chunk)
        local_checksum_sha256 = hash_sha256.hexdigest()

        return local_checksum_sha256 == irods_checksum_sha256
    except Exception as e:
        # Function will fail if data object doesn't exist
        print(
            f"Checksum failed for {data_object_path} because of eception: {e}",
            style="red bold",
        )
        return False


def upload_to_irods(
    irods_session: iRODSSession,
    local_path: pathlib.Path,
    irods_collection: str,
    local_base_path: pathlib.Path | None = None,
    verify_checksum=False,
    metadata_handlers: list[(Callable, dict)] = [],
):
    ## check if the object is in a local sub directory
    # start assuming it is not..
    rel_local_parent = None  # kinda '.'
    # did we get a proper monotoring base path?
    if local_base_path:
        # ok, then chop off the base monitoring path and see waht is left
        rel_local_path = local_path.relative_to(local_base_path)
        # check if there are parent paths left and isolate the full hierarchy to use in the
        # irods counter part later
        if len(rel_local_path.parents) > 1:
            rel_local_parent = rel_local_path.parent
    else:
        rel_local_path = local_path.name
    # if there are local sub directories, ensure these are also available in the irods destination base
    # by creating them if needed
    if rel_local_parent:
        irods_mkdir_p(
            irods_session,
            str(pathlib.PurePath(irods_collection, str(rel_local_parent))),
        )

    # utility iterator to read the local file in chunks: saves local disk space(!) and feeds a
    # progress bar
    def read_in_chuncks(file_handler, chunk_size=1024 * 1024 * 8):
        while True:
            data = file_handler.read(chunk_size)
            if not data:
                break
            yield data

    # consruct the irods destination full path
    dst_path = str(pathlib.PurePath(irods_collection, str(rel_local_path)))
    # make the local read buffer 32MB
    buffering = 32 * 1024 * 1024
    # open the file with cool 'Rich' progress bar as a console display asset which implictely decorates a regular open()
    with rich.progress.open(local_path, "rb", buffering=buffering) as f:
        with irods_session.data_objects.open(dst_path, "w", auto_close=True) as f_dst:
            for chunk in read_in_chuncks(f):
                f_dst.write(chunk)
    result_object = irods_session.data_objects.get(dst_path)

    # the whole aftermath validation chain
    # with replica status, then size comparison and if requested the (cpu and i/o expensive) checksum validation.
    # The 'and' operation ensures if the "easier" validation rule fails, the next expensive validation rule is not
    # unnecessarily executed
    if (
        check_data_object_replica_status(result_object)
        and (result_object.size == local_path.stat().st_size)
        and (
            not verify_checksum
            or (
                verify_checksum
                and compare_checksums(irods_session, str(local_path), dst_path)
            )
        )
    ):
        print(f"Successfully uploaded local {local_path} to irods {dst_path}")
        if metadata_handlers:
            metadata_dict = {}
            for metadata_handler, kwargs in metadata_handlers:
                metadata_dict |= metadata_handler(str(local_path), **kwargs)
            if metadata_dict:
                bulk_add_metadata(
                    item=result_object, metadata_items=metadata_dict, prefix="mg."
                )
                print(
                    f"Added {len(metadata_dict)} metadata items to {result_object.name}"
                )

        return result_object
    else:
        print(f"Failed uploading {local_path} to irods {dst_path}")
        return False


### intial sync function, inspired by Jef's sync script but adding + and - filters, including
### custom filters if requested. Also offers restart of previous failed transfers
def do_initial_sync_and_or_restart(
    irods_session: iRODSSession | None,
    path: pathlib.Path,
    destination: str,
    recursive=False,
    regex=[],
    filter=None,
    filter_kwargs=None,
    glob="*",
    restart_paths=[],  # list of path strings
    ignore=None,
    verify_checksum=False,
    metadata_handlers: list[(Callable, dict)] = [],
) -> dict:

    path_objects = []
    # if there are restart paths to treat, add them
    if restart_paths:
        path_objects = [pathlib.Path(path) for path in restart_paths]
    if glob:
        path_objects += [
            path for path in (path.rglob(glob) if recursive else path.glob(glob))
        ]

    for path_object in path_objects:
        if path_object.is_file() and (full_path := path_object.resolve()):

            print(f"sync {full_path}")
            if ignore and any(
                [re.search(pattern, str(full_path)) for pattern in ignore]
            ):
                print(f"ignoring {full_path}")
                continue

            if check_filters(
                full_path,
                regexes=regex,
                filter=filter,
                filter_kwargs=filter_kwargs,
            ):
                result["matched"].append(get_upload_status_record(full_path))
                if dry_run:
                    print(f"dry-run: would upload {full_path}")
                    continue
                # check if the object already exists and has the same size
                # and if checksum checks are enabled, verify also the checksum
                # to decide to ignore
                try:
                    rel_local_path = full_path.relative_to(path)
                    irods_path = str(pathlib.PurePath(destination, str(rel_local_path)))
                    irods_data_object = irods_session.data_objects.get(irods_path)
                    if full_path.stat().st_size == irods_data_object.size:
                        if verify_checksum and compare_checksums(
                            irods_session, str(full_path), irods_path
                        ):
                            result["ignored"].append(
                                get_upload_status_record(full_path)
                            )
                            print(
                                f"Ignoring existing data_object for {full_path}, checksum and size match"
                            )
                            continue
                        if not verify_checksum:
                            result["ignored"].append(
                                get_upload_status_record(full_path)
                            )
                            print(
                                f"Ignoring existing data_object for {full_path}, size matches"
                            )
                            continue
                except:
                    pass

                upload_result = upload_to_irods(
                    irods_session=irods_session,
                    local_path=full_path,
                    irods_collection=destination,
                    local_base_path=path,
                    verify_checksum=verify_checksum,
                    metadata_handlers=metadata_handlers,
                )
                if upload_result:
                    result["success"].append(get_upload_status_record(full_path))
                else:
                    result["failed"].append(get_upload_status_record(full_path))
            else:
                result["ingnored"].append(get_upload_status_record(full_path))
                continue
        else:
            print(f" did not treat local dir {path_object}")
    # not needed, but semantically correct:
    return result


### The main mango_ingest command
# Declare it as a mother ship command, which can be invoked with or without sub commands
# sub commands in this context are meant to be auxiliary
@click.group(context_settings={"show_default": True}, invoke_without_command=True)
# The many options start just here
@click.option("-v", "--verbose", is_flag=True, help="Show runtime messages")
@click.option("-r", "--recursive", is_flag=True, help="Also watch sub directories")
@click.option("-p", "--path", default=".", help="The (local) path to monitor")
@click.option(
    "-d",
    "--destination",
    default=None,
    help="iRODS destination collection path",
    prompt="iRODs destination collection",
)
@click.option(
    "--observer",
    default="native",
    type=click.Choice(["native", "polling"]),
    help="The observer system to use for getting changed paths. "
    "Defaults to 'native' which is recommended, but you can use also 'polling' "
    "to select a brute force algorithm, which can be needed for network mounted drives for example",
)
@click.option(
    "--regex", multiple=True, default=[], help="regular expression to match [multiple]"
)
@click.option(
    "--glob",
    multiple=True,
    default=[],
    help="glob expression to match as a simpler alternative to --regex [multiple]",
)
@click.option(
    "--filter-func",
    help="use an external filter (along regex/glob patterns), it will be dynamically imported",
)
@click.option(
    "--filter-func-kwargs",
    help="A json string that will be parsed as a dict and injected as kwargs into the filter after the path",
)
@click.option(
    "--ignore",
    multiple=True,
    help="regular expression to ignore certain files/folders [multiple]",
)
@click.option(
    "--ignore-glob",
    multiple=True,
    help="glob patterns to ignore files / folders [multiple]",
)
@click.option("--sync", is_flag=True, help="Do an initial sync")
@click.option("--verify-checksum", is_flag=True, help="Verify checksums")
@click.option(
    "--restart",
    type=click.Path(exists=True),
    help="Use restart file to retry failed uploads from a previous run",
)
@click.option(
    "--dry-run",
    "do_dry_run",
    is_flag=True,
    help="Dry run: do not upload anything, implies --verbose",
)
@click.option(
    "-nw",
    "--no-watch",
    is_flag=True,
    help="Do not start monitoring for future changes, implies --sync",
)
@click.option(
    "--path-extract",
    multiple=True,
    default=[],
    help="regular expression to extract metadata from the path [multiple]",
)
@click.pass_context
def mango_ingest(
    ctx,
    verbose,
    recursive,
    path,
    destination,
    observer,
    regex,
    glob,
    filter_func,
    filter_func_kwargs,
    ignore,
    ignore_glob,
    sync,
    verify_checksum,
    restart,
    do_dry_run,
    no_watch,
    path_extract,
):
    """
    ManGO ingest is a lightweight tool to monitor a local directory for file changes and ingest (part of) them into iRODS.
    There is no need for cronjobs as it is based on python watchdog which starts its own threads for continous operations.
    It has also the benefit of adapting to the local file system on windows, mac or linux.

    The main purpose it to be an easy entry point for ingestion of files into iRODS, from where possibly
    a ManGO Flow task will pick up and handle further processing

    If it detects a new file creation, the corresponding file is inspected through a white list (glob pattern and/or
    regular expression list) and if *any* of those match, it is uploaded to the specified path in iRODS/ManGO

    Ignore patterns `--ignore-glob` and regular expressions `--ignore` are evaluated before any `--glob` and/or `--regex`



    Custom filters can be specified too with --filter, if they are resolvable with a dynamic import. The parameter is a string defining the name of the
    module nf function in the form `<module>.<function>` and that functions takes as the first positional parameter the `pathlib.Path`
    parameter of the file to validate, followed by an optional set of kwargs parameters. See also the option `--filter-kwargs` which accepts a dict/json string.

    ENVIRONTMENT VARIABLES

    All parameters can also be set via environment variables using their long name, uppercased and prefixed
    with `MANGO_` . For example

        `export MANGO_DESTINATION="/zone/home/project/ingest" `

    is the same as specifying the command line option

        `mango_ingest --destination="/zone/home/project/ingest" `

    CONFIGURATION FILE

    Besides command line options, environment variables, you can also specify a Yaml formatted configuration file
    through the environment variable `MANGO_INGEST_CONFIG`. This can hold all or a subset of the command line options.
    It acts as a "default" setting for each option, and the value specified by the command line option or environment variable takes precedence.

    """

    # save the local parameters (the arguments of main()) in the click context object so other sub commands can read them
    ctx.obj = {**locals()}

    # the main processing: only execute if there is no (auxiliary) sub command invoked
    if ctx.invoked_subcommand is None:
        if verbose or do_dry_run:
            global print_output
            print_output = True
        if do_dry_run:
            global dry_run
            dry_run = True
            print(
                rich.panel.Panel(
                    f"Doing a dry run, no changes are made upstream to ManGO / iRODS",
                    style="red bold",
                    expand=False,
                )
            )

        # the local directory to watch
        path = pathlib.Path(path).resolve()

        # the parameters below are initially immutable tuples, make them mutable
        ignore_glob = list(ignore_glob)
        ignore = list(ignore)
        glob = list(glob)
        regex = list(regex)

        ## setup the reporting thread
        if not (do_dry_run or no_watch):

            def smart_save_results():
                while True:
                    report_file = pathlib.Path(path, result_filename)
                    if not report_file.exists() or (
                        report_file.exists()
                        and (
                            latest_result_time
                            > datetime.datetime.fromtimestamp(
                                report_file.stat().st_mtime, datetime.timezone.utc
                            )
                        )
                    ):
                        report_file.write_text(json.dumps(result, indent=2))
                        print(
                            f"Updated report file {report_file}", style="orange1 bold"
                        )
                    print(f"Reporting thread heartbeat", style="orange1 bold")
                    # @todo decide to make this an option or not
                    time.sleep(10)

            reporting_thread = threading.Thread(target=smart_save_results, daemon=True)
            reporting_thread.start()
            print("Reporting thread started", style="orange1")
            # add the report filename to the ignore list
            ignore_glob.append(result_filename_glob)

        #### Processing of arguments

        # since the option is not marked as required in order to have the option fall back
        # through environment variables and/or config file, we need to check it here
        if not destination:
            exit("No destination found")

        global irods_session
        if not (irods_session := get_irods_session()):
            exit("Cannot obtain a valid irods session")

        sync_glob = None
        if sync or no_watch:
            sync_glob = glob[0] if (len(glob) == 1 and not regex) else "*"

        # compile the glob patterns into regexes
        if glob:
            regex = [fnmatch.translate(pattern) for pattern in glob] + regex

        # compile the ignore glob patterns to regexes
        if ignore_glob:
            ignore = [fnmatch.translate(pattern) for pattern in ignore_glob] + ignore
        # set regexes explicitely to None if empty to trigger the default behavior
        # in the regex handler, it does not cope with empty lists
        regex = list(regex) if regex else None
        ignore = list(ignore) if ignore else None
        ignore_glob = ignore_glob if ignore_glob else None

        ## restart and or sync section
        restart_paths = []
        if restart:
            previous_result = json.loads(pathlib.Path(restart).read_text())
            restart_paths = list(
                set([path["name"] for path in previous_result["failed"]])
            )
        # a bit special: sync_glob is only used to do a pre-monitoring sync
        # but the sync may also be called with the restart option only
        # in this case the passed sync_glob needs to be set to None
        # if sync is called, and there is exactly 1 glob expression, use this
        # to do the glob scanning

        # like mango flow: partials, but simpler :-)
        metadata_handlers = []

        if path_extract:
            for path_e in list(path_extract):
                metadata_handlers.append(
                    (extract_metadata_from_path, {"path_regex": path_e})
                )
        # check for custom filter_func
        if filter_func and "." in filter_func:
            (filter_module, filter_function) = filter_func.rsplit(".", 1)
        filter_func_module = importlib.import_module(filter_module) if filter_func else None
        filter_func = getattr(filter_func_module, filter_function) if filter_func_module else None
        filter_func_kwargs = json.loads(filter_func_kwargs) if filter_func_kwargs else {}

        if sync or restart or no_watch:
            print("First doing an initial sync/restart", style="red")
            do_initial_sync_and_or_restart(
                irods_session,
                path,
                destination=destination,
                recursive=recursive,
                regex=regex,
                glob=sync_glob,
                ignore=ignore,
                filter=filter_func,
                filter_kwargs=filter_func_kwargs,
                restart_paths=restart_paths,
                verify_checksum=verify_checksum,
                metadata_handlers=metadata_handlers,
            )

        

        if not no_watch:
            watcher = ManGOIngestWatcher(
                path=path,
                handler=ManGOIngestHandler(
                    path,
                    irods_destination=destination,
                    filter=filter_func,
                    filter_kwargs=filter_func_kwargs,
                    verify_checksum=verify_checksum,
                    metadata_handlers=metadata_handlers,
                    regexes=regex,  # class RegexMatchingEventHandler
                    ignore_regexes=ignore,  # class RegexMatchingEventHandler
                ),
                recursive=recursive,
                observer=observer,
            )
            watcher.run()
        else:
            # still write the report file
            report_file = pathlib.Path(path, result_filename)
            report_file.write_text(json.dumps(result, indent=2))
            # if verbose output
            options = ctx.obj
            del options["ctx"]
            print(json.dumps(options, indent=2))


@mango_ingest.command()
@click.pass_context
def examples(ctx):
    """
    Examples

    The examples below assume the executable is in your PATH. Note that the order of the options does not matter

    1) watch the current directory recursively for changes and upload new files to an irods zone, show activity

    mango_ingest -v -r --glob "*.csv" -d "/zone/home/project/ingest"

        Note: if a subfolder has no irods collection counterpart, it will be created on the fly

    2) Do the same as 1) but upload all existing files before watching

    mango_ingest -v -r --glob "*.csv" -d "/zone/home/project/ingest" --sync

    3) Match multiple file types

    mango_ingest -v -r --regex ".*\.dat$" --regex ".*\.log" -d "/zone/home/project/ingest" --sync

    4) ignore some that would match the general patterns

    mango_ingest -v -r --regex "electron-.*\.dat$" --ignore "electron-ikwilunie.*\.dat" -d "/zone/home/project/ingest" --sync

    REGULAR EXPRESSIONS DOCUMENTATION

    Please consult https://docs.python.org/3/library/re.html to learn more about regular expressions in Python

    """

    console.print(ctx.get_help(), soft_wrap=False, markup=True)


@mango_ingest.command()
@click.option("-o", "--output", default="mango_ingest_config.yaml")
@click.pass_context
def generate_config(ctx, output):
    """
    Generate a YAML config template

    """
    options = ctx.obj
    del options["ctx"]
    yaml_config = yaml.safe_dump(options, default_flow_style=False, indent=2)
    pathlib.Path(output).write_text(yaml_config)

    console.print(yaml_config)

@mango_ingest.command()
@click.pass_context
def show(ctx):
    """
    Show parameter and values as would be used given the combination of config file, env variables, 
    command line parameters (if any) and finally the built in defaults

    """
    options = ctx.obj
    del options["ctx"]
    # yaml output is pretty readable anyway, re-use some code 
    current_config = yaml.safe_dump(options, default_flow_style=False, indent=2)
    console.print(current_config)


@mango_ingest.command()
@click.option("--regex", help="regular expression (Python syntax) to test")
@click.argument("filename")
def check_regex(regex, filename):
    """
    Utilty to test a regular expression against a filename or path


    """
    result = re.search(regex, filename)
    if result:
        console.print(
            f"Applying re.search({regex},{filename}): :heart: married",
            style="green bold",
        )
    else:
        console.print(
            f"Applying re.search({regex},{filename}): :poop: no match", style="red bold"
        )


@mango_ingest.command(name="clean")
@click.option(
    "-a",
    "clean_all",
    is_flag=True,
    help="Clean up all result files",
)
@click.option("--path", default=".", help="Directory holding the report files")
def clean_results(clean_all, path):
    """
    Clean up older (default) or all (-a) result files
    """
    path = pathlib.Path(path)
    result_files = sorted(
        [p for p in path.glob(result_filename_glob)], key=lambda t: t.stat().st_mtime
    )

    if not clean_all:
        # keep the most recent one
        result_files = result_files[:-1]

    for res in result_files:
        console.print(f"Cleaning {res}")
        res.unlink()


def entry_point():
    default_map = {}  # default values for all command line options of the main command
    if config_file := os.getenv("MANGO_INGEST_CONFIG"):
        try:
            default_map = yaml.safe_load(pathlib.Path(config_file).read_text())
        except Exception as e:
            console.print(
                f"Problem loading config file {config_file}: {e}", style="red bold"
            )
    mango_ingest(obj={}, auto_envvar_prefix="MANGO", default_map=default_map)


if __name__ == "__main__":
    entry_point()
