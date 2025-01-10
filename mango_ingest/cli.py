"""ManGO Ingest Command Line Interface"""

import sys
import pathlib
import json
import fnmatch
import importlib
import re
import getpass
import click
import yaml
import rich

from .irods import (
    get_irods_session,
    sync_to_irods,
    iinit as irods_iinit,
    MANGO_IRODS_HOST,
)
from .utils.config import load_config
from .utils.logging import configure_console_logging, set_verbosity
from .utils import results
from . import logger
from . import metadata
from . import watchdog

console = rich.get_console()
dry_run = False  # Global dry run flag; pylint: disable=invalid-name


@click.group(context_settings={"show_default": True}, invoke_without_command=True)
@click.option("-v", "--verbose", count=True, help="Show runtime messages")
@click.option("-r", "--recursive", is_flag=True, help="Also watch sub directories")
@click.option("-p", "--path", default=".", help="The (local) path to monitor")
@click.option(
    "-d", "--destination", default=None, help="iRODS destination collection path"
)
@click.option(
    "--observer",
    default="polling",
    type=click.Choice(["native", "polling"]),
    help="The observer system to use for getting changed paths. "
    "Defaults to 'polling' which is recommended for most use cases, but you can use also 'native' "
    "for linux/mac filesystems when watching for new files that are directly written into the "
    "watched directory. Polling is a rather brute force algorithm, needed for network mounted "
    "drives and windows for example",
)
@click.option(
    "--polling-interval",
    default=5,
    help="Polling interval in seconds in case the observer is specified as 'polling'",
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
    help="A json string that will be parsed as a dict and injected "
    "as kwargs into the filter after the path",
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
    "--metadata-path",
    "--md-path",
    multiple=True,
    default=[],
    help="regular expression to extract metadata from the path [multiple]",
)
@click.option(
    "--metadata-mtime",
    "--md-mtime",
    is_flag=True,
    help="Add the original modify time as metadata",
)
@click.option(
    "--metadata-handler",
    "--md-handler",
    help="a custom PYPON_PATH accessible module.function to handle metadata",
)
@click.option(
    "--metadata-handler-kwargs",
    "--md-handler-kwargs",
    help="kwargs parameters for the metadata-handler as a json string",
)
@click.pass_context
def mango_ingest(ctx, **kwargs):
    """ManGO Ingest CLI

    ManGO Ingest is a lightweight tool to monitor a local directory for file changes and
    ingest (part of) them into iRODS. There is no need for cronjobs as it is based on python
    watchdog which starts its own threads for continous operations.

    The main purpose it to be an easy entry point for ingestion of files into iRODS, from
    where possibly a ManGO Flow task will pick up and handle further processing

    If it detects a new file creation, the corresponding file is inspected through a white list
    (glob pattern and/or regular expression list) and if *any* of those match, it is uploaded
    to the specified path in iRODS/ManGO

    Ignore patterns `--ignore-glob` and regular expressions `--ignore` are evaluated before
    any `--glob` and/or `--regex`

    CUSTOM FILTERS

    Custom filters can be specified too with --custom-filter, if they are resolvable with a
    dynamic import. The parameter is a string defining the name of the module and function
    in the form `<module>.<function>` and that functions takes as the first positional
    parameter the `pathlib.Path` parameter of the file to validate, followed by an
    optional set of kwargs parameters. See also the option `--filter-kwargs` which accepts
    a dict/json string.

    METADATA

    In addition, there are a number of ways to add metadata on the fly. A few builtin
    functions cover the case for some rather obvious ones like metadata that is included
    in the path `--metadata-path` or shorter `--md-path` and file system properties
    such as modified time `--metadata-mtime` and symlink information

    You can also add your custom handler much in the same way as you can add custom filters,
    see `--help` and the `--metadata-handler` option. An example is also included in
    `doc/examples/extract_metadata.py` which relies on the exiftool executable and corresponding
    Python module.

    ENVIRONTMENT VARIABLES

    All parameters can also be set via environment variables using their long name,
    uppercased and prefixed with `MANGO_` . For example

        `export MANGO_DESTINATION="/zone/home/project/ingest"`

    is the same as specifying the command line option

        `mango_ingest --destination="/zone/home/project/ingest"`

    CONFIGURATION FILE

    Besides command line options, environment variables, you can also specify
    a Yaml formatted configuration file through the environment variable `MANGO_INGEST_CONFIG`.
    This can hold all or a subset of the command line options. It acts as a "default" setting
    for each option, and the value specified by the command line option or environment
    variable takes precedence.

    The builtin sub command `generate-config` will create such a yaml formatted config
    file for you.
    """

    # save the kwargs in the click context object so other sub commands can read them
    ctx.obj = {**kwargs}

    # Configure the package logger
    # verbosity should be 1 or higher if dry-run is requested
    verbosity = (
        max(1, kwargs.get("verbose", 0))
        if kwargs.get("do_dry_run")
        else kwargs.get("verbose", 0)
    )
    if kwargs.get("use_console_logger", True):
        # Configure the logger with rich handler for enriched console messages
        configure_console_logging(logger, verbosity, console)
    else:
        # Use the default logging configuration with given verbosity
        set_verbosity(logger, verbosity)

    # Only execute the main command if there is no (auxiliary) subcommand invoked
    if ctx.invoked_subcommand is not None:
        return

    # since the destination option is not marked as required in order to have the option fall back
    # through environment variables and/or config file, we need to check and get it here
    destination = kwargs.get("destination")
    if not destination:
        destination = click.prompt("Please enter an iRODS destination path")

    if kwargs.get("do_dry_run"):
        global dry_run  # pylint: disable=global-statement
        dry_run = True
        logger.info(
            "Doing a dry run, no changes are made upstream to ManGO/iRODS",
            extra={"panel": {"style": "red bold", "expand": False}},
        )

    path = pathlib.Path(kwargs.get("path", ".")).absolute()

    recursive = kwargs.get("recursive", False)

    glob = list(kwargs.get("glob", []))
    regex = list(kwargs.get("regex", []))
    ignore_glob = list(kwargs.get("ignore_glob", []))
    ignore = list(kwargs.get("ignore", []))

    # add the report filename (global variable in this script) to the ignore list
    ignore_glob.append(results.get_report_file_glob())

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

    # sync_glob is only used to do a pre-monitoring sync. If sync is called, and
    # there is exactly 1 glob expression, use this to do the glob scanning
    sync_glob = None
    if kwargs.get("sync") or kwargs.get("no_watch"):
        sync_glob = glob[0] if (len(glob) == 1 and not regex) else "*"

    # Restart failed uploads
    restart = kwargs.get("restart")
    restart_paths = []
    if restart:
        previous_results = results.read_report(restart)
        restart_paths = list(
            set(record["path"] for record in previous_results["failed"])
        )
        # If sync of failed uploads is requested, the sync_glob should be set to None
        sync_glob = None

    # Setup metadata handlers
    metadata_handlers = []
    if md_handler := kwargs.get("metadata_handler"):
        # handler is just a string: in the form "<module>.<function>"
        # <module> may be in itself also a hierarchy
        handler_module, handler_function = md_handler.rsplit(".", 1)
        handler_module = (
            importlib.import_module(handler_module) if handler_module else None
        )
        handler_function = (
            getattr(handler_module, handler_function)
            if (handler_module and handler_function)
            else None
        )
        metadata_handler_kwargs = json.loads(
            kwargs.get("metadata_handler_kwargs", "{}")
        )
        if handler_function:
            metadata_handlers.append((handler_function, metadata_handler_kwargs))

    # the built-in metadata handler can be called as well :-)
    if metadata_path := kwargs.get("metadata_path"):
        for path_expr in list(metadata_path):
            metadata_handlers.append(
                (metadata.extract_metadata_from_path, {"path_regex": path_expr})
            )

    if kwargs.get("metadata_mtime"):
        metadata_handlers.append(
            (
                metadata.extract_system_metadata_from_file,
                {"system_attributes": ["original_modify_time"]},
            )
        )

    # Setup filter function
    filter_func = None
    filter_kwargs = {}
    if filter_spec := kwargs.get("filter_func"):
        if "." not in filter_spec:
            sys.exit(
                "The filter function must be in the form <module>.<function>, "
                "e.g. mymodule.myfunction"
            )
        filter_module, filter_function = filter_spec.rsplit(".", 1)
        filter_module = importlib.import_module(filter_module)
        filter_func = getattr(filter_module, filter_function)
        if filter_func_kwargs := kwargs.get("filter_func_kwargs"):
            filter_kwargs = json.loads(filter_func_kwargs)

    # Get an iRODS session
    irods_session = get_irods_session()
    if not irods_session:
        sys.exit("Cannot obtain a valid irods session")

    # Start reporting thread
    if not (dry_run or kwargs.get("no_watch")):
        results.start_reporting(path)

    if kwargs.get("sync") or kwargs.get("restart") or kwargs.get("no_watch"):
        sync_to_irods(
            irods_session=irods_session,
            path=path,
            destination=destination,
            recursive=recursive,
            regex=regex,
            glob=sync_glob,
            ignore=ignore,
            filter=filter_func,
            filter_kwargs=filter_kwargs,
            restart_paths=restart_paths,
            verify_checksum=kwargs.get("verify_checksum"),
            metadata_handlers=metadata_handlers,
            dry_run=dry_run,
        )

    if not kwargs.get("no_watch"):
        watchdog.monitor_and_sync_changes(
            path=path,
            destination=destination,
            recursive=recursive,
            observer_type=kwargs.get("observer", "polling"),
            dry_run=dry_run,
            polling_interval=kwargs.get("polling_interval", 5),
            filter=filter_func,
            filter_kwargs=filter_kwargs,
            verify_checksum=kwargs.get("verify_checksum"),
            metadata_handlers=metadata_handlers,
            regexes=regex,  # class RegexMatchingEventHandler
            ignore_regexes=ignore,  # class RegexMatchingEventHandler
            ignore_directories=True,  # class RegexMatchingEventHandler
        )
        results.stop_reporting()

    else:
        # if not watching, write results to report file once
        report_file = results.get_report_file(path)
        results.write_report(report_file)
        options = ctx.obj
        logger.verbose("Used options: %s", json.dumps(options, indent=2))

    irods_session.cleanup()


@mango_ingest.command()
@click.pass_context
def examples(ctx):  # pylint: disable=unused-argument
    """
    Examples

    The examples below assume the executable is in your PATH. Note that the order
    of the options does not matter.

    1) watch the current directory recursively for changes and upload new files to an irods
    zone, show activity

    mango_ingest -v -r --glob "*.csv" -d "/zone/home/project/ingest"

        Note: if a subfolder has no irods collection counterpart, it will be created on the fly

    2) Do the same as 1) but upload all existing files before watching

    mango_ingest -v -r --glob "*.csv" -d "/zone/home/project/ingest" --sync

    3) Match multiple file types

    mango_ingest -v -r --regex ".*\\.dat$" --regex ".*\\.log" -d "/zone/home/project/ingest" --sync

    4) ignore some that would match the general patterns

    mango_ingest -v -r --regex "electron-.*\\.dat$" --ignore "electron-ikwilunie.*\\.dat" \\
        -d "/zone/home/project/ingest" --sync

    REGULAR EXPRESSIONS DOCUMENTATION

    Please consult https://docs.python.org/3/library/re.html to learn more about regular
    expressions in Python.
    """
    console.print(ctx.get_help(), soft_wrap=False, markup=True)


@mango_ingest.command()
@click.option("--irods-user", help="iRODS user name", required=False)
@click.option("--irods-password", help="iRODS password", required=False)
@click.option("--irods-zone", help="iRODS zone", required=False)
@click.option("--irods-host", help="iRODS host", default=MANGO_IRODS_HOST)
@click.option("--irods-port", help="iRODS port", default=1247)
@click.option(
    "--irods-auth-scheme", help="iRODS authentication scheme", default="native"
)
def iinit(
    irods_user=None,
    irods_password=None,
    irods_zone=None,
    irods_host=MANGO_IRODS_HOST,
    irods_port=1247,
    irods_auth_scheme="native",
):
    """Initialize iRODS session"""
    if not irods_user:
        irods_user = click.prompt("iRODS user name", default=getpass.getuser())
    if not irods_password:
        irods_password = click.prompt("iRODS password", hide_input=True)
    if not irods_zone:
        irods_zone = click.prompt("iRODS zone")
    if irods_host == MANGO_IRODS_HOST:
        irods_host = MANGO_IRODS_HOST.format(irods_zone=irods_zone)
        irods_host = click.prompt("iRODS host", default=irods_host)
    irods_iinit(
        irods_user=irods_user,
        irods_password=irods_password,
        irods_zone=irods_zone,
        irods_host=irods_host,
        irods_port=irods_port,
        irods_authentication_scheme=irods_auth_scheme,
    )


@mango_ingest.command()
@click.option("-o", "--output", default="mango_ingest_config.yaml")
@click.pass_context
def generate_config(ctx, output):
    """Generate a YAML config template"""
    options = ctx.obj
    yaml_config = yaml.safe_dump(options, default_flow_style=False, indent=2)
    pathlib.Path(output).write_text(yaml_config, encoding="utf-8")
    console.print(yaml_config)


@mango_ingest.command()
@click.pass_context
def show(ctx):
    """Show current parameters and values"""
    options = ctx.obj
    current_config = yaml.safe_dump(options, default_flow_style=False, indent=2)
    console.print(current_config)


@mango_ingest.command()
@click.option("--regex", help="regular expression (Python syntax) to test")
@click.argument("filename")
def check_regex(regex, filename):
    """Utility to test a regular expression"""
    result = re.search(regex, filename)
    if result:
        console.print(
            f"Applying re.search({regex},{filename}): :heart: matched",
            style="green bold",
        )
    else:
        console.print(
            f"Applying re.search({regex},{filename}): :poop: no match", style="red bold"
        )


@mango_ingest.command(name="clean")
@click.option("-a", "clean_all", is_flag=True, help="Clean up all result files")
@click.option("--path", default=".", help="Directory holding the report files")
def clean_results(clean_all, path):
    """Clean up result files"""
    results.delete_report(path, delete_all=clean_all)


def entry_point():
    """Entry point for the CLI"""
    config = load_config()
    mango_ingest(auto_envvar_prefix="MANGO", default_map=config)
