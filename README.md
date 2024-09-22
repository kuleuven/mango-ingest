
## Purpose


ManGO ingest is a lightweight tool to monitor a local directory for file
changes and ingest (part of) them into iRODS. There is no need for cronjobs
as it is based on python watchdog which starts its own threads for continous
operations.

The main purpose it to be an easy entry point for ingestion of files into
iRODS, from where possibly a ManGO Flow task will pick up and handle further
processing

## Current state of supported platforms: beta software

The initial development is focusing on Linux, but the target platforms are 
also including Windows and Mac OS. It may or may not work for you (yet), please
use the issue tracker to report on your findings/use cases and more..

## Installation

### Recommended

- check out this repository and cd into it
- run the following commands
```bash
$ python -m venv venv
$ . venv/bin/activate
$ pip install --editable src
```
Afterwards verify the executable `mango_ingest` is available in your PATH

```bash
$ mango_ingest --help
```

### Quick checkout

Just checkout the repository and copy the script `mango_ingest.py` around to where you want to execute it

### Authentication

Authentication is done by creating an `iRODSSession` from a configuration file either as specified by the environment variable `IRODS_ENVIRONMENT_FILE` or with a fallback to the current user `~/.irods/irods_environment.json`.

## Usage

`mango_ingest [OPTIONS] [COMMAND [OPTIONS] [ARGS]]`


  If it detects a new file creation, the corresponding file is inspected
  through a white list (glob pattern and/or regular expression list) and if
  *any* of those match, it is uploaded to the specified path in iRODS/ManGO

  Ignore patterns `--ignore-glob` and regular expressions `--ignore` are
  evaluated before any `--glob` and/or `--regex`

  CUSTOM FILTERS

  Custom filters can be specified too with --custom-filter, if they are
  resolvable with a dynamic import. The parameter is a string defining the
  name of the module nf function in the form `<module>.<function>` and that
  functions takes as the first positional parameter the `pathlib.Path`
  parameter of the file to validate, followed by an optional set of kwargs
  parameters. See also the option `--filter-kwargs` which accepts a dict/json
  string.

  METADATA

  In addition, there are a number of ways to add metadata on the fly. A few
  builtin functions cover the case for some rather obvious ones like metadata
  that is included in the path `--metadata-path` or shorter `--md-path` and
  file system properties such as modified time `--metadata-mtime` and symlink
  information

  You can also add your custom handler much in the same way as you can add
  custom filters, see `--help` and the `--metadata-handler` option. An example
  is also included in `doc/examples/extract_metadata.py` which relies on the
  exiftool executable and corresponding Python module.

  ENVIRONTMENT VARIABLES

  All parameters can also be set via environment variables using their long
  name, uppercased and prefixed with `MANGO_` . For example

      `export MANGO_DESTINATION="/zone/home/project/ingest" `

  is the same as specifying the command line option

      `mango_ingest --destination="/zone/home/project/ingest" `

  CONFIGURATION FILE

  Besides command line options, environment variables, you can also specify a
  Yaml formatted configuration file through the environment variable
  `MANGO_INGEST_CONFIG`. This can hold all or a subset of the command line
  options. It acts as a "default" setting for each option, and the value
  specified by the command line option or environment variable takes
  precedence.

  The builtin sub command `generate-config` will create such a yaml formatted
  config file for you.

```
Options:
  -v, --verbose                   Show runtime messages  [default: 0]
  -r, --recursive                 Also watch sub directories
  -p, --path TEXT                 The (local) path to monitor  [default: .]
  -d, --destination TEXT          iRODS destination collection path
  --observer [native|polling]     The observer system to use for getting
                                  changed paths. Defaults to 'polling' which
                                  is recommended for most use cases, but you
                                  can use also 'native' in for linux/mac
                                  filesystems when watching for new files that
                                  are directly written into the
                                  directorypolling is a rather brute force
                                  algorithm, needed for network mounted drives
                                  and windows for example  [default: polling]
  --polling-interval INTEGER      Polling interval in seconds in case the
                                  observer is specified as 'polling'
                                  [default: 5]
  --regex TEXT                    regular expression to match [multiple]
  --glob TEXT                     glob expression to match as a simpler
                                  alternative to --regex [multiple]
  --filter-func TEXT              use an external filter (along regex/glob
                                  patterns), it will be dynamically imported
  --filter-func-kwargs TEXT       A json string that will be parsed as a dict
                                  and injected as kwargs into the filter after
                                  the path
  --ignore TEXT                   regular expression to ignore certain
                                  files/folders [multiple]
  --ignore-glob TEXT              glob patterns to ignore files / folders
                                  [multiple]
  --sync                          Do an initial sync
  --verify-checksum               Verify checksums
  --restart PATH                  Use restart file to retry failed uploads
                                  from a previous run
  --dry-run                       Dry run: do not upload anything, implies
                                  --verbose
  -nw, --no-watch                 Do not start monitoring for future changes,
                                  implies --sync
  --metadata-path, --md-path TEXT
                                  regular expression to extract metadata from
                                  the path [multiple]
  --metadata-mtime, --md-mtime    Add the original modify time as metadata
  --metadata-handler, --md-handler TEXT
                                  a custom PYPON_PATH accessible
                                  module.function to handle metadata
  --metadata-handler-kwargs, --md-handler-kwargs TEXT
                                  kwargs parameters for the metadata-handler
                                  as a json string
  --help                          Show this message and exit.

Commands:
  check-regex      Utilty to test a regular expression against a filename...
  clean            Clean up older (default) or all (-a) result files
  examples         Examples
  generate-config  Generate a YAML config template
  show             Show parameter and values as would be used given the...

```
