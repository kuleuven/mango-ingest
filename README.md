Usage: mango_ingest.py [OPTIONS] COMMAND [ARGS]...

  ManGO ingest is a lightweight tool to monitor a local directory for file
  changes and ingest (part of) them into iRODS. There is no need for cronjobs
  as it is based on python watchdog which starts its own threads for continous
  operations. It has also the benefit of adapting to the local file system on
  windows, mac or linux.

  The main purpose it to be an easy entry point for ingestion of files into
  iRODS, from where possibly a ManGO Flow task will pick up and handle further
  processing

  If it detects a new file creation, the corresponding file is inspected
  through a white list (glob pattern and/or regular expression list) and if
  *any* of those match, it is uploaded to the specified path in iRODS/ManGO

  Ignore patterns `--ignore-glob` and regular expressions `--ignore` are
  evaluated before any `--glob` and/or `--regex`

  Custom filters can be specified too with --filter, if they are resolvable
  with a dynamic import. The parameter is a string defining the name of the
  module nf function in the form `<module>.<function>` and that functions
  takes as the first positional parameter the `pathlib.Path` parameter of the
  file to validate, followed by an optional set of kwargs parameters. See also
  the option `--filter-kwargs` which accepts a dict/json string.

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

Options:
  -v, --verbose                Show runtime messages
  -r, --recursive              Also watch sub directories
  -p, --path TEXT              The (local) path to monitor  [default: .]
  -d, --destination TEXT       iRODS destination collection path
  --observer [native|polling]  The observer system to use for getting changed
                               paths. Defaults to 'native' which is
                               recommended, but you can use also 'polling' to
                               select a brute force algorithm, which can be
                               needed for network mounted drives for example
                               [default: native]
  --regex TEXT                 regular expression to match [multiple]
  --glob TEXT                  single glob expression to match as a simpler
                               alternative to --regex
  --filter TEXT                use an external filter (along regex/glob
                               patterns), it will be dynamically imported
  --filter-kwargs TEXT         A json string that will be parsed as a dict and
                               injected as kwargs into the filter after the
                               path
  --ignore TEXT                regular expression to ignore certain
                               files/folders [multiple]
  --ignore-glob TEXT           glob patterns to ignore files / folders
                               [multiple]
  --sync                       Do an initial sync
  --verify-checksum            Verify checksums
  --restart PATH               Use restart file to retry failed uploads from a
                               previous run
  --dry-run                    Dry run: do not upload anything, implies
                               --verbose
  --help                       Show this message and exit.

Commands:
  check-regex               Utilty to test a regular expression against a...
  examples                  Examples
  generate-config-template  Generate a YAML config template
