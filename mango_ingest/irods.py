"""iRODS operations for Mango Ingest"""

import os
import ssl
import re
import pathlib
import base64
import json
from typing import Callable, Optional, Union, Dict, List
from hashlib import sha256
import binascii
import cachetools
from rich import progress
from irods.session import iRODSSession
from irods.password_obfuscation import encode as irods_password_encode
from irods.version import version_as_tuple
from irods.data_object import iRODSDataObject
from irods.collection import iRODSCollection
from irods.meta import AVUOperation, iRODSMeta
from .utils import results
from .errors import ManGOMetadataError
from . import metadata
from . import logger


# global flag to indicate if an upload is in progress
busy_uploading = False  # pylint: disable=invalid-name

# for PRC >=2.1.0, use native callback for progressbar
_progress_bar_irods = version_as_tuple() >= (2, 1, 0)  # pylint: disable=invalid-name

# Simple caching and re-use, to expand like mango flow/ mango portal with expiry checks?
_irods_session: Optional[iRODSSession] = None

# iRODS host template for KU Leuven ManGO.
MANGO_IRODS_HOST = r"{irods_zone}.irods.icts.kuleuven.be"


def get_irods_env():
    """Get path to iRODS environment file"""
    return os.getenv(
        "IRODS_ENVIRONMENT_FILE", os.path.expanduser("~/.irods/irods_environment.json")
    )


def get_irods_session():
    """Get an iRODS session"""

    global _irods_session  # pylint: disable=global-statement
    if _irods_session:
        return _irods_session

    env_file = get_irods_env()
    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ssl_settings = {"ssl_context": ssl_context}

    _irods_session = iRODSSession(irods_env_file=env_file, **ssl_settings)
    return _irods_session


def iinit(
    irods_user,
    irods_password,
    irods_host,
    irods_zone,
    irods_port: int = 1247,
    irods_authentication_scheme: str = "native",
):
    """Initialize iRODS environment"""
    irods_config = {
        "irods_host": irods_host,
        "irods_port": irods_port,
        "irods_zone_name": irods_zone,
        "irods_user_name": irods_user,
        "irods_encryption_algorithm": "AES-256-CBC",
        "irods_authentication_scheme": irods_authentication_scheme,
        "irods_encryption_salt_size": 8,
        "irods_encryption_key_size": 32,
        "irods_encryption_num_hash_rounds": 8,
        "irods_ssl_ca_certificate_file": "",
        "irods_ssl_verify_server": "cert",
        "irods_client_server_negotiation": "request_server_negotiation",
        "irods_client_server_policy": "CS_NEG_REQUIRE",
        "irods_default_resource": "default",
        "irods_cwd": f"/{irods_zone}/home/{irods_user}",
        "irods_authentication_uid": 1000,
    }

    def put(file_path, contents):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="UTF-8") as file:
            file.write(contents)

    if irods_authentication_scheme == "native":
        put(iRODSSession.get_irods_password_file(), irods_password_encode(irods_password, uid=1000))

    env_file = get_irods_env()
    put(env_file, str(json.dumps(irods_config)))
    logger.debug("creating iRODS session")
    with iRODSSession(irods_env_file=env_file, password=irods_password) as session:
        if irods_authentication_scheme == "PAM":
            put(
                iRODSSession.get_irods_password_file(),
                irods_password_encode(session.pam_pw_negotiated[0]),
            )
        logger.debug("Successfully authenticated to iRODS")


def check_filters(  # pylint: disable=redefined-builtin
    file_path: pathlib.Path, regexes=None, filter=None, filter_kwargs=None
) -> bool:
    """Check if a file path matches a set of filters"""

    if regexes and any(re.search(pattern, str(file_path)) for pattern in regexes):
        return True

    if filter:
        logger.verbose(
            "validating against custom filter with %s",
            filter_kwargs,
            extra={"style": "bold blue"},
        )
        try:
            if not filter(file_path, **filter_kwargs):
                logger.info(
                    "Custom filter returned False for %s",
                    file_path,
                    extra={"style": "red bold"},
                )
                return False
        except Exception:  # pylint: disable=broad-except
            logger.exception("An error occurred with external validation. Ignoring...")
            return False
    return True


def irods_to_sha256_checksum(irods_checksum):
    """Convert an iRODS checksum to a sha256 checksum"""
    if irods_checksum is None or not irods_checksum.startswith("sha2:"):
        return None
    return binascii.hexlify(base64.b64decode(irods_checksum[5:])).decode("utf-8")


def validate_checksums(session: iRODSSession, file_path: str, data_object_path: str):
    """Check whether the checksum of a local file matches its iRODS equivalent

    If succesful, returns the sha256 checksum
    """
    try:
        # get checksum from iRODS
        # put first so function fails early if data object does not exist
        obj = session.data_objects.get(data_object_path)
        try:
            irods_checksum = obj.chksum()
        except Exception as e:  # pylint: disable=broad-except
            if -1803000 in e.args:
                logger.info("Object %s is locked", data_object_path, extra={"color": "red"})
                results.register_locked(data_object_path)
            return False
        irods_checksum_sha256 = irods_to_sha256_checksum(irods_checksum)
        buffer_size = 32 * 1024 * 1024
        # get local checksum
        hash_sha256 = sha256()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(buffer_size), b""):
                hash_sha256.update(chunk)
        local_checksum_sha256 = hash_sha256.hexdigest()
        if local_checksum_sha256 == irods_checksum_sha256:
            return local_checksum_sha256
        else:
            return False
    except Exception:  # pylint: disable=broad-except
        # Function will fail if data object doesn't exist
        logger.exception("Checksum failed for %s", data_object_path)
        return False


def _cache_key_path_only(
    irods_sesion, collection_path
):  # pylint: disable=unused-argument
    """Helper function to generate a cache key for irods_mkdir_p"""
    return cachetools.keys.hashkey(collection_path)


@cachetools.cached(
    cache=cachetools.TTLCache(maxsize=500, ttl=1200),
    key=_cache_key_path_only,
    info=True,
)
def irods_mkdir_p(irods_session: iRODSSession, collection_path: str):
    """Force an iRODS collection to exist, but only once during cache
    lifetime (see ttl parameter, in seconds)"""
    try:
        irods_session.collections.create(collection_path)
    except Exception:
        # should ideally be more specific, if the collection already exists, fine,
        # if any other exception, should exit()
        logger.exception("Exception during irods_mkdir_p")
    return collection_path


def upload_to_irods(
    irods_session: iRODSSession,
    local_path: pathlib.Path,
    irods_collection: str,
    local_base_path: Optional[pathlib.Path] = None,
    verify_checksum=False,
    metadata_handlers: list[(Callable, dict)] = None,
):
    """Upload to iRODS"""
    # update the global busy uploading flag
    global busy_uploading  # pylint: disable=global-statement
    busy_uploading = True

    if metadata_handlers is None:
        metadata_handlers = []

    try:
        # check if the object is in a local sub directory, start assuming it is not...
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
        # if there are local sub directories, ensure these are also available in the
        # irods destination base by creating them if needed
        if rel_local_parent:
            irods_mkdir_p(
                irods_session,
                str(
                    pathlib.PurePosixPath(
                        irods_collection, str(rel_local_parent.as_posix())
                    )
                ),
            )

        # construct the irods destination full path
        dst_path = str(
            pathlib.PurePosixPath(irods_collection, str(rel_local_path.as_posix()))
        )
        logger.verbose("Destination path for upload is %s", dst_path)

        if _progress_bar_irods:
            with progress.Progress(
                progress.SpinnerColumn(),
                *progress.Progress.get_default_columns(),
                progress.TimeElapsedColumn(),
                progress.FileSizeColumn(),
                progress.TransferSpeedColumn(),
                progress.TotalFileSizeColumn(),
                progress.TextColumn(f"{local_path.name}"),
            ) as upload_progress:
                pbar_task = upload_progress.add_task(
                    "[green]Uploading ...", total=local_path.stat().st_size
                )

                def pbar_update(n):
                    upload_progress.update(task_id=pbar_task, advance=n)

                irods_session.data_objects.put(
                    local_path=local_path,
                    irods_path=dst_path,
                    updatables=(pbar_update,),
                )
        else:
            # pre prc 2.1.0 progressbar
            # utility iterator to read the local file in chunks: saves local disk space(!)
            # and feeds a progress bar
            def read_in_chuncks(file_handler, chunk_size=1024 * 1024 * 8):
                while True:
                    data = file_handler.read(chunk_size)
                    if not data:
                        break
                    yield data

            # make the local read buffer 32MB
            buffering = 32 * 1024 * 1024
            # open the file with cool 'Rich' progress bar as a console display asset which
            # implictely decorates a regular open()
            with progress.open(local_path, "rb", buffering=buffering) as f:
                with irods_session.data_objects.open(
                    dst_path, "w", auto_close=True
                ) as f_dst:
                    for chunk in read_in_chuncks(f):
                        f_dst.write(chunk)

        # implicit validation and explicit declaration outside the progress context
        result_object = irods_session.data_objects.get(dst_path)

        # the whole aftermath validation chain
        # with replica status, then size comparison and if requested the (cpu and i/o expensive)
        # checksum validation. The 'and' operation ensures if the "easier" validation rule fails,
        # the next expensive validation rule is not unnecessarily executed
        local_checksum = ""
        if (
            check_data_object_replica_status(result_object)
            and (result_object.size == local_path.stat().st_size)
            and (
                not verify_checksum
                or (
                    verify_checksum
                    and (
                        local_checksum := validate_checksums(
                            irods_session, str(local_path), dst_path
                        )
                    )
                )
            )
        ):
            logger.verbose(
                "Successfully uploaded local %s to irods %s", local_path, dst_path
            )
            metadata_dict = metadata.extract_metadata(local_path, metadata_handlers)
            if metadata_dict:
                bulk_add_metadata(
                    item=result_object, metadata_items=metadata_dict, prefix="mg."
                )
                logger.info(
                    "Added %s metadata items to %s",
                    len(metadata_dict),
                    result_object.name,
                )
            results.register_success(local_path, checksum=local_checksum)
            return result_object
        else:
            logger.error("Failed uploading %s to irods %s", local_path, dst_path)
            results.register_failed(local_path, checksum=local_checksum)
            return False
    except Exception:  # pylint: disable=broad-except
        logger.exception("Unexpected exception during upload")
        results.register_failed(local_path)
        return False
    finally:
        busy_uploading = False


def sync_to_irods(  # pylint: disable=redefined-builtin
    irods_session: iRODSSession | None,
    path: pathlib.Path,
    destination: str,
    recursive=False,
    regex=None,
    filter=None,
    filter_kwargs=None,
    glob="*",
    restart_paths=None,  # list of path strings
    ignore=None,
    verify_checksum=False,
    metadata_handlers=None,
    dry_run=False,
) -> dict:
    """Sync a local path to iRODS

    Inspired by Jef's sync script but adding + and - filters, including
    custom filters if requested. Also offers restart of previous failed transfers
    """
    if regex is None:
        regex = []
    if restart_paths is None:
        restart_paths = []
    if metadata_handlers is None:
        metadata_handlers = []

    path_objects = []
    # if there are restart paths to treat, add them
    if restart_paths:
        path_objects = [pathlib.Path(path) for path in restart_paths]
        logger.info("Restarting failed transfers for %s", restart_paths)
    if glob:
        path_objects += list(path.rglob(glob) if recursive else path.glob(glob))
        logger.info("Syncing %s with glob %s", path, glob)

    for path_object in path_objects:
        if path_object.is_file() and (full_path := path_object.absolute()):

            logger.verbose("Sync %s", full_path)
            if ignore and any(
                [re.search(pattern, str(full_path)) for pattern in ignore]
            ):
                logger.verbose("Ignoring %s", full_path)
                continue

            if check_filters(
                full_path,
                regexes=regex,
                filter=filter,
                filter_kwargs=filter_kwargs,
            ):
                results.register_matched(full_path)
                if dry_run:
                    logger.info("dry-run: would upload %s", full_path)
                    continue
                # check if the object already exists and has the same size
                # and if checksum checks are enabled, verify also the checksum
                # to decide to ignore
                try:
                    rel_local_path = full_path.relative_to(path)
                    irods_path = str(pathlib.PurePath(destination, str(rel_local_path)))
                    irods_data_object = irods_session.data_objects.get(irods_path)
                    if full_path.stat().st_size == irods_data_object.size:
                        if verify_checksum and validate_checksums(
                            irods_session, str(full_path), irods_path
                        ):
                            results.register_ignored(full_path)
                            logger.info(
                                "Ignoring existing data_object for %s, checksum and size match",
                                full_path,
                            )
                            continue
                        if not verify_checksum:
                            results.register_ignored(full_path)
                            logger.info(
                                "Ignoring existing data_object for %s, size matches",
                                full_path,
                            )
                            continue
                except Exception:  # pylint: disable=broad-except
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
                    results.register_success(full_path)
                else:
                    results.register_failed(full_path)
            else:
                results.register_ignored(full_path)
                continue
        else:
            logger.verbose("did not treat local dir %s", path_object)
    return results.get_results()


def check_data_object_replica_status(data_object: Optional[iRODSDataObject]) -> bool:
    """
    Check the data object status via its replica status.

    Args:
        data_object: iRODS data object to check

    Returns:
        bool: True if all replicas are valid
    """
    return (
        data_object is not None
        and data_object.replicas is not None
        and all(int(replica.status) == 1 for replica in data_object.replicas)
    )


def bulk_add_metadata(
    item: Union[iRODSDataObject, iRODSCollection],
    metadata_items: Dict[str, Union[str, List[str]]],
    unit_text: str = "analysis/mango_ingest",
    as_admin: bool = False,
    prefix: str = "",
) -> None:
    """
    Add or replace metadata AVU triplets.

    Args:
        item: iRODS object to add metadata to
        metadata_items: Dictionary of metadata items
        unit_text: Unit text for metadata
        as_admin: Whether to add metadata as admin
        prefix: Prefix for metadata names
    """
    if metadata_items:
        metadata_names = metadata_items.keys()
        avu_operations = [
            AVUOperation(operation="remove", avu=avu)
            for avu in item.metadata.items()
            if avu.name in metadata_names
        ]

        for m_name, m_value in metadata_items.items():
            m_name = prefix + m_name
            if isinstance(m_value, list):
                avu_operations.extend(
                    [
                        AVUOperation(
                            "add",
                            iRODSMeta(name=m_name, value=sub_value, units=unit_text),
                        )
                        for sub_value in m_value
                    ]
                )
            elif isinstance(m_value, str):
                avu_operations.append(
                    AVUOperation(
                        operation="add",
                        avu=iRODSMeta(name=m_name, value=m_value, units=unit_text),
                    )
                )
            else:
                raise ManGOMetadataError(
                    f"unknown_field_type for AVU operation: {type(m_value)}"
                )

        if len(avu_operations):
            logger.verbose("Adding metadata to %s: %s", item.name, metadata_items)
            item.metadata(admin=as_admin).apply_atomic_operations(*avu_operations)
