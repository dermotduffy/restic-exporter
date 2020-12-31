import attr
from dateutil import parser as dateutil_parser
import datetime
import logging
from typing import Any, Dict, List, Optional

from .const import (
    KEY_SNAPSHOT_HOSTNAME,
    KEY_SNAPSHOT_ID,
    KEY_SNAPSHOT_PATHS,
    KEY_SNAPSHOT_TAGS,
    KEY_SNAPSHOT_TIME,
    KEY_STATS_TOTAL_BLOB_COUNT,
    KEY_STATS_TOTAL_FILE_COUNT,
    KEY_STATS_TOTAL_SIZE,
    KEY_STATUS_BYTES_DONE,
    KEY_STATUS_BYTES_TOTAL,
    KEY_STATUS_FILES_DONE,
    KEY_STATUS_FILES_TOTAL,
    KEY_STATUS_PERCENT_DONE,
    KEY_STATUS_SECONDS_ELAPSED,
    KEY_STATUS_SECONDS_REMAINING,
    KEY_SUMMARY_DATA_ADDED,
    KEY_SUMMARY_DIRS_CHANGED,
    KEY_SUMMARY_DIRS_NEW,
    KEY_SUMMARY_DIRS_UNMODIFIED,
    KEY_SUMMARY_FILES_CHANGED,
    KEY_SUMMARY_FILES_NEW,
    KEY_SUMMARY_FILES_UNMODIFIED,
    KEY_SUMMARY_SNAPSHOT_ID,
    KEY_SUMMARY_TOTAL_BYTES_PROCESSED,
    KEY_SUMMARY_TOTAL_DURATION,
    KEY_SUMMARY_TOTAL_FILES_PROCESSED,
)


_LOGGER = logging.getLogger(__name__)
_LOGGER.level = logging.DEBUG


def convert_type_or_none(cast_type, val):
    return cast_type(val) if val is not None else val


def convert_int_or_none(val):
    return convert_type_or_none(int, val)


def convert_float_or_none(val):
    return convert_type_or_none(float, val)


def convert_str_or_none(val):
    return convert_type_or_none(str, val)


def validate_percent(_, __, val):
    if val is not None and (val < 0 or val > 1):
        raise ValueError(f"Not a valid percent: {val}")


def validate_positive(_, __, val):
    if val is not None and (val < 0):
        raise ValueError(f"Expected positive number: {val}")


def validate_non_empty_str(_, __, val):
    if val == "":
        raise ValueError(f"Expected non-empty string: {val}")


@attr.s
class ResticStats:
    total_size: int = attr.ib(converter=int, validator=[validate_positive])
    total_file_count: int = attr.ib(converter=int, validator=[validate_positive])
    total_blob_count: Optional[int] = attr.ib(
        default=None, converter=convert_int_or_none, validator=[validate_positive]
    )


def json_to_stats(stats_json: Dict[str, Any]) -> Optional[ResticStats]:
    if not stats_json:
        return

    blob_count = stats_json.get(KEY_STATS_TOTAL_BLOB_COUNT)
    try:
        return ResticStats(
            total_size=stats_json[KEY_STATS_TOTAL_SIZE],
            total_file_count=stats_json[KEY_STATS_TOTAL_FILE_COUNT],
            total_blob_count=blob_count,
        )
    except KeyError as ex:
        _LOGGER.warning(f"Skipping restic stats with missing key: {ex}")
    except ValueError as ex:
        _LOGGER.warning(f"Skipping restic stats with invalid value: {ex}")


@attr.s
class ResticStatsBundle:
    raw: ResticStats = attr.ib(validator=attr.validators.instance_of(ResticStats))
    restore: ResticStats = attr.ib(validator=attr.validators.instance_of(ResticStats))


@attr.s
class ResticSnapshotKeys:
    hostname: str = attr.ib(
        validator=[attr.validators.instance_of(str), validate_non_empty_str]
    )
    paths: List[str] = attr.ib(
        factory=list, validator=attr.validators.instance_of((type(None), list))
    )
    tags: List[str] = attr.ib(
        factory=list, validator=attr.validators.instance_of((type(None), list))
    )
    snapshot_id: str = attr.ib(
        default=None, converter=convert_str_or_none, validator=[validate_non_empty_str]
    )


@attr.s
class ResticSnapshot:
    key: ResticSnapshotKeys = attr.ib(
        validator=attr.validators.instance_of(ResticSnapshotKeys)
    )
    snapshot_time: datetime.datetime = attr.ib(
        validator=[attr.validators.instance_of(datetime.datetime)]
    )
    stats: Optional[ResticStatsBundle] = attr.ib(
        default=None,
        validator=attr.validators.instance_of((type(None), ResticStatsBundle)),
    )


def json_to_snapshot(snapshot_json: Dict[str, Any]) -> Optional[ResticSnapshot]:
    if not snapshot_json:
        return
    try:
        snapshot_time = snapshot_json[KEY_SNAPSHOT_TIME]
        try:
            snapshot_time = dateutil_parser.parse(snapshot_time)
        except (TypeError, dateutil_parser.ParserError) as e:
            _LOGGER.warning(f"Skipping unparseable snapshot time: {snapshot_time}")
            return
        return ResticSnapshot(
            key=ResticSnapshotKeys(
                hostname=snapshot_json[KEY_SNAPSHOT_HOSTNAME],
                paths=snapshot_json[KEY_SNAPSHOT_PATHS],
                tags=snapshot_json.get(KEY_SNAPSHOT_TAGS),
                snapshot_id=snapshot_json[KEY_SNAPSHOT_ID],
            ),
            snapshot_time=snapshot_time,
        )
    except KeyError as ex:
        _LOGGER.warning(f"Skipping snapshot with missing key: {ex}")


@attr.s
class ResticBackupStatus:
    key: ResticSnapshotKeys = attr.ib(
        validator=attr.validators.instance_of(ResticSnapshotKeys)
    )
    files_total: int = attr.ib(converter=int, validator=[validate_positive])
    bytes_total: int = attr.ib(converter=int, validator=[validate_positive])
    percent_done: float = attr.ib(
        default=None, converter=convert_float_or_none, validator=[validate_percent]
    )
    files_done: int = attr.ib(
        default=None, converter=convert_int_or_none, validator=[validate_positive]
    )
    bytes_done: int = attr.ib(
        default=None, converter=convert_int_or_none, validator=[validate_positive]
    )
    seconds_elapsed: int = attr.ib(
        default=None, converter=convert_int_or_none, validator=[validate_positive]
    )
    seconds_remaining: int = attr.ib(
        default=None, converter=convert_int_or_none, validator=[validate_positive]
    )


def json_to_backup_status(
    status_json, key: ResticSnapshotKeys
) -> Optional[ResticBackupStatus]:
    if not status_json:
        return
    try:
        return ResticBackupStatus(
            key=key,
            files_total=status_json[KEY_STATUS_FILES_TOTAL],
            bytes_total=status_json[KEY_STATUS_BYTES_TOTAL],
            percent_done=status_json.get(KEY_STATUS_PERCENT_DONE),
            files_done=status_json.get(KEY_STATUS_FILES_DONE),
            bytes_done=status_json.get(KEY_STATUS_BYTES_DONE),
            seconds_elapsed=status_json.get(KEY_STATUS_SECONDS_ELAPSED),
            seconds_remaining=status_json.get(KEY_STATUS_SECONDS_REMAINING),
        )
    except KeyError as ex:
        _LOGGER.warning(f"Skipping backup status with missing key: {ex}")
    except ValueError as ex:
        _LOGGER.warning(f"Skipping backup status with invalid value: {ex}")


@attr.s
class ResticBackupSummary:
    key: ResticSnapshotKeys = attr.ib(
        validator=attr.validators.instance_of(ResticSnapshotKeys)
    )
    files_new: int = attr.ib(converter=int, validator=[validate_positive])
    files_changed: int = attr.ib(converter=int, validator=[validate_positive])
    files_unmodified: int = attr.ib(converter=int, validator=[validate_positive])
    dirs_new: int = attr.ib(converter=int, validator=[validate_positive])
    dirs_changed: int = attr.ib(converter=int, validator=[validate_positive])
    dirs_unmodified: int = attr.ib(converter=int, validator=[validate_positive])
    data_added: int = attr.ib(converter=int, validator=[validate_positive])
    files_processed: int = attr.ib(converter=int, validator=[validate_positive])
    bytes_processed: int = attr.ib(converter=int, validator=[validate_positive])
    duration: float = attr.ib(converter=float, validator=[validate_positive])


def json_to_backup_summary(
    summary_json, key: ResticSnapshotKeys
) -> Optional[ResticBackupSummary]:
    if not summary_json:
        return
    try:
        key.snapshot_id = summary_json[KEY_SUMMARY_SNAPSHOT_ID]
        return ResticBackupSummary(
            key=key,
            files_new=summary_json[KEY_SUMMARY_FILES_NEW],
            files_changed=summary_json[KEY_SUMMARY_FILES_CHANGED],
            files_unmodified=summary_json[KEY_SUMMARY_FILES_UNMODIFIED],
            dirs_new=summary_json[KEY_SUMMARY_DIRS_NEW],
            dirs_changed=summary_json[KEY_SUMMARY_DIRS_CHANGED],
            dirs_unmodified=summary_json[KEY_SUMMARY_DIRS_UNMODIFIED],
            data_added=summary_json[KEY_SUMMARY_DATA_ADDED],
            files_processed=summary_json[KEY_SUMMARY_TOTAL_FILES_PROCESSED],
            bytes_processed=summary_json[KEY_SUMMARY_TOTAL_BYTES_PROCESSED],
            duration=summary_json[KEY_SUMMARY_TOTAL_DURATION],
        )
    except KeyError as ex:
        _LOGGER.warning(f"Skipping backup summary with missing key: {ex}")
    except ValueError as ex:
        _LOGGER.warning(f"Skipping backup summary with invalid value: {ex}")
