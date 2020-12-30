#!/usr/bin/env python

import argparse
import attr
from dateutil import parser as dateutil_parser
import datetime
import os
import re
import sys
import json
import logging
import subprocess
from typing import Any, Dict, List, Optional, Union
from influxdb import InfluxDBClient

_LOGGER = logging.getLogger(__name__)
_LOGGER.level = logging.DEBUG

KEY_SNAPSHOT_TIME = "time"
KEY_SNAPSHOT_PATHS = "paths"
KEY_SNAPSHOT_HOSTNAME = "hostname"
KEY_SNAPSHOT_TAGS = "tags"
KEY_SNAPSHOT_ID = "id"
KEY_SNAPSHOT_SHORT_ID = "short_id"
KEY_GROUP_KEY = "group_key"
KEY_SNAPSHOTS = "snapshots"

KEY_STATS_TOTAL_SIZE = "total_size"
KEY_STATS_TOTAL_FILE_COUNT = "total_file_count"
KEY_STATS_TOTAL_BLOB_COUNT = "total_blob_count"

KEY_GROUP_BY_HOST = "hostname"
KEY_GROUP_BY_TAGS = "tags"
KEY_GROUP_BY_PATHS = "paths"

KEY_COMMAND_SNAPSHOTS = "snapshots"
KEY_COMMAND_STATS = "stats"

KEY_MESSAGE_TYPE = "message_type"
KEY_MESSAGE_TYPE_STATUS = "status"
KEY_MESSAGE_TYPE_SUMMARY = "summary"

KEY_MODE_RESTORE_SIZE = "restore-size"
KEY_MODE_RAW_DATA = "raw-data"

MEASUREMENT_BACKUP_STATUS = "restic_backup_status"
MEASUREMENT_BACKUP_SUMMARY = "restic_backup_summary"
MEASUREMENT_SNAPSHOTS = "restic_snapshots"
MEASUREMENT_REPO = "restic_repo"

KEY_RAW_SIZE = "raw_size"
KEY_RAW_FILE_COUNT = "raw_file_count"
KEY_RAW_BLOB_COUNT = "raw_blob_count"
KEY_RESTORE_SIZE = "restore_size"
KEY_RESTORE_FILE_COUNT = "restore_file_count"
KEY_RESTORE_BLOB_COUNT = "restore_blob_count"

ENV_INFLUX_PASSWORD = "INFLUXDB_PASSWORD"

DEFAULT_INFLUX_DATABASE = "restic"

# TODO restic args arbitrary (e.g. -r --repository_file)
# TODO snapshot id from backup summary same in snapshots?


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
            _LOGGER.warning(f"Skipping unparseable snapshot time: {restic_time}")
            return
        print(snapshot_json)
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


KEY_STATUS_FILES_TOTAL = "total_files"
KEY_STATUS_FILES_DONE = "files_done"
KEY_STATUS_BYTES_TOTAL = "total_bytes"
KEY_STATUS_BYTES_DONE = "bytes_done"
KEY_STATUS_PERCENT_DONE = "percent_done"
KEY_STATUS_SECONDS_ELAPSED = "seconds_elapsed"
KEY_STATUS_SECONDS_REMAINING = "seconds_remaining"


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


KEY_SUMMARY_FILES_NEW = "files_new"
KEY_SUMMARY_FILES_CHANGED = "files_changed"
KEY_SUMMARY_FILES_UNMODIFIED = "files_unmodified"
KEY_SUMMARY_DIRS_NEW = "dirs_new"
KEY_SUMMARY_DIRS_CHANGED = "dirs_changed"
KEY_SUMMARY_DIRS_UNMODIFIED = "dirs_unmodified"
KEY_SUMMARY_DATA_ADDED = "data_added"
KEY_SUMMARY_TOTAL_FILES_PROCESSED = "total_files_processed"
KEY_SUMMARY_TOTAL_BYTES_PROCESSED = "total_bytes_processed"
KEY_SUMMARY_TOTAL_DURATION = "total_duration"
KEY_SUMMARY_SNAPSHOT_ID = "snapshot_id"


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
    duration: int = attr.ib(converter=int, validator=[validate_positive])


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


class ResticExecutor:
    def __init__(self, path_binary: str) -> None:
        self._path_binary = path_binary

    def _run_command(self, args: List[str]) -> Optional[Dict[str, Any]]:
        args = [self._path_binary, "--json"] + args
        _LOGGER.debug(f"Running: {args}")
        out = subprocess.run(args, capture_output=True)
        _LOGGER.debug(f">> Result (rc={out.returncode}): {out.stdout}, {out.stderr}")
        if out.returncode != 0:
            _LOGGER.error(
                'Command failed ("'
                + " ".join(args)
                + f'") with exit code {out.returncode}, '
                + f"stdout: {out.stdout}, stderr: {out.stderr}"
            )
            return

        try:
            return json.loads(out.stdout)
        except (ValueError, json.decoder.JSONDecodeError):
            _LOGGER.error(f"{args} yielded non-JSON output: {out.stdout}")
            return

    def get_stats(
        self,
        mode: str,
        snapshot_ids: Optional[List[str]] = None,
    ) -> Optional[ResticStats]:
        return json_to_stats(
            self._run_command(
                [KEY_COMMAND_STATS, f"--mode={mode}"] + (snapshot_ids or [])
            )
        )

    def get_snapshots(self, group_by, last) -> List[ResticSnapshot]:
        result = self._run_command(
            [KEY_COMMAND_SNAPSHOTS]
            + ([f"--group-by={group_by}"])
            + (["--last"] if last else [])
        )
        snapshots = []
        for grouped_snapshot in result or []:
            for snapshot in grouped_snapshot.get(KEY_SNAPSHOTS) or []:
                snapshots += [json_to_snapshot(snapshot)] or []

        if not snapshots:
            _LOGGER.warning(f"No valid snapshots found in JSON: {result}")
        return snapshots


class ResticStatsGenerator:
    def __init__(self, executor, group_by, last, backup_status_window_seconds):
        self._executor = executor
        self._group_by = group_by
        self._last = last
        self._backup_status_window_seconds: int = backup_status_window_seconds

        self._backup_status_last_update: datetime.datetime = None

    def _generate_last_status_from_summary(
        self, summary: ResticBackupSummary
    ) -> ResticBackupStatus:
        return ResticBackupStatus(
            key=summary.key,
            files_total=summary.files_processed,
            bytes_total=summary.bytes_processed,
            percent_done=1.0,
            files_done=summary.files_processed,
            bytes_done=summary.bytes_processed,
            seconds_elapsed=summary.duration,
        )

    def get_piped_stats(
        self, line: bytes, key: ResticSnapshotKeys
    ) -> List[Union[ResticBackupStatus, ResticBackupSummary]]:
        try:
            data = json.loads(line)
        except (ValueError, json.decoder.JSONDecodeError):
            return []
        if not KEY_MESSAGE_TYPE in data:
            return []

        if data[KEY_MESSAGE_TYPE] == KEY_MESSAGE_TYPE_STATUS:
            now = datetime.datetime.now()
            if (
                self._backup_status_last_update is not None
                and self._backup_status_window_seconds > 0
                and now
                < (
                    self._backup_status_last_update
                    + datetime.timedelta(seconds=self._backup_status_window_seconds)
                )
            ):
                return []
            self._backup_status_last_update = now
            return [json_to_backup_status(data, key)]
        elif data[KEY_MESSAGE_TYPE] == KEY_MESSAGE_TYPE_SUMMARY:
            summary = json_to_backup_summary(data, key)
            if not summary:
                return []
            return [self._generate_last_status_from_summary(summary), summary]

    def get_snapshot_stats(self) -> List[ResticSnapshot]:
        snapshots = self._executor.get_snapshots(
            group_by=self._group_by, last=self._last
        )
        for snapshot in snapshots:
            snapshot.stats = ResticStatsBundle(
                raw=self._executor.get_stats(
                    snapshot_ids=[snapshot.key.snapshot_id], mode=KEY_MODE_RAW_DATA
                ),
                restore=self._executor.get_stats(
                    snapshot_ids=[snapshot.key.snapshot_id], mode=KEY_MODE_RESTORE_SIZE
                ),
            )
        return snapshots

    def _get_repo_stats(self, mode) -> ResticStats:
        return self._executor.get_stats(mode=mode)


class Exporter:
    @classmethod
    def get_password(cls, env_var: str, password_file: str = None) -> Optional[str]:
        if password_file is not None:
            return open(password_file).read().strip()
        if env_var in os.environ:
            return os.environ[env_var]

    @classmethod
    def construct_from_args(cls, args):
        pass

    def start(self):
        pass

    def export(self, stats):
        pass


class ExporterInfluxDB(Exporter):
    # restic_backup_progress:
    #   hostname
    #   paths
    #   tags
    #   percent_done: 0-1
    #   total_files: n
    #   files_done: <= n
    #   total_bytes: n
    #   bytes_done: <= n
    #   seconds_elapsed: n
    # restic_backup_completions
    #   hostname
    #   paths
    #   tags
    #   snapshot_id
    #   files_new
    #   files_changed
    #   files_unmodified
    #   dirs_new
    #   dirs_changed
    #   dirs_unmodified
    #   data_added: bytes
    #   total_files_processed
    #   total_bytes
    # restic_snapshots
    #   time
    #   hostname
    #   paths
    #   tags
    #   raw_size_bytes
    #   raw_size_file_count
    #   restore_size_bytes
    #   restore_size_file_count
    # restic_repo
    #   raw_size_bytes
    #   raw_size_file_count
    #   restore_size_bytes
    #   restore_size_file_count

    def __init__(self, host, port, username, password, database):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._client = None

    @classmethod
    def construct_from_args(cls, args):
        password = Exporter.get_password(
            ENV_INFLUX_PASSWORD, args.influxdb_password_file
        )

        return ExporterInfluxDB(
            host=args.influxdb_host,
            port=args.influxdb_port,
            username=args.influxdb_username,
            password=password,
            database=args.influxdb_database,
        )

    def start(self):
        _LOGGER.debug(
            f"Starting InfluxDB connection to {self._host}:{self._port} "
            f"for user {self._username} to database {self._database}"
        )
        _LOGGER.debug(f"Password is {self._password}")
        self._client = InfluxDBClient(
            self._host, self._port, self._username, self._password, self._database
        )
        self._client.create_database(self._database)

    def _submit_point(self, point):
        _LOGGER.debug(f"Writing data to InfluxDB: {point} ")
        self._client.write_points([point])

    def _export_snapshot(self, snapshot: ResticSnapshot) -> None:
        fields = {
            KEY_RAW_SIZE: int(snapshot.stats.raw.total_size),
            KEY_RAW_FILE_COUNT: int(snapshot.stats.raw.total_file_count),
            KEY_RESTORE_SIZE: int(snapshot.stats.restore.total_size),
            KEY_RESTORE_FILE_COUNT: int(snapshot.stats.restore.total_file_count),
            KEY_SNAPSHOT_ID: snapshot.key.snapshot_id,
        }
        if snapshot.stats.raw.total_blob_count is not None:
            fields[KEY_RAW_BLOB_COUNT] = snapshot.stats.raw.total_blob_count
        if snapshot.stats.restore.total_blob_count is not None:
            fields[KEY_RESTORE_BLOB_COUNT] = snapshot.stats.restore.total_blob_count

        point = {
            "measurement": MEASUREMENT_SNAPSHOTS,
            "tags": self._get_influx_tags_from_key(snapshot.key),
            "time": snapshot.snapshot_time,
            "fields": fields,
        }
        self._submit_point(point)

    def _get_influx_tags_from_key(self, key):
        tags = {
            "hostname": key.hostname,
            "paths": ",".join(key.paths),
        }
        if key.tags:
            tags["tags"] = ",".join(key.tags)
        return tags

    def _export_backup_status(self, stats):
        fields = {
            KEY_STATUS_FILES_TOTAL: stats.files_total,
            KEY_STATUS_BYTES_TOTAL: stats.bytes_total,
        }
        optional_fields = (
            (KEY_STATUS_PERCENT_DONE, stats.percent_done),
            (KEY_STATUS_FILES_DONE, stats.files_done),
            (KEY_STATUS_BYTES_DONE, stats.bytes_done),
            (KEY_STATUS_SECONDS_ELAPSED, stats.seconds_elapsed),
            (KEY_STATUS_SECONDS_REMAINING, stats.seconds_elapsed),
        )
        for key, value in optional_fields:
            if value is not None:
                fields[key] = value

        point = {
            "measurement": MEASUREMENT_BACKUP_STATUS,
            "tags": self._get_influx_tags_from_key(stats.key),
            "time": datetime.datetime.now(),
            "fields": fields,
        }
        self._submit_point(point)

    def _export_backup_summary(self, stats):
        fields = {
            KEY_SUMMARY_FILES_NEW: stats.files_new,
            KEY_SUMMARY_FILES_CHANGED: stats.files_changed,
            KEY_SUMMARY_FILES_UNMODIFIED: stats.files_unmodified,
            KEY_SUMMARY_DIRS_NEW: stats.dirs_new,
            KEY_SUMMARY_DIRS_CHANGED: stats.dirs_changed,
            KEY_SUMMARY_DIRS_UNMODIFIED: stats.dirs_unmodified,
            KEY_SUMMARY_DATA_ADDED: stats.data_added,
            KEY_SUMMARY_TOTAL_FILES_PROCESSED: stats.files_processed,
            KEY_SUMMARY_TOTAL_BYTES_PROCESSED: stats.bytes_processed,
            KEY_SUMMARY_TOTAL_DURATION: stats.duration,
            KEY_SUMMARY_SNAPSHOT_ID: stats.key.snapshot_id,
        }
        point = {
            "measurement": MEASUREMENT_BACKUP_SUMMARY,
            "tags": self._get_influx_tags_from_key(stats.key),
            "time": datetime.datetime.now(),
            "fields": fields,
        }
        self._submit_point(point)

    def export(self, stats):
        if isinstance(stats, ResticBackupStatus):
            return self._export_backup_status(stats)
        elif isinstance(stats, ResticBackupSummary):
            return self._export_backup_summary(stats)
        elif isinstance(stats, ResticSnapshot):
            return self._export_snapshot(stats)
        _LOGGER.warning(f"ExporterInfluxDB cannot handle stats of type: {stats}")


EXPORTERS = {
    "influxdb": ExporterInfluxDB,
}


def get_snapshot_key_from_args(
    ap: argparse.ArgumentParser, args: argparse.Namespace
) -> ResticSnapshotKeys:
    def split_args(args: List[str]) -> List[str]:
        out = []
        for arg in args if isinstance(args, list) else [args]:
            out.extend(re.split("[,\s]", arg))
        return out

    if args.backup_host is None:
        _LOGGER.error("Backup host must be provided (--backup-host)")
        ap.print_usage()
        ap.exit()

    paths = tags = None
    if args.backup_path:
        paths = split_args(args.backup_path)
    if args.backup_tag:
        tags = split_args(args.backup_tag)
    return ResticSnapshotKeys(
        hostname=args.backup_host,
        paths=paths,
        tags=tags,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-r", "--restic_path", default="restic", help="Path to restic binary"
    )
    ap.add_argument(
        "-l",
        "--loglevel",
        default="ERROR",
        help="Logging level. Default: ERROR.",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
    )
    ap.add_argument(
        "-g",
        "--group-by",
        default="host,path",
        help="string for grouping snapshots by host,paths,tags. See restic documentation.",
    )
    ap.add_argument(
        "--all",
        help="Whether to export all stats, rather than just the last backup",
        default=False,
        action="store_true",
    )
    ap.add_argument(
        "exporters", nargs="+", help="Exporters to output to.", choices=EXPORTERS.keys()
    )
    ap.add_argument(
        "--influxdb-host",
        default="localhost",
        help="InfluxDB hostname",
    )
    ap.add_argument(
        "--influxdb-port",
        help="InfluxDB port",
        type=int,
        default=8086,
    )
    ap.add_argument(
        "--influxdb-username",
        help="InfluxDB user",
    )
    ap.add_argument(
        "--influxdb-password-file",
        help="InfluxDB password file.",
    )
    ap.add_argument(
        "--influxdb-database",
        default=DEFAULT_INFLUX_DATABASE,
        help="InfluxDB database",
    )
    ap.add_argument("--backup-host", help="Host to attach to stats piped from backup.")
    ap.add_argument(
        "--backup-tag",
        nargs="*",
        action="extend",
        default=None,
        help="Tags to attach to stats piped from backup (comma/space separated, or specified multiple times)",
    )
    ap.add_argument(
        "--backup-path",
        required=True,
        nargs="+",
        action="extend",
        default=None,
        help="Paths to attach to stats piped from backup (comma/space separated, or specified multiple times)",
    )
    ap.add_argument(
        "--backup-status-window-seconds",
        type=int,
        default=10,
        help="1 status update is allowed per window, set to 0 for no limit.",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.getLevelName(args.loglevel),
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d] %(message)s",
        datefmt="%F %H:%M:%S",
    )

    exporters = []
    for exporter_key in args.exporters:
        exporter = EXPORTERS[exporter_key].construct_from_args(args)
        if exporter:
            exporter.start()
            exporters.append(exporter)

    generator = ResticStatsGenerator(
        executor=ResticExecutor(path_binary=args.restic_path),
        group_by=args.group_by,
        last=not args.all,
        backup_status_window_seconds=args.backup_status_window_seconds,
    )

    if not sys.stdin.isatty():
        key = get_snapshot_key_from_args(ap, args)
        for line in sys.stdin:
            if not line:
                break
            stats = generator.get_piped_stats(line, key)
            for exporter in exporters:
                for stat in stats:
                    exporter.export(stat)
    else:
        stats = generator.get_snapshot_stats()
        for exporter in exporters:
            for stat in stats:
                exporter.export(stat)