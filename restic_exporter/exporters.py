#!/usr/bin/env python

import argparse
import datetime
import os
import logging
from typing import Any, Dict, List, Optional, Union
import influxdb

from .const import (
    DEFAULT_INFLUX_DATABASE,
    ENV_INFLUX_PASSWORD,
    EXPORTER_INFLUXDB,
    KEY_COMMAND_SNAPSHOTS,
    KEY_COMMAND_STATS,
    KEY_MESSAGE_TYPE,
    KEY_MESSAGE_TYPE_STATUS,
    KEY_MESSAGE_TYPE_SUMMARY,
    KEY_MODE_RAW_DATA,
    KEY_MODE_RESTORE_SIZE,
    KEY_RAW_BLOB_COUNT,
    KEY_RAW_FILE_COUNT,
    KEY_RAW_SIZE,
    KEY_RESTORE_BLOB_COUNT,
    KEY_RESTORE_FILE_COUNT,
    KEY_RESTORE_SIZE,
    KEY_SNAPSHOT_ID,
    KEY_SNAPSHOTS,
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
    MEASUREMENT_BACKUP_STATUS,
    MEASUREMENT_BACKUP_SUMMARY,
    MEASUREMENT_SNAPSHOTS,
    MEASUREMENT_REPO,
)

from .types import (
    ResticBackupStatus,
    ResticBackupSummary,
    ResticSnapshot,
    ResticSnapshotKeys,
    ResticStats,
    ResticStatsBundle,
    json_to_stats,
    json_to_snapshot,
    json_to_backup_status,
    json_to_backup_summary,
)

_LOGGER = logging.getLogger(__name__)
_LOGGER.level = logging.DEBUG


class Exporter:
    @classmethod
    def add_args_to_parser(cls, args):
        pass

    @classmethod
    def construct_from_args(cls, args):
        pass

    def export(self, stats):
        pass

    @classmethod
    def get_password(self, env_var: str, password_file_path: str = None) -> Optional[str]:
        if password_file_path is not None:
            return open(password_file_path).read().strip()
        if env_var in os.environ:
            return os.environ[env_var]

    def start(self):
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
    def add_args_to_parser(cls, ap: argparse.ArgumentParser) -> None:
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
        self._client = influxdb.InfluxDBClient(
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
    EXPORTER_INFLUXDB: ExporterInfluxDB,
}