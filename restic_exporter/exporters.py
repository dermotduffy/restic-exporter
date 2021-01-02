"""Exporters of Restic statistics."""

import argparse
import os
import logging
from typing import Any, Dict, List, Optional
import influxdb  # type: ignore

from . import get_current_datetime

from .const import (
    DEFAULT_INFLUX_DATABASE,
    ENV_INFLUX_PASSWORD,
    EXPORTER_INFLUXDB,
    KEY_RAW_BLOB_COUNT,
    KEY_RAW_FILE_COUNT,
    KEY_RAW_SIZE,
    KEY_RESTORE_BLOB_COUNT,
    KEY_RESTORE_FILE_COUNT,
    KEY_RESTORE_SIZE,
    KEY_SNAPSHOT_SHORT_ID,
    KEY_STATUS_BYTES_DONE,
    KEY_STATUS_BYTES_TOTAL,
    KEY_STATUS_FILES_DONE,
    KEY_STATUS_FILES_TOTAL,
    KEY_STATUS_PERCENT_DONE,
    KEY_STATUS_SECONDS_ELAPSED,
    KEY_STATUS_SECONDS_REMAINING,
    KEY_SUMMARY_DATA_ADDED,
    KEY_SUMMARY_DATA_BLOBS,
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
    KEY_SUMMARY_TREE_BLOBS,
    MEASUREMENT_BACKUP_STATUS,
    MEASUREMENT_BACKUP_SUMMARY,
    MEASUREMENT_REPO_STATS,
    MEASUREMENT_SNAPSHOTS,
)

from .types import (
    ResticBackupStatus,
    ResticBackupSummary,
    ResticRepoStats,
    ResticSnapshot,
    ResticSnapshotKeys,
    ResticStatsBundle,
)

_LOGGER = logging.getLogger(__name__)


class Exporter:
    """Generic baseclass for Restic exporters."""

    @classmethod
    def add_args_to_parser(cls, args: argparse.ArgumentParser) -> None:
        """Add command line arguments to argument parser."""
        pass

    @classmethod
    def construct_from_args(cls, args: argparse.Namespace) -> "Exporter":
        """Construct an exporter from command line arguments."""
        pass

    def export(self, stats: List[Any]) -> None:
        """Export a statistic."""
        pass

    @classmethod
    def get_password(
        self, env_var: str, password_file_path: Optional[str] = None
    ) -> Optional[str]:
        """Get a password from a file or environmental variable."""
        if password_file_path is not None:
            return open(password_file_path).read().strip()
        if env_var in os.environ:
            return os.environ[env_var]
        return None

    def start(self) -> None:
        """Start an exporter."""
        pass


class ExporterInfluxDB(Exporter):
    """InfluxDB exporter."""

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

    def __init__(
        self,
        host: str,
        port: int,
        username: Optional[str],
        password: Optional[str],
        database: str,
    ):
        """Initialize InfluxDB exporter."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._client: Optional[influxdb.InfluxDBClient] = None

    @classmethod
    def add_args_to_parser(cls, ap: argparse.ArgumentParser) -> None:
        """Add command line arguments to argument parser."""
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
    def construct_from_args(cls, args: argparse.Namespace) -> "ExporterInfluxDB":
        """Construct an exporter from command line arguments."""
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

    def start(self) -> None:
        """Start an exporter."""
        _LOGGER.debug(
            f"Starting InfluxDB connection to {self._host}:{self._port} "
            f"for user {self._username} to database {self._database}"
        )
        self._client = influxdb.InfluxDBClient(
            self._host, self._port, self._username, self._password, self._database
        )
        self._client.create_database(self._database)

    def _submit_points(self, points: List[Dict[str, Any]]) -> None:
        """Submit an InfluxDB point."""
        if self._client is not None:
            _LOGGER.debug(f"Writing data to InfluxDB: {points} ")
            self._client.write_points(points)

    def _add_optional_fields(self, optional_fields: Dict[str, Any]) -> Dict[str, Any]:
        """Add optional fields to measurement data."""
        out = {}
        for key, value in optional_fields.items():
            if value is not None:
                out[key] = value
        return out

    def _get_fields_from_stats_bundle(
        self, stats_bundle: ResticStatsBundle
    ) -> Dict[str, Any]:
        """Get the Influx fields from a stats bundle."""
        fields = {}
        if stats_bundle.raw:
            fields.update(
                {
                    KEY_RAW_SIZE: stats_bundle.raw.total_size,
                    KEY_RAW_FILE_COUNT: stats_bundle.raw.total_file_count,
                }
            )
            fields.update(
                self._add_optional_fields(
                    {
                        KEY_RAW_BLOB_COUNT: stats_bundle.raw.total_blob_count,
                    }
                )
            )

        if stats_bundle.restore:
            fields.update(
                {
                    KEY_RESTORE_SIZE: stats_bundle.restore.total_size,
                    KEY_RESTORE_FILE_COUNT: stats_bundle.restore.total_file_count,
                }
            )
            fields.update(
                self._add_optional_fields(
                    {
                        KEY_RESTORE_BLOB_COUNT: stats_bundle.restore.total_blob_count,
                    }
                )
            )

        return fields

    def _export_snapshot(self, snapshot: ResticSnapshot) -> List[Dict[str, Any]]:
        """Export a snapshot object."""

        fields = {
            KEY_SNAPSHOT_SHORT_ID: snapshot.key.snapshot_id,
        }

        assert snapshot.stats is not None
        fields.update(self._get_fields_from_stats_bundle(snapshot.stats))

        point = {
            "measurement": MEASUREMENT_SNAPSHOTS,
            "tags": self._get_influx_tags_from_key(snapshot.key),
            "time": snapshot.snapshot_time,
            "fields": fields,
        }
        return [point]

    def _get_influx_tags_from_key(self, key: ResticSnapshotKeys) -> Dict[str, str]:
        """Get influx tags from a snapshot key."""
        tags = {
            "hostname": key.hostname,
            "paths": ",".join(key.paths),
        }
        if key.tags:
            tags["tags"] = ",".join(key.tags)
        return tags

    def _export_backup_status(self, stats: ResticBackupStatus) -> List[Dict[str, Any]]:
        """Export a backup status object."""
        fields = {
            KEY_STATUS_FILES_TOTAL: stats.files_total,
            KEY_STATUS_BYTES_TOTAL: stats.bytes_total,
        }
        optional_fields = {
            KEY_STATUS_PERCENT_DONE: stats.percent_done,
            KEY_STATUS_FILES_DONE: stats.files_done,
            KEY_STATUS_BYTES_DONE: stats.bytes_done,
            KEY_STATUS_SECONDS_ELAPSED: stats.seconds_elapsed,
            KEY_STATUS_SECONDS_REMAINING: stats.seconds_elapsed,
        }
        fields.update(self._add_optional_fields(optional_fields))

        point = {
            "measurement": MEASUREMENT_BACKUP_STATUS,
            "tags": self._get_influx_tags_from_key(stats.key),
            "time": get_current_datetime(),
            "fields": fields,
        }
        return [point]

    def _export_backup_summary(
        self, stats: ResticBackupSummary
    ) -> List[Dict[str, Any]]:
        """Export a backup summary object."""
        fields = {
            KEY_SUMMARY_FILES_NEW: stats.files_new,
            KEY_SUMMARY_FILES_CHANGED: stats.files_changed,
            KEY_SUMMARY_FILES_UNMODIFIED: stats.files_unmodified,
            KEY_SUMMARY_DIRS_NEW: stats.dirs_new,
            KEY_SUMMARY_DIRS_CHANGED: stats.dirs_changed,
            KEY_SUMMARY_DIRS_UNMODIFIED: stats.dirs_unmodified,
            KEY_SUMMARY_DATA_ADDED: stats.data_added,
            KEY_SUMMARY_DATA_BLOBS: stats.data_blobs,
            KEY_SUMMARY_SNAPSHOT_ID: stats.key.snapshot_id,
            KEY_SUMMARY_TOTAL_FILES_PROCESSED: stats.files_processed,
            KEY_SUMMARY_TOTAL_BYTES_PROCESSED: stats.bytes_processed,
            KEY_SUMMARY_TOTAL_DURATION: stats.duration,
            KEY_SUMMARY_TREE_BLOBS: stats.tree_blobs,
        }
        point = {
            "measurement": MEASUREMENT_BACKUP_SUMMARY,
            "tags": self._get_influx_tags_from_key(stats.key),
            "time": get_current_datetime(),
            "fields": fields,
        }
        return [point]

    def _export_repo(self, repo: ResticRepoStats) -> List[Dict[str, Any]]:
        """Export a backup summary object."""
        fields = self._get_fields_from_stats_bundle(repo.stats)
        if not fields:
            return []

        point = {
            "measurement": MEASUREMENT_REPO_STATS,
            "time": get_current_datetime(),
            "fields": fields,
        }
        return [point]

    def export(self, stats: List[Any]) -> None:
        """Export a statistics object."""
        points = []
        for stat in stats:
            if isinstance(stat, ResticBackupStatus):
                points.extend(self._export_backup_status(stat))
            elif isinstance(stat, ResticBackupSummary):
                points.extend(self._export_backup_summary(stat))
            elif isinstance(stat, ResticSnapshot):
                points.extend(self._export_snapshot(stat))
            elif isinstance(stat, ResticRepoStats):
                points.extend(self._export_repo(stat))
            else:
                _LOGGER.warning(f"ExporterInfluxDB cannot handle stats of type: {stat}")
        self._submit_points(points)


EXPORTERS = {
    EXPORTER_INFLUXDB: ExporterInfluxDB,
}
