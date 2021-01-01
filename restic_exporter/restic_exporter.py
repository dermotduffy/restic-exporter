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

from . import get_current_datetime

from .const import (
    DEFAULT_INFLUX_DATABASE,
    ENV_INFLUX_PASSWORD,
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

from .exporters import EXPORTERS

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


# TODO restic args arbitrary (e.g. -r --repository_file)
# TODO snapshot id from backup summary same in snapshots?
# TODO verify consts, may need to rename a few.
# TODO get restic tables as close to json output as possible.
# TODO action=extent may not work in Python 3.7?

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
        snapshot_ids: Optional[List[ResticStats]] = None,
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
            now = get_current_datetime()
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
        return []

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

    # TODO: Add repo stats.
    # def _get_repo_stats(self, mode) -> ResticStats:
    #     return self._executor.get_stats(mode=mode)


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


def main():
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

    for exporter_key in EXPORTERS:
        EXPORTERS[exporter_key].add_args_to_parser(ap)

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
        while True:
            line = sys.stdin.readline()
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

if __name__ == "__main__":
    main()