#!/usr/bin/env python
"""Statistics exporter for restic backups."""

import argparse
import datetime
import re
import sys
import json
import logging
import subprocess
from typing import Any, List, Optional, Union

from . import get_current_datetime

from .const import (
    KEY_COMMAND_SNAPSHOTS,
    KEY_COMMAND_STATS,
    KEY_MESSAGE_TYPE,
    KEY_MESSAGE_TYPE_STATUS,
    KEY_MESSAGE_TYPE_SUMMARY,
    KEY_MODE_RAW_DATA,
    KEY_MODE_RESTORE_SIZE,
    KEY_SNAPSHOTS,
)

from .exporters import EXPORTERS

from .types import (
    ResticBackupStatus,
    ResticBackupSummary,
    ResticRepoStats,
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


class ResticExecutor:
    """Executes restic commands."""

    def __init__(
        self, restic_binary: str, restic_args: Optional[List[str]] = None
    ) -> None:
        """Initialize Restic Executor."""
        self._restic_binary = restic_binary
        self._restic_args = restic_args or []

    def _run_command(self, args: List[str]) -> Any:
        """Run a Restic command."""
        args = [self._restic_binary, "--json"] + self._restic_args + args
        _LOGGER.debug(f"Running: {args}")
        out = subprocess.run(args, capture_output=True)
        _LOGGER.debug(
            f">> Result (rc={out.returncode}): {out.stdout!r}, {out.stderr!r}"
        )
        if out.returncode != 0:
            _LOGGER.error(
                'Command failed ("'
                + " ".join(args)
                + f'") with exit code {out.returncode}, '
                + f"stdout: {out.stdout!r}, stderr: {out.stderr!r}"
            )
            return None

        try:
            return json.loads(out.stdout)
        except (ValueError, json.decoder.JSONDecodeError):
            _LOGGER.error(f"{args} yielded non-JSON output: {out.stdout!r}")
        return None

    def get_stats(
        self,
        mode: str,
        snapshot_ids: Optional[List[str]] = None,
    ) -> Optional[ResticStats]:
        """Get Restic statistics JSON."""
        return json_to_stats(
            self._run_command(
                [KEY_COMMAND_STATS, f"--mode={mode}"] + (snapshot_ids or [])
            )
        )

    def get_snapshots(self, group_by: str, last: bool) -> List[ResticSnapshot]:
        """Get Restic snapshots JSON."""
        result = self._run_command(
            [KEY_COMMAND_SNAPSHOTS]
            + ([f"--group-by={group_by}"])
            + (["--last"] if last else [])
        )
        snapshots: List[ResticSnapshot] = []
        for grouped_snapshot in result or []:
            for json_snapshot in grouped_snapshot.get(KEY_SNAPSHOTS) or []:
                snapshot = json_to_snapshot(json_snapshot)
                if snapshot:
                    snapshots.append(snapshot)

        if not snapshots:
            _LOGGER.warning(f"No valid snapshots found in JSON: {result}")
        return snapshots


class ResticStatsGenerator:
    """Generate Restic statistics."""

    def __init__(
        self,
        executor: ResticExecutor,
        group_by: str,
        last: bool,
        backup_status_window_seconds: int,
    ):
        """Initialize Restic statistics generator."""
        self._executor = executor
        self._group_by = group_by
        self._last = last
        self._backup_status_window_seconds: int = backup_status_window_seconds

        self._backup_status_last_update: Optional[datetime.datetime] = None

    def _generate_last_status_from_summary(
        self, summary: ResticBackupSummary
    ) -> ResticBackupStatus:
        """Generate a status object from a backup summary."""
        return ResticBackupStatus(
            key=summary.key,
            files_total=summary.files_processed,
            bytes_total=summary.bytes_processed,
            percent_done=1.0,
            files_done=summary.files_processed,
            bytes_done=summary.bytes_processed,
            seconds_elapsed=int(summary.duration),
        )

    def get_piped_stats(
        self, line: str, key: ResticSnapshotKeys
    ) -> List[Union[ResticBackupStatus, ResticBackupSummary]]:
        """Get statistics based on data piped in."""
        try:
            data = json.loads(line)
        except (ValueError, json.decoder.JSONDecodeError):
            return []
        if KEY_MESSAGE_TYPE not in data:
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
            status = json_to_backup_status(data, key)
            if status:
                return [status]
        elif data[KEY_MESSAGE_TYPE] == KEY_MESSAGE_TYPE_SUMMARY:
            summary = json_to_backup_summary(data, key)
            if not summary:
                return []
            return [self._generate_last_status_from_summary(summary), summary]
        return []

    def get_snapshot_stats(self) -> List[ResticSnapshot]:
        """Get Restic snapshots data."""
        snapshots = self._executor.get_snapshots(
            group_by=self._group_by, last=self._last
        )
        for snapshot in snapshots:
            assert snapshot.key.snapshot_id
            snapshot.stats = ResticStatsBundle(
                raw=self._executor.get_stats(
                    snapshot_ids=[snapshot.key.snapshot_id], mode=KEY_MODE_RAW_DATA
                ),
                restore=self._executor.get_stats(
                    snapshot_ids=[snapshot.key.snapshot_id], mode=KEY_MODE_RESTORE_SIZE
                ),
            )
        return snapshots

    def get_repo_stats(self) -> List[ResticRepoStats]:
        """Get Restic repository statistics."""
        return [
            ResticRepoStats(
                stats=ResticStatsBundle(
                    raw=self._executor.get_stats(mode=KEY_MODE_RAW_DATA),
                    restore=self._executor.get_stats(mode=KEY_MODE_RESTORE_SIZE),
                )
            )
        ]


def split_arg(arg: str) -> Optional[List[str]]:
    """Split an argument into multiple."""
    if not arg:
        return None
    return re.split(r"[,\s]", arg)


def get_snapshot_key_from_args(
    ap: argparse.ArgumentParser, args: argparse.Namespace
) -> ResticSnapshotKeys:
    """Generate a snapshot key from the command line arguments."""

    if args.backup_host is None:
        _LOGGER.error("Backup host must be provided (--backup-host)")
        ap.print_usage()
        ap.exit()

    if args.backup_path is None:
        _LOGGER.error("Backup path must be provided (--backup-path)")
        ap.print_usage()
        ap.exit()
    paths = split_arg(args.backup_path)
    assert paths

    tags = None
    if args.backup_tag:
        tags = split_arg(args.backup_tag)

    return ResticSnapshotKeys(
        hostname=args.backup_host,
        paths=paths,
        tags=tags,
    )


def main() -> None:
    """Restic Exporter main."""
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--restic-binary",
        default="restic",
        help="Path to restic binary",
    )
    ap.add_argument(
        "--restic-args",
        help="Arbitrary argument to pass to restic calls (comma/space separated)",
        default=None,
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
        help="Tags to attach to stats piped from backup (comma/space separated)",
    )
    ap.add_argument(
        "--backup-path",
        help="Paths to attach to stats piped from backup (comma/space separated)",
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
        executor=ResticExecutor(
            restic_binary=args.restic_binary, restic_args=split_arg(args.restic_args)
        ),
        group_by=args.group_by,
        last=not args.all,
        backup_status_window_seconds=args.backup_status_window_seconds,
    )

    stats: List[
        Union[ResticBackupStatus, ResticBackupSummary, ResticRepoStats, ResticSnapshot]
    ] = []

    if not sys.stdin.isatty():
        key = get_snapshot_key_from_args(ap, args)
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            stats.extend(generator.get_piped_stats(line, key))
            for exporter in exporters:
                exporter.export(stats)
    else:
        stats.extend(generator.get_snapshot_stats())
        stats.extend(generator.get_repo_stats())
        for exporter in exporters:
            exporter.export(stats)


if __name__ == "__main__":
    main()
