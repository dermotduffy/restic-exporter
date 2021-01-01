#!/usr/bin/python
"""Test for the restic-exporter."""

import argparse
import datetime
import json
import os
import pytest
import logging
import subprocess
import string
import sys
from typing import cast, Any, Dict, List, Optional, Tuple
from unittest import mock

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

from restic_exporter.restic_exporter import (
    get_snapshot_key_from_args,
    main,
    EXPORTERS,
    ResticExecutor,
    ResticBackupStatus,
    ResticSnapshotKeys,
    ResticStatsBundle,
    ResticStatsGenerator,
)
from restic_exporter.types import (
    json_to_backup_status,
    json_to_snapshot,
    json_to_stats,
    json_to_backup_summary,
)
from restic_exporter import get_current_datetime

from . import (
    dict_without,
    TEST_STATS_DATA_RAW,
    TEST_STATS_DATA_RESTORE,
    TEST_GROUPED_SNAPSHOT_DATA,
    TEST_SNAPSHOT_DATA,
    TEST_BACKUP_STATUS_DATA,
    TEST_BACKUP_SUMMARY_DATA,
)

# TODO trim
from restic_exporter.const import (
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


def test_get_current_datetime():
    dt = get_current_datetime()
    assert dt
    assert type(dt) == datetime.datetime


def _get_completed_process(args=[], rc=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args, returncode=rc, stdout=stdout, stderr=stderr
    )


@mock.patch("restic_exporter.restic_exporter.subprocess")
def test_restic_executor_get_stats(mock_subprocess, caplog):
    path_binary = "/path/to/binary"
    expected_args = [path_binary, "--json", "stats", "--mode=raw-data"]
    output = TEST_STATS_DATA_RAW

    r = ResticExecutor(path_binary)

    # Test: Success.
    mock_subprocess.run = mock.Mock(
        return_value=_get_completed_process(
            args=expected_args,
            rc=0,
            stdout=json.dumps(output),
        )
    )
    stats = r.get_stats(mode=KEY_MODE_RAW_DATA)

    mock_subprocess.run.assert_called_with(expected_args, capture_output=True)
    assert stats == json_to_stats(output)

    # Test: Non-zero return code.
    mock_subprocess.run = mock.Mock(
        return_value=_get_completed_process(
            args=expected_args,
            rc=1,
        )
    )
    stats = r.get_stats(mode=KEY_MODE_RAW_DATA)

    mock_subprocess.run.assert_called_with(expected_args, capture_output=True)
    assert stats == None
    assert "Command failed" in caplog.text

    # Test: Non-JSON returned.
    mock_subprocess.run = mock.Mock(
        return_value=_get_completed_process(
            args=expected_args, rc=0, stdout="this will not decode"
        )
    )
    stats = r.get_stats(mode=KEY_MODE_RAW_DATA)

    mock_subprocess.run.assert_called_with(expected_args, capture_output=True)
    assert stats == None
    assert "yielded non-JSON output" in caplog.text


@mock.patch("restic_exporter.restic_exporter.subprocess")
def test_restic_executor_get_snapshots(mock_subprocess, caplog):
    path_binary = "/path/to/binary"
    expected_args = [
        path_binary,
        "--json",
        "snapshots",
        "--group-by=host,path,tags",
        "--last",
    ]
    output = TEST_GROUPED_SNAPSHOT_DATA

    r = ResticExecutor(path_binary)

    # Test: Success.
    mock_subprocess.run = mock.Mock(
        return_value=_get_completed_process(
            args=expected_args,
            rc=0,
            stdout=json.dumps(output),
        )
    )
    stats = r.get_snapshots(group_by="host,path,tags", last=True)

    mock_subprocess.run.assert_called_with(expected_args, capture_output=True)
    assert stats == [json_to_snapshot(TEST_SNAPSHOT_DATA)]

    # Test: No snapshots.
    mock_subprocess.run = mock.Mock(
        args=expected_args,
        rc=1,
    )
    stats = r.get_snapshots(group_by="host,path,tags", last=True)

    mock_subprocess.run.assert_called_with(expected_args, capture_output=True)
    assert stats == []
    assert "No valid snapshots found" in caplog.text


def test_restic_stats_generator_get_snapshot_stats():
    mock_executor = mock.Mock()
    generator = ResticStatsGenerator(
        mock_executor, group_by="group_by", last=True, backup_status_window_seconds=10
    )

    mock_executor.get_snapshots = mock.Mock(
        return_value=[json_to_snapshot(TEST_SNAPSHOT_DATA)]
    )
    mock_executor.get_stats = mock.Mock(
        side_effect=[
            json_to_stats(TEST_STATS_DATA_RAW),
            json_to_stats(TEST_STATS_DATA_RESTORE),
        ]
    )

    stats = generator.get_snapshot_stats()
    mock_executor.get_snapshots.assert_called_with(group_by="group_by", last=True)
    mock_executor.get_stats.assert_has_calls(
        [
            mock.call(snapshot_ids=["ab1234"], mode=KEY_MODE_RAW_DATA),
            mock.call(snapshot_ids=["ab1234"], mode=KEY_MODE_RESTORE_SIZE),
        ]
    )

    expected_stats = json_to_snapshot(TEST_SNAPSHOT_DATA)
    expected_stats.stats = ResticStatsBundle(
        raw=json_to_stats(TEST_STATS_DATA_RAW),
        restore=json_to_stats(TEST_STATS_DATA_RESTORE),
    )


@mock.patch("restic_exporter.restic_exporter.get_current_datetime")
def test_restic_stats_generator_get_piped_stats_backup_status(mock_current_datetime):
    generator = ResticStatsGenerator(
        None, group_by="group_by", last=True, backup_status_window_seconds=10
    )

    key = ResticSnapshotKeys(hostname="hostname", paths=["path1"])

    mock_current_datetime.return_value = datetime.datetime(2020, 12, 30, 8, 27, 23)

    # Test: Normal.
    stats = generator.get_piped_stats(line=json.dumps(TEST_BACKUP_STATUS_DATA), key=key)
    assert stats == [json_to_backup_status(TEST_BACKUP_STATUS_DATA, key)]

    # Test: Invalid data.
    stats = generator.get_piped_stats(line="this is garbage", key=key)
    assert stats == []

    # Test: Missing message type.
    stats = generator.get_piped_stats(
        line=json.dumps(dict_without(TEST_BACKUP_STATUS_DATA, "message_type")), key=key
    )
    assert stats == []

    # Test: Unsupported message type.
    stats = generator.get_piped_stats(
        line=json.dumps({**TEST_BACKUP_STATUS_DATA, "message_type": "unsupported"}),
        key=key,
    )
    assert stats == []

    # Test: Another stat in the same window should be ignored.
    stats = generator.get_piped_stats(line=json.dumps(TEST_BACKUP_STATUS_DATA), key=key)
    assert stats == []

    mock_current_datetime.return_value = datetime.datetime(2020, 12, 30, 8, 28, 23)

    # Test: .. but another later should be fine.
    stats = generator.get_piped_stats(line=json.dumps(TEST_BACKUP_STATUS_DATA), key=key)
    assert stats == [json_to_backup_status(TEST_BACKUP_STATUS_DATA, key)]


def test_restic_stats_generator_get_piped_stats_backup_summary():
    generator = ResticStatsGenerator(
        None, group_by="group_by", last=True, backup_status_window_seconds=10
    )

    key = ResticSnapshotKeys(hostname="hostname", paths=["path1"])
    backup_summary = json_to_backup_summary(TEST_BACKUP_SUMMARY_DATA, key)
    last_backup_status = ResticBackupStatus(
        key=backup_summary.key,
        files_total=backup_summary.files_processed,
        bytes_total=backup_summary.bytes_processed,
        percent_done=1.0,
        files_done=backup_summary.files_processed,
        bytes_done=backup_summary.bytes_processed,
        seconds_elapsed=backup_summary.duration,
    )

    # Test: Normal.
    stats = generator.get_piped_stats(
        line=json.dumps(TEST_BACKUP_SUMMARY_DATA), key=key
    )
    assert stats == [last_backup_status, backup_summary]

    # Test: Broken summary.
    stats = generator.get_piped_stats(
        line=json.dumps(dict_without(TEST_BACKUP_SUMMARY_DATA, "files_new")), key=key
    )
    assert stats == []


def test_get_snapshot_key_from_args(caplog):
    ap = argparse.ArgumentParser()
    ap.add_argument("--backup-host")
    ap.add_argument("--backup-path", nargs="+", action="extend")
    ap.add_argument("--backup-tag", nargs="+", action="extend")

    # Test: Normal.
    args = ap.parse_args(
        ["--backup-host", "host", "--backup-path=/path", "--backup-tag", "tag1"]
    )
    assert get_snapshot_key_from_args(ap, args) == ResticSnapshotKeys(
        hostname="host",
        paths=["/path"],
        tags=["tag1"],
    )

    # Test: Missing --backup-host
    args = ap.parse_args(["--backup-path=/path", "--backup-tag", "tag1"])
    with pytest.raises(SystemExit):
        get_snapshot_key_from_args(ap, args)
        assert "Backup host must be provided" in caplog.text

    # Test: Multiple tags
    args = ap.parse_args(
        [
            "--backup-host",
            "host",
            "--backup-path=/path",
            "--backup-tag",
            "tag1,tag2 tag3",
            "--backup-tag",
            "tag4",
        ]
    )
    assert get_snapshot_key_from_args(ap, args) == ResticSnapshotKeys(
        hostname="host",
        paths=["/path"],
        tags=["tag1", "tag2", "tag3", "tag4"],
    )


def test_main_tty(caplog):
    test_args = [sys.argv[0], "mock_exporter"]

    mock_exporter = mock.Mock()
    mock_exporter.construct_from_args = mock.Mock(return_value=mock_exporter)

    mock_stdin = mock.Mock()
    mock_stdin.isatty = mock.Mock(return_value=True)

    mock_generator = mock.Mock()
    mock_generator.get_snapshot_stats = mock.Mock(return_value=["stats_here"])

    with mock.patch.dict(
        EXPORTERS, {"mock_exporter": mock_exporter}, clear=True
    ), mock.patch.object(sys, "argv", test_args), mock.patch(
        "restic_exporter.restic_exporter.ResticStatsGenerator",
        return_value=mock_generator,
    ), mock.patch.object(
        sys, "stdin", mock_stdin
    ):
        main()

    assert mock_exporter.add_args_to_parser.called
    assert mock_exporter.start.called
    mock_exporter.export.assert_called_with("stats_here")


def test_main_not_tty(caplog):
    test_args = [sys.argv[0], "mock_exporter", "--backup-host=host"]

    mock_exporter = mock.Mock()
    mock_exporter.construct_from_args = mock.Mock(return_value=mock_exporter)

    stdin_lines = ["line1", "line2", ""]
    mock_stdin = mock.Mock()
    mock_stdin.isatty = mock.Mock(return_value=False)
    mock_stdin.readline = mock.Mock(side_effect=stdin_lines)

    test_stats = [["stat1"], ["stat2", "stat3"]]
    mock_generator = mock.Mock()
    mock_generator.get_piped_stats = mock.Mock(side_effect=test_stats)

    with mock.patch.dict(
        EXPORTERS, {"mock_exporter": mock_exporter}, clear=True
    ), mock.patch.object(sys, "argv", test_args), mock.patch(
        "restic_exporter.restic_exporter.ResticStatsGenerator",
        return_value=mock_generator,
    ), mock.patch.object(
        sys, "stdin", mock_stdin
    ):
        main()

    assert mock_exporter.add_args_to_parser.called
    assert mock_exporter.start.called
    mock_exporter.export.assert_has_calls(
        [mock.call("stat1"), mock.call("stat2"), mock.call("stat3")]
    )
