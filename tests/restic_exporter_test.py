#!/usr/bin/python
"""Test for the restic-exporter."""

import json
import os
import pytest
import logging
import subprocess
import string
from typing import cast, Any, Dict, List, Optional, Tuple
from unittest import mock

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

from restic_exporter.restic_exporter import ResticExecutor
from restic_exporter.types import json_to_snapshot, json_to_stats

from . import (
    TEST_STATS_DATA,
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


def _get_completed_process(args=[], rc=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args, returncode=rc, stdout=stdout, stderr=stderr
    )


@mock.patch("restic_exporter.restic_exporter.subprocess")
def test_restic_executor_get_stats(mock_subprocess, caplog):
    path_binary = "/path/to/binary"
    expected_args = [path_binary, "--json", "stats", "--mode=raw-data"]
    output = TEST_STATS_DATA

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
