#!/usr/bin/python
"""Test for the restic-exporter types"""

import json
import pytest
import logging
from typing import cast, Any, Dict, List, Optional, Tuple

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

from restic_exporter.types import (
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


def test_json_to_stats(caplog):
    # Normal.
    assert json_to_stats(
        {"total_size": 1709, "total_file_count": 1, "total_blob_count": 4}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=4)

    # Convert from floats.
    assert json_to_stats(
        {"total_size": 1709.0, "total_file_count": 1.0, "total_blob_count": 4.0}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=4)

    # Support None.
    assert json_to_stats(
        {"total_size": 1709.0, "total_file_count": 1.0, "total_blob_count": None}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=None)

    assert json_to_stats({"foo": "bar"}) is None
    assert "Skipping restic stats with missing key" in caplog.text

    assert json_to_stats({"total_size": "str", "total_file_count": 1, "total_blob_count": 4}) is None
    assert "Skipping restic stats with invalid value" in caplog.text

    assert json_to_stats(None) is None
