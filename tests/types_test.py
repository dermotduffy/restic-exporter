"""Test for the restic-exporter types"""
import datetime
import dateutil
import pytest
import logging

# TODO: typing for tests

from restic_exporter.types import (
    ResticBackupStatus,
    ResticBackupSummary,
    ResticSnapshot,
    ResticSnapshotKeys,
    ResticStats,
    json_to_stats,
    json_to_snapshot,
    json_to_backup_status,
    json_to_backup_summary,
)

from . import (
    dict_without,
    TEST_SNAPSHOT_DATA,
    TEST_BACKUP_STATUS_DATA,
    TEST_BACKUP_SUMMARY_DATA,
)

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def test_json_to_stats(caplog):
    # Test: Normal.
    assert json_to_stats(
        {"total_size": 1709, "total_file_count": 1, "total_blob_count": 4}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=4)

    # Test: Converting from floats.
    assert json_to_stats(
        {"total_size": 1709.0, "total_file_count": 1.0, "total_blob_count": 4.0}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=4)

    # Test: None is supported.
    assert json_to_stats(
        {"total_size": 1709.0, "total_file_count": 1.0, "total_blob_count": None}
    ) == ResticStats(total_size=1709, total_file_count=1, total_blob_count=None)

    # Test: Negatives fail validation.
    assert (
        json_to_stats(
            {"total_size": -1, "total_file_count": 1.0, "total_blob_count": None}
        )
        is None
    )
    assert "Expected positive number" in caplog.text

    # Test: Missing keys result in a warning.
    assert json_to_stats({"foo": "bar"}) is None
    assert "Skipping restic stats with missing key" in caplog.text

    # Test: Invalid values result in a warning.
    assert (
        json_to_stats(
            {"total_size": "str", "total_file_count": 1, "total_blob_count": 4}
        )
        is None
    )
    assert "Skipping restic stats with invalid value" in caplog.text

    # Test: None -> None.
    assert json_to_stats(None) is None


def test_json_to_snapshot(caplog):
    # Test: Normal.
    assert json_to_snapshot(TEST_SNAPSHOT_DATA) == ResticSnapshot(
        ResticSnapshotKeys(
            hostname="hostname",
            paths=["/path/whatever"],
            tags=None,
            snapshot_id="ab1234",
        ),
        snapshot_time=datetime.datetime(
            2020, 12, 28, 21, 28, 23, 403981, tzinfo=dateutil.tz.tzoffset(None, -28800)
        ),
        stats=None,
    )

    # Test: Missing keys result in a warning.
    assert json_to_snapshot(dict_without(TEST_SNAPSHOT_DATA, "time")) is None
    assert "Skipping snapshot with missing key" in caplog.text

    # Test: Invalid values result in a warning.
    assert json_to_snapshot({**TEST_SNAPSHOT_DATA, "time": "garbage"}) is None
    assert "Skipping unparseable snapshot time" in caplog.text

    # Test: None -> None.
    assert json_to_snapshot(None) is None


def test_json_to_backup_status(caplog):
    key = ResticSnapshotKeys(hostname="hostname", paths=["path1"])

    # Test: Normal.
    assert json_to_backup_status(TEST_BACKUP_STATUS_DATA, key) == ResticBackupStatus(
        key=key,
        files_total=9586,
        bytes_total=147893659,
        percent_done=0.25,
        files_done=2321,
        bytes_done=36953149,
        seconds_elapsed=None,
        seconds_remaining=None,
    )

    # Test: Percent outside the range of 0-1.
    assert (
        json_to_backup_status({**TEST_BACKUP_STATUS_DATA, "percent_done": 2.0}, key)
        is None
    )

    # Test: Missing keys result in a warning.
    assert (
        json_to_backup_status(dict_without(TEST_BACKUP_STATUS_DATA, "total_files"), key)
        is None
    )
    assert "Skipping backup status with missing key" in caplog.text

    # Test: Invalid values result in a warning.
    assert (
        json_to_backup_status(
            {**TEST_BACKUP_STATUS_DATA, "total_files": "garbage"}, key
        )
        is None
    )
    assert "Skipping backup status with invalid value" in caplog.text

    # Test: None -> None.
    assert json_to_backup_status(None, key) is None


def test_json_to_backup_summary(caplog):
    key = ResticSnapshotKeys(hostname="hostname", paths=["path1"])

    # Test: Normal.
    assert json_to_backup_summary(TEST_BACKUP_SUMMARY_DATA, key) == ResticBackupSummary(
        key=ResticSnapshotKeys(
            hostname="hostname",
            paths=["path1"],
            snapshot_id="a34dda71",
        ),
        files_new=1265,
        files_changed=41,
        files_unmodified=77637,
        dirs_new=217,
        dirs_changed=44,
        dirs_unmodified=17511,
        data_added=14909514,
        files_processed=78943,
        bytes_processed=1667325955,
        duration=4.035790225,
    )

    # Test: Missing keys result in a warning.
    assert (
        json_to_backup_summary(dict_without(TEST_BACKUP_SUMMARY_DATA, "files_new"), key)
        is None
    )
    assert "Skipping backup summary with missing key" in caplog.text

    # Test: Invalid values result in a warning.
    assert (
        json_to_backup_summary(
            {**TEST_BACKUP_SUMMARY_DATA, "files_new": "garbage"}, key
        )
        is None
    )
    assert "Skipping backup summary with invalid value" in caplog.text

    # Test: None -> None.
    assert json_to_backup_summary(None, key) is None


def test_restic_snapshot_keys_validation():

    # Test: Normal.
    assert ResticSnapshotKeys(hostname="hostname", paths=["path1"]) is not None

    # Test: Empty hostname should not be allowed.
    with pytest.raises(ValueError):
        assert ResticSnapshotKeys(hostname="", paths=["path1"]) is None
