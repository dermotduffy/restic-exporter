"""Test for the restic-exporter exporters"""
import argparse
import datetime
import dateutil
import os
from unittest import mock
import logging

from restic_exporter.const import EXPORTER_INFLUXDB
from restic_exporter.exporters import Exporter, EXPORTERS

from restic_exporter.types import (
    ResticBackupStatus,
    ResticBackupSummary,
    ResticSnapshot,
    ResticSnapshotKeys,
    ResticStats,
    ResticStatsBundle,
)

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

TEST_INFLUXDB_EXPORTER_ARGS = {
    "host": "test_host",
    "port": 1234,
    "username": "test_username",
    "password": "test_password",
    "database": "test_database",
}


def test_exporter_add_args_to_parser():
    ap = argparse.ArgumentParser()
    Exporter.add_args_to_parser(ap)
    args = ap.parse_args("")
    assert not vars(args)


def test_exporter_construct_from_args():
    ap = argparse.ArgumentParser()
    assert Exporter.construct_from_args(ap) is None


def test_exporter_start():
    exporter = Exporter()
    assert exporter.start() is None


def test_exporter_export():
    exporter = Exporter()
    assert exporter.export("will_be_ignored") is None


def test_exporter_get_password(tmp_path):
    password_env = "TEST_ENV_VAR"
    os.environ[password_env] = "test_password"

    assert Exporter.get_password(password_env) == "test_password"

    password_file_path = os.path.join(tmp_path, "restic_exporter_password_file")
    with open(password_file_path, "w") as fh:
        fh.write("different_test_password")
    assert (
        Exporter.get_password(password_env, password_file_path)
        == "different_test_password"
    )


def test_exporter_influxdb_add_args_to_parser():
    exporter_class = EXPORTERS[EXPORTER_INFLUXDB]
    ap = argparse.ArgumentParser()
    exporter_class.add_args_to_parser(ap)
    args = ap.parse_args("")
    assert args.influxdb_database == "restic"
    assert args.influxdb_host == "localhost"
    assert args.influxdb_username is None
    assert args.influxdb_password_file is None
    assert args.influxdb_port == 8086


def test_exporter_influxdb_construct_from_args():
    exporter_class = EXPORTERS[EXPORTER_INFLUXDB]
    ap = argparse.ArgumentParser()
    exporter_class.add_args_to_parser(ap)
    args = ap.parse_args("")
    exporter = exporter_class.construct_from_args(args)
    assert exporter._database == "restic"
    assert exporter._host == "localhost"
    assert exporter._username is None
    assert exporter._password is None
    assert exporter._port == 8086


def setup_test_influxdb_exporter(mock_influxdb):
    exporter = EXPORTERS[EXPORTER_INFLUXDB](**TEST_INFLUXDB_EXPORTER_ARGS)
    assert exporter is not None

    mock_influxdb_client = mock.Mock()
    mock_influxdb.return_value = mock_influxdb_client

    exporter.start()

    mock_influxdb.assert_called_with(
        "test_host", 1234, "test_username", "test_password", "test_database"
    )
    assert mock_influxdb_client.create_database.called

    return (exporter, mock_influxdb_client)


@mock.patch("restic_exporter.exporters.influxdb.InfluxDBClient")
def test_exporter_influxdb_start(mock_influxdb):
    (_, _) = setup_test_influxdb_exporter(mock_influxdb)


@mock.patch("restic_exporter.exporters.influxdb.InfluxDBClient")
@mock.patch("restic_exporter.exporters.get_current_datetime")
def test_exporter_influxdb_export_restic_backup_status(
    mock_current_datetime, mock_influxdb
):
    (exporter, mock_influxdb_client) = setup_test_influxdb_exporter(mock_influxdb)

    key = ResticSnapshotKeys(hostname="hostname", paths=["path1"])
    backup_status = ResticBackupStatus(
        key=key,
        files_total=9586,
        bytes_total=147893659,
        percent_done=0.25,
        files_done=2321,
        bytes_done=36953149,
        seconds_elapsed=None,
        seconds_remaining=None,
    )

    current_datetime = datetime.datetime(2020, 12, 30, 8, 27, 23)
    mock_current_datetime.return_value = current_datetime

    exporter.export(backup_status)

    mock_influxdb_client.write_points.assert_called_with(
        [
            {
                "measurement": "restic_backup_status",
                "tags": {"hostname": "hostname", "paths": "path1"},
                "time": current_datetime,
                "fields": {
                    "total_files": 9586,
                    "total_bytes": 147893659,
                    "percent_done": 0.25,
                    "files_done": 2321,
                    "bytes_done": 36953149,
                },
            }
        ]
    )


@mock.patch("restic_exporter.exporters.influxdb.InfluxDBClient")
@mock.patch("restic_exporter.exporters.get_current_datetime")
def test_exporter_influxdb_export_restic_backup_summary(
    mock_current_datetime, mock_influxdb
):
    (exporter, mock_influxdb_client) = setup_test_influxdb_exporter(mock_influxdb)

    backup_summary = ResticBackupSummary(
        key=ResticSnapshotKeys(
            hostname="hostname",
            paths=["path1", "path2"],
            snapshot_id="a34dda71",
            tags=["tag1", "tag2"],
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

    current_datetime = datetime.datetime(2020, 12, 30, 8, 27, 23)
    mock_current_datetime.return_value = current_datetime

    exporter.export(backup_summary)

    mock_influxdb_client.write_points.assert_called_with(
        [
            {
                "measurement": "restic_backup_summary",
                "tags": {"hostname": "hostname", "paths": "path1,path2", "tags": "tag1,tag2"},
                "time": current_datetime,
                "fields": {
                    "files_new": 1265,
                    "files_changed": 41,
                    "files_unmodified": 77637,
                    "dirs_new": 217,
                    "dirs_changed": 44,
                    "dirs_unmodified": 17511,
                    "data_added": 14909514,
                    "total_files_processed": 78943,
                    "total_bytes_processed": 1667325955,
                    "total_duration": 4.035790225,
                    "snapshot_id": "a34dda71",
                },
            }
        ]
    )


@mock.patch("restic_exporter.exporters.influxdb.InfluxDBClient")
def test_exporter_influxdb_export_restic_snapshot(mock_influxdb):
    (exporter, mock_influxdb_client) = setup_test_influxdb_exporter(mock_influxdb)

    snapshot = ResticSnapshot(
        ResticSnapshotKeys(
            hostname="hostname",
            paths=["/path/whatever"],
            tags=None,
            snapshot_id="1234",
        ),
        snapshot_time=datetime.datetime(
            2020, 12, 28, 21, 28, 23, 403981, tzinfo=dateutil.tz.tzoffset(None, -28800)
        ),
        stats=ResticStatsBundle(
            raw=ResticStats(total_size=1709, total_file_count=1, total_blob_count=None),
            restore=ResticStats(
                total_size=1710, total_file_count=2, total_blob_count=None
            ),
        ),
    )

    exporter.export(snapshot)

    mock_influxdb_client.write_points.assert_called_with(
        [
            {
                "measurement": "restic_snapshots",
                "tags": {"hostname": "hostname", "paths": "/path/whatever"},
                "time": snapshot.snapshot_time,
                "fields": {
                    "id": "1234",
                    "raw_size": 1709,
                    "raw_file_count": 1,
                    "restore_size": 1710,
                    "restore_file_count": 2,
                },
            }
        ]
    )


@mock.patch("restic_exporter.exporters.influxdb.InfluxDBClient")
def test_exporter_influxdb_export_unknown(mock_influxdb, caplog):
    (exporter, mock_influxdb_client) = setup_test_influxdb_exporter(mock_influxdb)

    exporter.export("this_is_not_an_expected_type")

    assert "cannot handle stats of type" in caplog.text
