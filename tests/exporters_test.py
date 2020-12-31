"""Test for the restic-exporter exporters"""
import argparse
import datetime
import dateutil
import os
import pytest
from unittest import mock
import logging
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

from restic_exporter.const import ENV_INFLUX_PASSWORD, EXPORTER_INFLUXDB
from restic_exporter.exporters import Exporter, EXPORTERS

def test_exporter_add_args_to_parser(caplog):
    exporter_class = Exporter

    ap = argparse.ArgumentParser()
    Exporter.add_args_to_parser(ap)
    args = ap.parse_args("")
    assert not vars(args)

def test_exporter_construct_from_args(caplog):
    ap = argparse.ArgumentParser()
    assert Exporter.construct_from_args(ap) is None

def test_exporter_start(caplog):
    exporter = Exporter()
    assert exporter.start() == None

def test_exporter_export(caplog):
    exporter = Exporter()
    assert exporter.export("will_be_ignored") == None

def test_exporter_get_password(tmp_path):
    password_env = "TEST_ENV_VAR"
    os.environ[password_env] = "test_password"
    
    assert Exporter.get_password(password_env) == "test_password"

    password_file_path = os.path.join(tmp_path, "restic_exporter_password_file")
    with open(password_file_path, "w") as fh:
        fh.write("different_test_password")
    assert Exporter.get_password(password_env, password_file_path) == "different_test_password"

def test_exporter_influxdb_add_args_to_parser(caplog):
    exporter_class = EXPORTERS[EXPORTER_INFLUXDB]
    ap = argparse.ArgumentParser()
    exporter_class.add_args_to_parser(ap)
    args = ap.parse_args("")
    assert args.influxdb_database == "restic"
    assert args.influxdb_host == "localhost"
    assert args.influxdb_username == None
    assert args.influxdb_password_file == None
    assert args.influxdb_port == 8086

def test_exporter_influxdb_construct_from_args(caplog):
    exporter_class = EXPORTERS[EXPORTER_INFLUXDB]
    ap = argparse.ArgumentParser()
    exporter_class.add_args_to_parser(ap)
    args = ap.parse_args("")
    exporter = exporter_class.construct_from_args(args)
    assert exporter._database == "restic"
    assert exporter._host == "localhost"
    assert exporter._username == None
    assert exporter._password == None
    assert exporter._port == 8086
    

@mock.patch('restic_exporter.exporters.influxdb.InfluxDBClient')
def test_exporter_influxdb_start(mock_influxdb):
    exporter_class = EXPORTERS[EXPORTER_INFLUXDB]
    exporter = exporter_class(
        host="test_host",
        port=1234,
        username="test_username",
        password="test_password",
        database="test_database"
    )
    assert exporter is not None

    mock_influxdb_client = mock.Mock()
    mock_influxdb.return_value = mock_influxdb_client

    exporter.start()

    mock_influxdb.assert_called_with('test_host', 1234, 'test_username', 'test_password', 'test_database')
    assert mock_influxdb_client.create_database.called
   