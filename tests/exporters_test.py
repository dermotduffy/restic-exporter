"""Test for the restic-exporter exporters"""
import datetime
import dateutil
import pytest
import logging
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig()
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

from restic_exporter.exporters import ExporterInfluxDB

def test_exporter_influxdb(caplog):
    pass
   