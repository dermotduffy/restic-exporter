[![PyPi](https://img.shields.io/pypi/v/restic-exporter.svg)](https://pypi.org/project/restic-exporter/)
[![PyPi](https://img.shields.io/pypi/pyversions/restic-exporter.svg)](https://pypi.org/project/restic-exporter/)
[![Build Status](https://travis-ci.com/dermotduffy/restic-exporter.svg?branch=main)](https://travis-ci.com/dermotduffy/restic-exporter)
[![Coverage](https://img.shields.io/codecov/c/github/dermotduffy/restic-exporter)](https://codecov.io/gh/dermotduffy/restic-exporter)

# Restic Exporter

A small Python binary to export stats from a [restic](https://github.com/restic/restic)
backup repository, for example for use in monitoring systems.

## Installation

```bash
$ pip3 install restic-exporter
```
## Features

   * Decoupled from [restic](https://github.com/restic/restic) itself, can be run on any
     host as long as it can talk to the repository.
   * Can provide both repo-based statistics, and backup progress statistics from restic clients.

## Statistics Exported

Almost all data made available from the restic tool itself can be exported.

   * Backup progress (`restic_backup_progress`)
      * hostname
      * paths
      * tags
      * bytes_done
      * total_bytes
      * files_done
      * total_files
      * percent_done
      * seconds_tag
   * Backup summary  (`restic_backup_summary`)
      * hostname
      * paths
      * tags
      * snapshot_id
      * files_new
      * files_unmodified
      * files_changed
      * dirs_new
      * dirs_unmodified
      * dirs_changed
      * data_added
      * total_bytes_processed
      * total_files_processed
      * total_duration
   * Snapshots  (`restic_snapshots`)
      * hostname
      * paths
      * tags
      * raw_blob_count
      * raw_file_count
      * raw_size
      * restore_file_count
      * restore_size

## Supported Exporters

This tool can export statistics to:

   * [InfluxDB](https://github.com/influxdata/influxdb)

# Usage

To be completed.
