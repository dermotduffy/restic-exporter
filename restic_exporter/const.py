"""Consts for the Restic Exporter."""

DEFAULT_INFLUX_DATABASE = "restic"

ENV_INFLUX_PASSWORD = "INFLUXDB_PASSWORD"

EXPORTER_INFLUXDB = "influxdb"

KEY_COMMAND_SNAPSHOTS = "snapshots"
KEY_COMMAND_STATS = "stats"

KEY_GROUP_BY_HOST = "hostname"
KEY_GROUP_BY_PATHS = "paths"
KEY_GROUP_BY_TAGS = "tags"
KEY_GROUP_KEY = "group_key"

KEY_MESSAGE_TYPE = "message_type"
KEY_MESSAGE_TYPE_STATUS = "status"
KEY_MESSAGE_TYPE_SUMMARY = "summary"

KEY_MODE_RAW_DATA = "raw-data"
KEY_MODE_RESTORE_SIZE = "restore-size"

KEY_RAW_BLOB_COUNT = "raw_blob_count"
KEY_RAW_FILE_COUNT = "raw_file_count"
KEY_RAW_SIZE = "raw_size"
KEY_RESTORE_BLOB_COUNT = "restore_blob_count"
KEY_RESTORE_FILE_COUNT = "restore_file_count"
KEY_RESTORE_SIZE = "restore_size"

KEY_SNAPSHOT_HOSTNAME = "hostname"
KEY_SNAPSHOT_ID = "id"
KEY_SNAPSHOT_PATHS = "paths"
KEY_SNAPSHOT_SHORT_ID = "short_id"
KEY_SNAPSHOTS = "snapshots"
KEY_SNAPSHOT_TAGS = "tags"
KEY_SNAPSHOT_TIME = "time"

KEY_STATS_TOTAL_BLOB_COUNT = "total_blob_count"
KEY_STATS_TOTAL_FILE_COUNT = "total_file_count"
KEY_STATS_TOTAL_SIZE = "total_size"

KEY_STATUS_BYTES_DONE = "bytes_done"
KEY_STATUS_BYTES_TOTAL = "total_bytes"
KEY_STATUS_FILES_DONE = "files_done"
KEY_STATUS_FILES_TOTAL = "total_files"
KEY_STATUS_PERCENT_DONE = "percent_done"
KEY_STATUS_SECONDS_ELAPSED = "seconds_elapsed"
KEY_STATUS_SECONDS_REMAINING = "seconds_remaining"

KEY_SUMMARY_DATA_ADDED = "data_added"
KEY_SUMMARY_DATA_BLOBS = "data_blobs"
KEY_SUMMARY_DIRS_CHANGED = "dirs_changed"
KEY_SUMMARY_DIRS_NEW = "dirs_new"
KEY_SUMMARY_DIRS_UNMODIFIED = "dirs_unmodified"
KEY_SUMMARY_FILES_CHANGED = "files_changed"
KEY_SUMMARY_FILES_NEW = "files_new"
KEY_SUMMARY_FILES_UNMODIFIED = "files_unmodified"
KEY_SUMMARY_SNAPSHOT_ID = "snapshot_id"
KEY_SUMMARY_TREE_BLOBS = "tree_blobs"
KEY_SUMMARY_TOTAL_BYTES_PROCESSED = "total_bytes_processed"
KEY_SUMMARY_TOTAL_DURATION = "total_duration"
KEY_SUMMARY_TOTAL_FILES_PROCESSED = "total_files_processed"

MEASUREMENT_BACKUP_STATUS = "restic_backup_status"
MEASUREMENT_BACKUP_SUMMARY = "restic_backup_summary"
MEASUREMENT_REPO = "restic_repo"
MEASUREMENT_SNAPSHOTS = "restic_snapshots"
