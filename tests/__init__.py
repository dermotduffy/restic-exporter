TEST_STATS_DATA = {"total_size": 1709, "total_file_count": 1, "total_blob_count": 4}

TEST_SNAPSHOT_DATA = {
    "time": "2020-12-28T21:28:23.403981118-08:00",
    "parent": "parent_here",
    "tree": "tree_here",
    "paths": ["/path/whatever"],
    "hostname": "hostname",
    "username": "username",
    "uid": 1000,
    "gid": 1000,
    "id": "1234",
    "short_id": "12",
}

TEST_GROUPED_SNAPSHOT_DATA = [
    {
        "group_key": {"hostname": "hostname", "paths": ["path1"], "tags": ["tag1"]},
        "snapshots": [TEST_SNAPSHOT_DATA],
    }
]


TEST_BACKUP_STATUS_DATA = {
    "message_type": "status",
    "percent_done": 0.25,
    "total_files": 9586,
    "files_done": 2321,
    "total_bytes": 147893659,
    "bytes_done": 36953149,
}

TEST_BACKUP_SUMMARY_DATA = {
    "message_type": "summary",
    "files_new": 1265,
    "files_changed": 41,
    "files_unmodified": 77637,
    "dirs_new": 217,
    "dirs_changed": 44,
    "dirs_unmodified": 17511,
    "data_blobs": 849,
    "tree_blobs": 262,
    "data_added": 14909514,
    "total_files_processed": 78943,
    "total_bytes_processed": 1667325955,
    "total_duration": 4.035790225,
    "snapshot_id": "a34dda71",
}
