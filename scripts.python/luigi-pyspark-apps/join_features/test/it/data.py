import os

import numpy as np

from prj.apps.utils.testing.defines import TEST_DB, TEST_HDFS_DIR

from ..e2e.data_success import (
    NAME,
    MD_DATA,
    MD_TABLE,
    MD_SCHEMA,
    SOURCE_DATA,
    SOURCE_TABLE,
    TMP_HDFS_DIR,
    MATCHING_DATA,
    SOURCE_SCHEMA,
    MATCHING_TABLE,
    MATCHING_SCHEMA,
    MD_PARTITION_COLUMNS,
    SOURCE_PARTITION_COLUMNS,
    MATCHING_PARTITION_COLUMNS,
    generate_test_data,
)

# flake8: noqa
# fmt: off
# @formatter:off
