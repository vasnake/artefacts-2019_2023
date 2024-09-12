# Description

Join almost arbitrary datasets to standartized ML features data-mart.
Extra app features: UID matching (mapping) with columns aggregation.

Input tables partitions, output table partitions -- Hive (uid, uid_type) partitioned tables.
* Input: config, facts, dimensions, matching table.
* Output: features data-mart partitions.

UID: Universal ID, not User ID.
Global unique universal ID: touple (uid, uid_type).

All tables should have partitioning by dt column (YYYY-MM-DD).
