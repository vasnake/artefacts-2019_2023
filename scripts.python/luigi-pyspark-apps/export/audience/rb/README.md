# Description

Prepare and export audience partition (selected uid_type) to HDFS directory, TSV format.

Config example:
```json
{
  "audience_id": 737,
  "uid_type": "OKID",
  "life_time": 15,
  "exclude_audience_ids": [12, 389],
  "target_dt": "2023-03-09",
  "source_db": "ds_auditories",
  "source_table": "clal_audience",
  "source_partition_conf": {
    "audience_name": ["A1", "B1"],
    "category": "positive"
  },  
  "min_score": 0.0,
  "max_score": 1.0,
  "extra_filters": [
    {
      "db": "ds_auditories",
      "table": "active_audience",
      "partition_conf": {
        "audience_name": "ANTIFRAUD_SHOW",
        "category": "3_of_28"
      },
      "max_dt_diff": 3,
      "filter_type": "intersection"
    }
  ],
  "min_target_rows": 10000000,
  "max_target_rows": 100000000,
  "shuffle_partitions": 100
}
```