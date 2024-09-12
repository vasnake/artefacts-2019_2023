# Description

Build (combine) ad. features and export to CSV files in Hadoop directory.

Config example:
```json
{
  "target_dt": "2021-09-27",
  "features_subdir": "banner_pad_cats_ctr_wscore_1d",
  "source_db": "ds_scoring",
  "source_table": "dm_ad_feature",
  "source_partition_conf": {
    "feature_name": "banner_pad_cats_ctr_wscore_1d",
    "uid_type": "BANNER"
  },
  "export_columns": {
    "uid": {
      "name": "banner",
      "expr": "uid"
    },
    "features": [
      {
        "name": "num_banner_feature_0",
        "expr": "gavg(score_map)"
      }
    ]
  },
  "min_target_rows": 1000,
  "max_target_rows": 1000000,
  "max_collection_size": 100,
  "shuffle_partitions": 8
}
```
