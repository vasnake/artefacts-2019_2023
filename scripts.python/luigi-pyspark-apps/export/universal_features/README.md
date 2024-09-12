# Description

Combine and export universal user features to HDFS directory, CSV format.

### Config examples

#### Feature `array<float>`
```json
{
  "target_dt": "2021-06-10",
  "features_subdir": "some_score",
  "source_db": "ds_auditories",
  "source_table": "clal_retro_audience",
  "source_partition_conf": {
    "uid_type": "VKID",
    "category": ["foo", "bar"],
    "audience_name": "work"
  },
  "export_columns": {
    "uid": "uid",
    "uid_type": "uid_type",
    "features": [
      {
        "name":  "some_score",
        "expr":  "map_values_ordered(user_dmdesc.collect(category, cast(score as float)), array('foo', 'bar'))"
      }
    ]
  },
  "min_target_rows": 1000000,
  "output_max_rows_per_file": 1000000,
  "max_collection_size": 100,
  "shuffle_partitions": 400
}
```

#### Feature `float` with added link `bu_link_id`
```json
{
  "vars": {
    "date": "{{ datetime.date.today() - datetime.timedelta(days=1) }}",
    "audience_names": [
      "rng_installs__all_trg_72510",
      "rng_installs__auto_trg_72510",
      "rng_installs__finance_trg_72510"
    ]
  },
  "stages": [
    "export"
  ],
  "jobs": {
    "ExportUniversalFeatures": {
      "stage": "export",
      "xapp": "ExportUniversalFeatures",
      "trigger": "all_success",
      "tags": [
        "default"
      ],
      "config": {
        "target_dt": "{{ vars.date }}",
        "features_subdir": "rng_installs_predict_categories",
        "source_db": "ds_auditories",
        "source_table": "clal_adhoc_audience",
        "source_partition_conf": {
          "audience_name": "{{ vars.audience_names }}",
          "category": "positive",
          "uid_type": "GAID"
        },
        "max_dt_diff": 0,
        "export_columns": {
          "uid": "uid",
          "uid_type": "uid_type",
          "bu_link_id": {
            "expr": "CASE audience_name WHEN 'rng_installs__all_trg_72510' THEN 0 WHEN 'rng_installs__auto_trg_72510' THEN 1 WHEN 'rng_installs__finance_trg_72510' THEN 2 ELSE null END",
            "link_type": "AggBannerTopic"
          },
          "features": [
            {
              "name":  "rng_installs_predict_categories",
              "expr":  "avg(cast((CASE audience_name WHEN 'rng_installs__all_trg_72510' AND scores_raw[0] < 0.25 THEN 1 WHEN 'rng_installs__all_trg_72510' AND scores_raw[0] < 0.32 THEN 2 WHEN 'rng_installs__all_trg_72510' AND scores_raw[0] < 1.01 THEN 10 ELSE 0 END) as float))"
            }
          ]
        },
        "min_target_rows": 1000000,
        "output_max_rows_per_file": 1000000,
        "max_collection_size": 100,
        "shuffle_partitions": 1000
      }
    }
  }
}
```
