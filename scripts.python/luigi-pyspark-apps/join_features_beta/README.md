# Description

Join almost arbitrary datasets to standartized ML features data-mart.
Extra app features: UID matching (mapping) with columns aggregation.

Input tables partitions, output table partitions -- Hive (uid, uid_type) partitioned tables.
* Input: config, facts, dimensions, matching table.
* Output: features data-mart partitions.

UID: Universal ID, not User ID.
Global unique universal ID: touple (uid, uid_type).

All tables should have partitioning by dt column (YYYY-MM-DD).

## Control config examples

### w/o uid matching

```json
{
  "vars": {"date": "{{ datetime.date.today() - datetime.timedelta(days=1) }}"},
  "stages": ["join"],
  "jobs": {
    "JoinSomeFeatures": {
      "stage": "join",
      "xapp": "JoinerFeatures",
      "trigger": "all_success",
      "tags": ["default"],
      "config": {
        "target_dt": "{{ vars.date }}",
        "target_db": "dmgrinder_dev_source",
        "target_table": "some_test_dataset",
        "uid_types": ["VKID", "OKID"],
        "feature_sources": {
          "some_features_source1": {
            "db": "dmgrinder_source",
            "table": "some_features_source1",
            "partition_conf": {
              "uid_type": ["VKID", "OKID"]
            },
            "period": 1,
            "dt_selection_mode": "single_last",
            "md_sources": [
              {
                "db": "dmgrinder_source",
                "table": "some_extension",
                "max_dt_diff": 33,
                "select": {
                  "package_id": "cast(id as string)",
                  "some_stats": "counts"
                },
                "where": "id is not null",
                "join_conf": {
                  "on": ["package_id"],
                  "how": "left"
                }
              }
            ],
            "where": "age > 21",
            "features": [
              {"feature1": "cast(some_stats as float)"}
            ],
            "uids": {"uid_type":  "uid"},
            "checkpoint": false
          },
          "some_features_source2": {
            "db": "dmgrinder_source",
            "table": "some_features_source2",
            "partition_conf": {
              "uid_type": ["VKID"]
            },
            "period": 1,
            "dt_selection_mode": "single_last",
            "features": [
              {"feature2": "feature2"},
              {"feature3": "feature5"},
              {"feature4": "feature4"},
              {"feature5": "feature5"}
            ]
          }
        },
        "join_rule": "some_features_source1 full_outer some_features_source2",
        "domains": [
          {"name": "primitive_domain", "type": "float", "source": "some_features_source1"},
          {"name": "array_domain", "type": "array<float>", "source": "some_features_source2", "columns": ["feature2", "feature3"]},
          {"name": "map_domain", "type": "map<string,float>", "source": "some_features_source2", "columns": ["feature4", "feature5"]}
        ],
        "final_columns": [
          "cast(primitive_domain_feature1 as float) as f1",
          "cast(array_domain as array<float>) as f2",
          "cast(map_domain as map<string,float>) as f3"
        ],
        "strict_check_array": false
      }
    }
  }
}
```

### with uid matching

```json
{
  "vars": {"date": "{{ datetime.date.today() - datetime.timedelta(days=1) }}"},
  "stages": ["join"],
  "jobs": {
    "JoinSomeFeatures": {
      "stage": "join",
      "xapp": "JoinerFeatures",
      "trigger": "all_success",
      "tags": ["default"],

      "config": {

        "target_dt": "{{ vars.date }}",
        "target_db": "dmgrinder_dev_source",
        "target_table": "some_test_dataset",
        "uid_types": [
          {
            "output": "HID",
            "matching": {
              "input": ["VKID", "OKID"],
              "db": "cdm_hid",
              "table": "hid_stable_matching",
              "max_dt_diff": 33
            }
          }
        ],
        "feature_sources": {

          "some_features_source1": {
            "db": "dmgrinder_source",
            "table": "some_features_source1",
            "partition_conf": {
              "uid_type": ["VKID", "OKID"]
            },
            "period": 1,
            "dt_selection_mode": "single_last",
            "md_sources": [
              {
                "db": "dmgrinder_source",
                "table": "some_extension",
                "max_dt_diff": 33,
                "select": {
                  "package_id": "cast(id as string)",
                  "some_stats": "counts"
                },
                "where": "id is not null",
                "join_conf": {
                  "on": ["package_id"],
                  "how": "left"
                }
              }
            ],
            "where": "age > 21",
            "features": [
              {"feature1": "gavg(cast(some_stats as float))"}
            ],
            "agg": "true",
            "uids": {"uid_type":  "uid"},
            "checkpoint": true
          },

          "some_features_source2": {
            "db": "dmgrinder_source",
            "table": "some_features_source2",
            "partition_conf": {
              "uid_type": ["VKID"]
            },
            "period": 1,
            "dt_selection_mode": "single_last",
            "features": [
              {"feature2": "gmax(feature2)"},
              {"feature3": "gmax(feature2)"},
              {"feature4": "gmax(feature2)"},
              {"feature5": "gmax(feature2)"}
            ],
            "agg": "true"
          }

        },
        "join_rule": "some_features_source1 full_outer some_features_source2",
        "domains": [
          {"name": "primitive_domain", "type": "float", "source": "some_features_source1"},
          {"name": "array_domain", "type": "array<float>", "source": "some_features_source2", "columns": ["feature2", "feature3"]},
          {"name": "map_domain", "type": "map<string,float>", "source": "some_features_source2", "columns": ["feature4", "feature5"]}
        ],
        "final_columns": [
          "cast(primitive_domain_feature1 as float) as f1",
          "cast(array_domain as array<float>) as f2",
          "cast(map_domain as map<string,float>) as f3"
        ],
        "strict_check_array": false
      }
    }
  }
}
```
