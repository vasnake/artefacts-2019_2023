# Description

ETL (ELT) for universal features data-mart.

### Config examples
#### Feature `float` with filter by `uid` and mapping via `banner-user`
```json
{
  "feature_name": "some_score",
  "feature_hashing": false,
  "target_dt": "2020-06-01",
  "target_db": "ds_scoring",
  "target_table": "dm_universal_feature",
  "source_db": "ds_auditories",
  "source_table": "clal_retro_audience",
  "source_partition_conf": {
    "uid_type": "GAID",
    "category": "positive",
    "audience_name": ["ru.mail", "spa.games", "zombie.survival.dead.shooting"]
  },
  "period": 1,
  "dt_selection_mode": "single_last",
  "combine_columns": {
    "uid": "uid",
    "uid_type": "uid_type",
    "bu_link_id": {
      "expr": "audience_name",
      "matching_config": {
        "db": "md_mobile",
        "table": "mobile_app",
        "partition_conf": {},
        "join_conf": {
          "type": "skewed",
          "salt_parts": 100
        },
        "bu_link_id_from": "cast(store_id as string)",
        "bu_link_id_to": "cast(id as string)"
      }
    },
    "feature": "avg(score)"
  },
  "filter_config": {
    "db": "cdm_hid",
    "table": "hid_stable_matching",
    "partition_conf": {
      "uid1_type": "GAID",
      "uid2_type": "HID"
    },
    "columns": {
      "uid": "uid1",
      "uid_type": "uid1_type"
    }
  },  
  "min_target_rows": 1000000,
  "max_collection_size": 100,
  "shuffle_partitions": 400
}
```

#### Feature `array<float>`

```json
{
  "vars": {
    "audience_names": [
      "dm8569_compose_agg7_cat__svyaz",
      "dm8569_compose_agg7_cat__finansi",
      "dm8569_compose_agg7_cat__meditsina"
    ]
  },
  "stages": [
    "combine"
  ],
  "jobs": {
    "combine:CombineUniversalFeatures__bu_features": {
      "stage": "combine",
      "xapp": "CombineUniversalFeatures",
      "trigger": "all_success",
      "tags": [
        "default"
      ],
      "config": {
        "feature_name": "some_lal_scores",
        "feature_hashing": false,
        "target_dt": "2021-10-01",
        "target_db": "ds_scoring",
        "target_table": "dm_universal_feature",
        "source_db": "ds_auditories",
        "source_table": "clal_retro_audience",
        "source_partition_conf": {
          "audience_name": "{{ vars.audience_names }}",
          "category": "positive",
          "uid_type": [
            "IDFA",
            "GAID"
          ]
        },
        "period": 1,
        "dt_selection_mode": "single_last",
        "combine_columns": {
          "uid": "uid",
          "uid_type": "uid_type",
          "feature": "map_values_ordered(user_dmdesc.collect(audience_name, cast(score as float)), array('{{ \"', '\".join(vars.audience_names) }}'))"
        },
        "min_target_rows": 100000,
        "max_collection_size": 100,
        "shuffle_partitions": 400
      }
    }
  }
}
```

#### Feature `array<float>` with `bu_link_id`

```json
{
  "vars": {
    "date": "{{ datetime.date.today() - datetime.timedelta(days=2) }}",
    "feature_name": "bu_features",
    "audience_names": [
      "dm8569_compose_agg7_cat__meditsina",
      "dm8569_compose_agg7_cat__pokupki",
      "dm8569_compose_agg7_cat__svyaz"
    ],
    "categories": [
      "positive"
    ],
    "uid_types": [
      "IDFA",
      "GAID"
    ]
  },
  "stages": [
    "combine"
  ],
  "jobs": {
    "combine:CombineUniversalFeatures__bu_features": {
      "stage": "combine",
      "xapp": "CombineUniversalFeatures",
      "trigger": "all_success",
      "tags": [
        "default"
      ],
      "config": {
        "feature_name": "{{ vars.feature_name }}",
        "feature_hashing": false,
        "target_dt": "{{ vars.date }}",
        "target_db": "ds_scoring",
        "target_table": "dm_universal_feature",
        "source_db": "ds_auditories",
        "source_table": "clal_retro_audience",
        "source_partition_conf": {
          "audience_name": "{{ vars.audience_names }}",
          "category": "{{ vars.categories }}",
          "uid_type": "{{ vars.uid_types }}"
        },
        "period": 1,
        "dt_selection_mode": "single_last",
        "combine_columns": {
          "uid": "uid",
          "uid_type": "uid_type",
          "bu_link_id": {
            "expr": "CASE WHEN audience_name = 'dm8569_compose_agg7_cat__meditsina' THEN 101 WHEN audience_name = 'dm8569_compose_agg7_cat__pokupki' THEN 110 ELSE 111 END"
          },
          "feature": "map_values_ordered(user_dmdesc.collect(category, cast(score as float)), array('{{ \"', '\".join(vars.categories) }}'))"
        },
        "min_target_rows": 100000,
        "max_collection_size": 100,
        "shuffle_partitions": 400
      }
    }
  }
}
```

Target table schema:
```
uid           string
bu_link_id    string
score         float
score_list    array<float>
score_map     map<string,float>
cat_list      array<string>
feature_name  string
dt            string
uid_type      string

# Partition Information
feature_name  string
dt            string
uid_type      string
```

[user_dmdesc.collect]: https://github.com/klout/brickhouse/blob/863a370820f64a7825c337f708116149978c097a/src/main/java/brickhouse/udf/collect/CollectUDAF.java#L46
