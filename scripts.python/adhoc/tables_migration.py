"""
Re-create invalid tables
- create the new table
- insert rows to the new table from the old table
- check ...
- rename the old table
- rename the new table

How to run this script

- ssh hadoop-host
- vim ~/table_migration.py
- gdfs rm -f hdfs:/user/vlk/table_migration.py && \
  gdfs put ~/table_migration.py hdfs:/user/vlk/ && \
  gdfs chmod 666 hdfs:/user/vlk/table_migration.py && \
  gdfs cat hdfs:/user/vlk/table_migration.py | less # upload script to hdfs
- https://jenkins.host/view/snb-prj/job/snb-prj-manual-patch/build?delay=0sec
  PY_SCRIPT_HDFS_PATH: /user/vlk/table_migration.py
"""

import subprocess


class Stages:
    COPY_TABLE = "first step: create table and copy data"
    RENAME_TABLES = "second step: rename tables, only after thorough checking of copied data"
    NOOP = "safety on, do nothing"


# modify 2 parameters:
CURRENT_TABLE = "/data/prj/prod/hive/prj_source.db/hid_dataset_desc__raw_partial/"  # id
CURRENT_STEP = Stages.NOOP

# id -> ddl
TABLES_CREATE_DDL = {
    # hive> SHOW CREATE TABLE foo.bar;
    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc__raw_partial/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_desc__raw_partial_new (
  `uid` string,
  `user_search_vector` array<float>,
  `nearest_neighbours_profs` array<float>,
  `nearest_neighbours_vector` array<float>)
PARTITIONED BY (
  `dt` string,
  `uid_type` string)
STORED AS ORC;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc_0/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_desc_0_new (
  `uid` string,
  `shows_activity` array<float>,
  `nearest_neighbours_profs` array<float>,
  `nearest_neighbours_vector` array<float>,
  `user_search_vector` array<float>,
  `banner_cats_ctr_wscore_30d` map<string,float>,
  `hw_osm_ratios` map<string,float>,
  `hw_osm_stats` array<float>,
  `topics_motor200` map<string,float>,
  `living_region_ids` map<string,float>,
  `living_region_stats` array<float>,
  `all_profs` array<float>,
  `sn_epf` array<float>,
  `sn_topics100` array<float>,
  `app_stats` array<float>,
  `app_topics100` map<string,float>,
  `tp_os_counts` map<string,float>,
  `tp_device_stats` array<float>,
  `app_cats_activity` map<string,float>,
  `mob_operators` map<string,float>,
  `device_vendors` map<string,float>,
  `app_events` map<string,float>,
  `onelink_payments` map<string,float>,
  `onelink_logins` map<string,float>,
  `onelink_login_recencies` map<string,float>,
  `topgoal_topics200` map<string,float>,
  `mpop_senders` map<string,float>,
  `app_cats_pc_cri_wscore_180d` map<string,float>,
  `app_cats_installed` map<string,float>)
PARTITIONED BY (
  `dt` string,
  `uid_type` string COMMENT 'Type of uid, e.g. VKID, EMAIL, PHONE')
STORED AS ORC;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0__raw_partial/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_3_0__raw_partial_new (
  `uid` string, 
  `topics_motor200` map<string,float>, 
  `living_region_ids` map<string,float>, 
  `living_region_stats` array<float>, 
  `all_profs` array<float>, 
  `sn_epf` array<float>, 
  `sn_topics100` array<float>, 
  `app_stats` array<float>, 
  `app_topics100` map<string,float>, 
  `tp_os_counts` map<string,float>, 
  `tp_device_stats` array<float>, 
  `app_cats_activity` map<string,float>, 
  `mob_operators` map<string,float>, 
  `device_vendors` map<string,float>, 
  `app_events` map<string,float>, 
  `onelink_payments` map<string,float>, 
  `onelink_logins` map<string,float>, 
  `onelink_login_recencies` map<string,float>, 
  `topgoal_topics200` map<string,float>, 
  `mpop_senders` map<string,float>, 
  `app_cats_pc_cri_wscore_180d` map<string,float>, 
  `app_cats_installed` map<string,float>)
PARTITIONED BY ( 
  `dt` string, 
  `uid_type` string)
STORED AS ORC;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_3_0_new (
  `uid` string, 
  `shows_activity` array<float>, 
  `banner_cats_ctr_wscore_30d` map<string,float>, 
  `hw_osm_ratios` map<string,float>, 
  `hw_osm_stats` array<float>, 
  `topics_motor200` map<string,float>, 
  `living_region_ids` map<string,float>, 
  `living_region_stats` array<float>, 
  `all_profs` array<float>, 
  `sn_epf` array<float>, 
  `sn_topics100` array<float>, 
  `app_stats` array<float>, 
  `app_topics100` map<string,float>, 
  `tp_os_counts` map<string,float>, 
  `tp_device_stats` array<float>, 
  `app_cats_activity` map<string,float>, 
  `mob_operators` map<string,float>, 
  `device_vendors` map<string,float>, 
  `app_events` map<string,float>, 
  `onelink_payments` map<string,float>, 
  `onelink_logins` map<string,float>, 
  `onelink_login_recencies` map<string,float>, 
  `topgoal_topics200` map<string,float>, 
  `mpop_senders` map<string,float>, 
  `app_cats_pc_cri_wscore_180d` map<string,float>, 
  `app_cats_installed` map<string,float>)
PARTITIONED BY ( 
  `dt` string, 
  `uid_type` string COMMENT 'Type of uid, e.g. VKID, EMAIL, PHONE')
STORED AS ORC;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_2_0_dm8225/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_2_0_dm8225_new (
  `uid` string,
  `all_profs` array<float>,
  `living_regions` array<float>,
  `topics_motor200` map<string,float>,
  `sn_topics100` map<string,float>,
  `app_stats` array<float>,
  `app_providers` map<string,float>,
  `app_topics100` map<string,float>,
  `app_cats_pc_cri_wscore_180d` map<string,float>,
  `app_cats_pvc_cri_wscore_90d` map<string,float>,
  `app_cats_ctr_wscore_90d` map<string,float>,
  `banner_topics_ctr_wscore_30d` map<string,float>,
  `cats_installed` map<string,float>)
PARTITIONED BY (
  `dt` string,
  `uid_type` string)
STORED AS ORC;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_0_1/": """
CREATE TABLE IF NOT EXISTS prj_source.hid_dataset_0_1_new (
  `uid` string,
  `all_profs` array<float>,
  `topics_motor200` map<string,float>,
  `sn_topics100` map<string,float>,
  `living_regions` array<float>,
  `vk_epf_common` array<float>,
  `vk_epf_edu` array<float>,
  `vk_epf_work` array<float>,
  `vk_epf_family` array<float>,
  `vk_epf_chars` map<string,float>,
  `app_installs` array<float>,
  `app_providers` map<string,float>,
  `app_topics100` map<string,float>)
PARTITIONED BY (
  `dt` string,
  `uid_type` string)
STORED AS ORC;
    """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset_expanded/": """
CREATE TABLE IF NOT EXISTS snb_ds_segmentation.grinder_mobile_dataset_expanded_new (
  `uid` string,
  `all_profs` array<float>,
  `living_regions` array<float>,
  `app_installs` array<float>,
  `app_providers` map<string,float>,
  `app_topics100` map<string,float>)
PARTITIONED BY (
  `dt` string,
  `uid_type` string)
STORED AS ORC;
    """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset/": """
CREATE TABLE IF NOT EXISTS snb_ds_segmentation.grinder_mobile_dataset_new (
  `uid` string, 
  `all_profs` array<float>, 
  `living_regions` array<float>, 
  `app_installs` array<float>, 
  `app_providers` map<string,float>, 
  `app_topics100` map<string,float>
) PARTITIONED BY ( 
  `dt` string, 
  `uid_type` string COMMENT 'UID type (EMAIL, OKID, VKID, VID)')
STORED AS ORC;
        """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_desktop_dataset/": """
CREATE TABLE snb_ds_segmentation.grinder_desktop_dataset_new(
`uid` string COMMENT 'raw user id (email is not encrypted, not prefix)',
`topics_motor200` map<string,float>,
`sn_topics100` map<string,float>,
`all_profs` array<float>,
`living_regions` array<float>,
`vk_epf_common` array<float>,
`vk_epf_edu` array<float>,
`vk_epf_work` array<float>,
`vk_epf_family` array<float>,
`vk_epf_chars` map<string,float>)
PARTITIONED BY (
`dt` string,
`uid_type` string COMMENT 'uid_type VKID, OKID, EMAIL, VID')
STORED AS ORC;
        """,
}

# id -> cfg
INSERT_INTO_HIVE_PARAMS = {
    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc__raw_partial/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_desc__raw_partial where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_desc__raw_partial_new",
        "all_dts": [
            "2022-07-19",
        ],
        "batch_size": 5,
        "max_rows_per_bucket": "int(6e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc_0/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_desc_0 where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_desc_0_new",
        "all_dts": [
            "2022-07-18",
        ],
        "batch_size": 5,
        "max_rows_per_bucket": "int(11e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0__raw_partial/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_3_0__raw_partial where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_3_0__raw_partial_new",
        "all_dts": [
            "2022-07-18",
        ],
        "batch_size": 6,
        "max_rows_per_bucket": "int(11e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_3_0 where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_3_0_new",
        "all_dts": [
            "2022-07-17",
        ],
        "batch_size": 10,
        "max_rows_per_bucket": "int(12e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/data/prj/prod/hive/prj_source.db/hid_dataset_2_0_dm8225/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_2_0_dm8225 where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_2_0_dm8225_new",
        "all_dts": [
            "2022-07-11",
        ],
        "batch_size": 12,
        "max_rows_per_bucket": "int(13e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/data/prj/prod/hive/prj_source.db/hid_dataset_0_1/": {
        "source_expr": "SELECT * FROM prj_source.hid_dataset_0_1 where dt in (_list_of_dates_)",
        "target_db": "prj_source",
        "target_table": "hid_dataset_0_1_new",
        "all_dts": [
            "2022-06-02",
        ],
        "batch_size": 10,
        "max_rows_per_bucket": "int(13e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset_expanded/": {
        "source_expr": "SELECT * FROM snb_ds_segmentation.grinder_mobile_dataset_expanded",
        "target_db": "snb_ds_segmentation",
        "target_table": "grinder_mobile_dataset_expanded_new",
        "max_rows_per_bucket": "int(15e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset/": {
        "source_expr": "SELECT * FROM snb_ds_segmentation.grinder_mobile_dataset",
        "target_db": "snb_ds_segmentation",
        "target_table": "grinder_mobile_dataset_new",
        "max_rows_per_bucket": "int(15e5)",
        "app_name": "TRG-78476-insert_into_hive",
    },
}

# id -> ddl
TABLES_RENAME_DDL = {
    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc__raw_partial/": """
ALTER TABLE prj_source.hid_dataset_desc__raw_partial
RENAME TO   prj_source.hid_dataset_desc__raw_partial_old;
ALTER TABLE prj_source.hid_dataset_desc__raw_partial_new
RENAME TO   prj_source.hid_dataset_desc__raw_partial;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_desc_0/": """
ALTER TABLE prj_source.hid_dataset_desc_0
RENAME TO   prj_source.hid_dataset_desc_0_old;
ALTER TABLE prj_source.hid_dataset_desc_0_new
RENAME TO   prj_source.hid_dataset_desc_0;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0__raw_partial/": """
ALTER TABLE prj_source.hid_dataset_3_0__raw_partial
RENAME TO   prj_source.hid_dataset_3_0__raw_partial_old;
ALTER TABLE prj_source.hid_dataset_3_0__raw_partial_new
RENAME TO   prj_source.hid_dataset_3_0__raw_partial;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_3_0/": """
ALTER TABLE prj_source.hid_dataset_3_0
RENAME TO   prj_source.hid_dataset_3_0_old;
ALTER TABLE prj_source.hid_dataset_3_0_new
RENAME TO   prj_source.hid_dataset_3_0;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_2_0_dm8225/": """
ALTER TABLE prj_source.hid_dataset_2_0_dm8225
RENAME TO   prj_source.hid_dataset_2_0_dm8225_old;
ALTER TABLE prj_source.hid_dataset_2_0_dm8225_new
RENAME TO   prj_source.hid_dataset_2_0_dm8225;
    """,

    "/data/prj/prod/hive/prj_source.db/hid_dataset_0_1/": """
ALTER TABLE prj_source.hid_dataset_0_1
RENAME TO   prj_source.hid_dataset_0_1_old;
ALTER TABLE prj_source.hid_dataset_0_1_new
RENAME TO   prj_source.hid_dataset_0_1;
    """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset_expanded/": """
ALTER TABLE snb_ds_segmentation.grinder_mobile_dataset_expanded
RENAME TO   snb_ds_segmentation.grinder_mobile_dataset_expanded_old;
ALTER TABLE snb_ds_segmentation.grinder_mobile_dataset_expanded_new
RENAME TO   snb_ds_segmentation.grinder_mobile_dataset_expanded;
    """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset/": """
ALTER TABLE snb_ds_segmentation.grinder_mobile_dataset
RENAME TO   snb_ds_segmentation.grinder_mobile_dataset_old;
ALTER TABLE snb_ds_segmentation.grinder_mobile_dataset_new
RENAME TO   snb_ds_segmentation.grinder_mobile_dataset;
        """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_desktop_dataset/": """
ALTER TABLE snb_ds_segmentation.grinder_desktop_dataset
RENAME TO   snb_ds_segmentation.grinder_desktop_dataset_old;
ALTER TABLE snb_ds_segmentation.grinder_desktop_dataset_new
RENAME TO   snb_ds_segmentation.grinder_desktop_dataset;
        """,
}

SET_HIVE_VARS = """
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 10000;
SET hive.exec.max.dynamic.partitions = 100000;
SET hive.exec.max.created.files = 1000000;
SET hive.compute.query.using.stats = false;
"""

INSERT_INTO_HIVE_SPARK_APP = """
from pyspark.sql import SparkSession
from pyspark.ml.wrapper import JavaWrapper

def insert_into_hive(
        df,
        database,
        table,
        max_rows_per_bucket,
        overwrite=True,
        raise_on_missing_columns=True,
        check_parameter=None,
        jar="hdfs:/lib/prj-transformers-assembly-1.5.3.jar",
):
    df.sql_ctx.sql("ADD JAR " + jar)
    writer = JavaWrapper._create_from_java_class("com.prj.hive.Writer")
    writer._java_obj.insertIntoHive(
        df._jdf,
        database,
        table,
        max_rows_per_bucket,
        overwrite,
        raise_on_missing_columns,
        check_parameter,
    )

def main(spark):
    source_expr = "{source_expr}"
    print("Loading source: " + source_expr)
    df = spark.sql(source_expr).persist()
    print("Inserting rows: " + str(df.count()))
    insert_into_hive(
        df=df,
        database="{target_db}",
        table="{target_table}",
        max_rows_per_bucket={max_rows_per_bucket},
        overwrite=True,
        raise_on_missing_columns=True,
        check_parameter="prj_success_flag",
    )

if __name__ == "__main__":    
    spark = SparkSession.builder.appName("{app_name}").getOrCreate()
    main(spark)
    spark.stop()
"""

SPARK_SCRIPT_FILE_NAME = "insert_into_hive_spark_app.py"

# deprecated
TABLES_INSERT_DML = {
    "/dwh/snb/snb_ds_segmentation.db/grinder_mobile_dataset/": """
INSERT OVERWRITE TABLE snb_ds_segmentation.grinder_mobile_dataset_new
PARTITION (dt, uid_type)
SELECT * FROM snb_ds_segmentation.grinder_mobile_dataset;
        """,

    "/dwh/snb/snb_ds_segmentation.db/grinder_desktop_dataset/": """
INSERT OVERWRITE TABLE snb_ds_segmentation.grinder_desktop_dataset_new
PARTITION (dt, uid_type)
SELECT * FROM snb_ds_segmentation.grinder_desktop_dataset;
        """,
}


class SubprocessError(RuntimeError):
    def __init__(self, message=None, out=None, err=None):
        super(SubprocessError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err


def log(msg):
    print(msg)


def load_hive_cmd():
    return "hive".split(" ")  # ['hive']


def run_hive(args, check_return_code):
    cmd = load_hive_cmd() + args
    log("Run hive subprocess: {}\n".format(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if check_return_code and p.returncode != 0:
        raise SubprocessError(
            "Command: `{0}` failed with error code: {1}".format(" ".join(cmd), p.returncode),
            stdout,
            stderr
        )
    return stdout.decode("utf-8")


def run_hive_cmd(cmd, check_return_code=True):
    log("Run Hive command: `{}`\n".format(cmd))
    run_hive(args=["-e", cmd], check_return_code=check_return_code)


def create_table():
    log("Create the new table `{}`".format(CURRENT_TABLE))
    create_table_ddl = TABLES_CREATE_DDL[CURRENT_TABLE].strip()
    run_hive_cmd(create_table_ddl)


# deprecated
def insert_rows_hive():
    """Inserting OK but files size are un-even in partitions"""
    log("Insert rows to the new table `{}` from the old table".format(CURRENT_TABLE))
    insert_dml = TABLES_INSERT_DML[CURRENT_TABLE].strip()
    run_hive_cmd("{}\n{}".format(SET_HIVE_VARS.strip(), insert_dml))


def _insert_rows_spark(params):
    script = INSERT_INTO_HIVE_SPARK_APP.format(**params)
    log("Writing script {}:\n{}\n".format(SPARK_SCRIPT_FILE_NAME, script))
    with open(SPARK_SCRIPT_FILE_NAME, "w") as f:
        f.write(script)

    cmd = (
              "spark-submit --verbose --master yarn --deploy-mode cluster --queue dev.rb.priority"
              " --num-executors 2 --executor-cores 4 --executor-memory 8G --driver-memory 8G"
              " --conf spark.driver.maxResultSize=0"
              " --conf spark.executor.memoryOverhead=4G"
              " --conf spark.sql.shuffle.partitions=1024"
              " --conf spark.speculation=true"
              " --conf spark.dynamicAllocation.enabled=true"
              " --conf spark.dynamicAllocation.maxExecutors=384 --conf spark.dynamicAllocation.minExecutors=2"
              " --conf spark.hadoop.zlib.compress.level=BEST_COMPRESSION"
              " --conf spark.yarn.maxAppAttempts=2"
              " --conf spark.eventLog.enabled=true"
              " --conf spark.hadoop.hive.exec.dynamic.partition=true"
              " --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict"
              " --conf spark.hadoop.hive.exec.max.dynamic.partitions=1000000"
              " --conf spark.hadoop.hive.exec.max.dynamic.partitions.pernode=100000"
              " --conf spark.hadoop.hive.metastore.client.socket.timeout=900s"
          ).split(" ") + ["--name", params["app_name"], SPARK_SCRIPT_FILE_NAME]

    log("Starting cmd:\n{}\n".format(" ".join(cmd)))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise SubprocessError(
            "Command: `{0}` failed with error code: {1}".format(" ".join(cmd), p.returncode),
            stdout,
            stderr
        )


def insert_rows_spark():
    params = INSERT_INTO_HIVE_PARAMS[CURRENT_TABLE]
    source_expr = params["source_expr"]
    all_dts = params["all_dts"]
    batch_size = params.get("batch_size", 10)

    batches = [all_dts[i: i + batch_size] for i in range(0, len(all_dts), batch_size)]
    dates_expressions = ["'{}'".format("','".join(batch)) for batch in batches]

    for list_of_dates in dates_expressions:
        actual_params = dict(params, source_expr=source_expr.replace("_list_of_dates_", list_of_dates))
        actual_params.pop("all_dts")
        actual_params.pop("batch_size")
        _insert_rows_spark(actual_params)


def insert_rows():
    # return insert_rows_hive()  # easy but files size uneven
    return insert_rows_spark()


def rename_tables():
    log("Rename current table `{}` to old, rename new table to current".format(CURRENT_TABLE))
    rename_tables_ddl = TABLES_RENAME_DDL[CURRENT_TABLE].strip()
    run_hive_cmd(rename_tables_ddl)


def main():
    log("Current step: {}".format(CURRENT_STEP))
    if CURRENT_STEP == Stages.COPY_TABLE:
        create_table()
        insert_rows()
        log("\nCHECK THAT OLD AND NEW TABLES ARE EQUAL!!!\n")

    # only after thorough checking!
    # rename the old table
    # rename the new table
    elif CURRENT_STEP == Stages.RENAME_TABLES:
        rename_tables()
        log("\nCHECK THAT NEW TABLE HAVE EXPECTED PROPERTIES!!!\n")
    else:
        raise RuntimeError("Unexpected step: `{}`".format(CURRENT_TABLE))


if __name__ == "__main__":
    log("Starting main ...")
    main()
    log("Main is done.")
