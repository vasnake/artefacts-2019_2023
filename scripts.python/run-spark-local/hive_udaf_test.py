import os
import six

from pyspark.sql import SparkSession, functions as sqlfn
from pyspark.sql.types import StructType, StructField, StringType, MapType, FloatType, ArrayType, DoubleType

conf = {
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.ui.enabled": "false",
    "spark.driver.host": "localhost",
    "spark.default.parallelism": 3,
    "spark.sql.shuffle.partitions": 3,
    # files
    # "spark.checkpoint.dir": "/tmp/DM-8709/spark-checkpoints",
    # "spark.sql.warehouse.dir": "/tmp/DM-8709/warehouse",
    # "spark.driver.extraJavaOptions": "-Dderby.system.home=/tmp/DM-8709",
    # hive 2.1 support
    # "spark.sql.hive.metastore.version": "2.1.1",
    # "spark.sql.hive.metastore.jars": os.environ["HIVE_METASTORE_JARS"],
    # "spark.sql.catalogImplementation": "hive",
}

spark = SparkSession.builder.master("local[3]")
spark = spark.enableHiveSupport()
for k, v in six.iteritems(conf):
    spark = spark.config(k, v)
spark_session = spark.getOrCreate()

spark = spark_session

# @formatter:off

df = spark.createDataFrame(
    # uid, score, score_double, score_list, score_list_double, score_map, score_map_double, feature_name, dt, uid_type
    spark.sparkContext.parallelize([
        # one good value for each field
        ("a", 1.0, 2.0, [float(x) for x in range(3, 6)], [float(x) for x in range(6, 9)], {"1": 1., "2": 2., "3": 3.}, {"4": 4., "6": 6., "8": 8.}, "foo", "2020-10-15", "HID"),
        # set of primitive values
        ("b", 2.,           None,                       None, None, None, None, "foo", "2020-10-15", "HID"),
        ("b", None,         3.,                         None, None, None, None, "foo", "2020-10-15", "HID"),
        ("b", 1.,           float("nan"),               None, None, None, None, "foo", "2020-10-15", "HID"),
        ("b", float("nan"), 4.,                         None, None, None, None, "foo", "2020-10-15", "HID"),
        ("b", float("+Infinity"), float("-Infinity"),   None, None, None, None, "foo", "2020-10-15", "HID"),
        # set of array<float>
        ("c", None, None, [1., 3.5], None, None, None, "foo", "2020-10-15", "HID"),
        ("c", None, None, [4., 2.5], None, None, None, "foo", "2020-10-15", "HID"),
        # set of array<double>
        ("d", None, None, None, [4., 2.5], None, None, "foo", "2020-10-15", "HID"),
        ("d", None, None, None, [1., 3.5], None, None, "foo", "2020-10-15", "HID"),
        # set of map<string,float>
        ("e", None, None, None, None, {"2": 1., "3": 4.},   None, "foo", "2020-10-15", "HID"),
        ("e", None, None, None, None, {"2": 3., "3": 2.},   None, "foo", "2020-10-15", "HID"),
        ("e", None, None, None, None, {"5": 5.},            None, "foo", "2020-10-15", "HID"),
        # set of map<string,double>
        ("f", None, None, None, None, None, {"2": 3., "3": 2.},   "foo", "2020-10-15", "HID"),
        ("f", None, None, None, None, None, {"2": 1., "3": 4.},   "foo", "2020-10-15", "HID"),
        ("f", None, None, None, None, None, {"5": 5.},            "foo", "2020-10-15", "HID"),
        # set of string features
        ("g", None, None, None, None, None, None, "foo", "2020-10-15", "VID"),
        ("g", None, None, None, None, None, None, "foo", "2020-10-15", "HID"),
        ("g", None, None, None, None, None, None, "foo", "2020-10-15", "OKID"),
        ("g", None, None, None, None, None, None, "foo", "2020-10-15", "HID"),
        ("g", None, None, None, None, None, None, "foo", "2020-10-15", "VID"),
        # set of int features
        ("h", 1., None, None, None, None, None, "foo", "2020-10-15", "VID"),
        ("h", 2., None, None, None, None, None, "foo", "2020-10-15", "HID"),
        ("h", 3., None, None, None, None, None, "foo", "2020-10-15", "OKID"),
        ("h", 2., None, None, None, None, None, "foo", "2020-10-15", "HID"),
        ("h", 1., None, None, None, None, None, "foo", "2020-10-15", "VID"),
        # set of null features
        ("i", None, None, None, None, None, None, "1", None, None),
        ("i", None, None, None, None, None, None, "2", None, None),
        ("i", None, None, None, None, None, None, "3", None, None),
        ("i", None, None, None, None, None, None, "4", None, None),
        ("i", None, None, None, None, None, None, "5", None, None),
        # all is null but one value
        ("j", None, None, None, None, None, None, "1", None, None),
        ("j", None, None, None, None, None, None, "2", None, None),
        ("j", 42.,  None, None, None, None, None, "3", None, "HID"),
        ("j", None, None, None, None, None, None, "4", None, None),
        ("j", None, None, None, None, None, None, "5", None, None),
        # not empty set with choice
        ("k", None, None, None, None, None, None, "1", None, None),
        ("k", 42.,  None, None, None, None, None, "2", None, "HID"),
        ("k", 42.,  None, None, None, None, None, "3", None, "HID"),
        ("k", 33.,  None, None, None, None, None, "4", None, "AAA"),
        ("k", 33.,  None, None, None, None, None, "5", None, "AAA"),
        ("k", 1.,   None, None, None, None, None, "6", None, "000"),
        # set of null and nan features
        ("l", None,         None,           [None],         [None],         {"nan": None},          {"nan": None},          "1", None, None),
        ("l", None,         None,           [None],         [None],         {"nan": None},          {"nan": None},          "2", None, None),
        ("l", float("nan"), float("nan"),   [float("nan")], [float("nan")], {"nan": float("nan")},  {"nan": float("nan")},  "3", None, None),
        ("l", None,         None,           [None],         [None],         {"nan": None},          {"nan": None},          "4", None, None),
        ("l", None,         None,           [None],         [None],         {"nan": None},          {"nan": None},          "5", None, None),
        # set of all kind of values
        ("m", None,         None,           [None],           [None],           {"nan": None},          {"nan": None},          "1", None, None),
        ("m", 1.,           3.,             [5., 6.],         [9., 1.],         {"nan": 4.},            {"nan": 6.},            "2", None, None),
        ("m", float("nan"), float("nan"),   [float("nan")],   [float("nan")],   {"nan": float("nan")},  {"nan": float("nan")},  "3", None, None),
        ("m", 2.,           4.,             [7., 8.],         [2., 3.],         {"nan": 5.},            {"nan": 7.},            "4", None, None),
        ("m", None,         None,           [None],           [None],           {"nan": None},          {"nan": None},          "5", None, None),
        # set of null and empty collections
        ("n", None, None, None, None, None, None, "1", None, None),
        ("n", None, None, None, None, None, None, "2", None, None),
        ("n", None, None, [],   [],   {},   {},   "3", None, None),
        ("n", None, None, None, None, None, None, "4", None, None),
        ("n", None, None, None, None, None, None, "5", None, None),
        # null in key
        ("o", None,         None, None,                         None, None,               None, "1", None, None),
        ("o", float("nan"), None, [],                           None, {},                 None, "2", None, None),
        ("o", None,         None, [None, None],                 None, {"": 1.},           None, "3", None, None),
        ("o", 0.123,        None, [float("nan"), float("nan")], None, {"": 2., "k": 3.},  None, "4", None, None),
        ("o", 123.0,        None, [None, 3.3, None],            None, {"": float("nan")}, None, "5", None, None),
        ("o", None,         None, None,                         None, {"null": None},     None, "6", None, None),
        # uid, score, score_double, score_list, score_list_double, score_map, score_map_double, feature_name, dt, uid_type
    ]),
    StructType([
        StructField(name, _type)
        for name, _type in [
            ("uid", StringType()),
            ("score", FloatType()),
            ("score_double", DoubleType()),
            ("score_list", ArrayType(FloatType())),
            ("score_list_double", ArrayType(DoubleType())),
            ("score_map", MapType(StringType(), FloatType())),
            ("score_map_double", MapType(StringType(), DoubleType())),
            ("feature_name", StringType()),
            ("dt", StringType()),
            ("uid_type", StringType())
        ]
    ])
).repartition("feature_name").cache()

assert df.count() == 63
df.printSchema()
df.show(200, truncate=False)
df.createOrReplaceTempView("features")

spark.sql("DROP TEMPORARY FUNCTION IF EXISTS min_features")
spark.sql("DROP TEMPORARY FUNCTION IF EXISTS max_features")
spark.sql("DROP TEMPORARY FUNCTION IF EXISTS sum_features")
spark.sql("DROP TEMPORARY FUNCTION IF EXISTS avg_features")
spark.sql("DROP TEMPORARY FUNCTION IF EXISTS most_freq_features")

spark.sql("CREATE TEMPORARY FUNCTION min_features AS 'hive.udaf.features.GenericMinUDAF'")
spark.sql("CREATE TEMPORARY FUNCTION max_features AS 'hive.udaf.features.GenericMaxUDAF'")
spark.sql("CREATE TEMPORARY FUNCTION sum_features AS 'hive.udaf.features.GenericSumUDAF'")
spark.sql("CREATE TEMPORARY FUNCTION avg_features AS 'hive.udaf.features.GenericAvgUDAF'")
spark.sql("CREATE TEMPORARY FUNCTION most_freq_features AS 'hive.udaf.features.GenericMostFreqUDAF'")


def smoke_test():
    for qry in [
        "select uid, min_features(score), min_features(score_double) from features group by uid",
        "select uid, min_features(score_list), min_features(score_list_double) from features group by uid",
        "select uid, min_features(score_list) from features where uid='c' group by uid",
        "select uid, min_features(score_list_double) from features where uid='d' group by uid",
        "select uid, min_features(score_map) from features where uid='e' group by uid",
        "select uid, min_features(score_map_double) from features where uid='f' group by uid",

        "select uid, max_features(score), max_features(score_double) from features where uid='b' group by uid",
        "select uid, max_features(score_list) from features where uid='c' group by uid",
        "select uid, max_features(score_list_double) from features where uid='d' group by uid",
        "select uid, max_features(score_map) from features where uid='e' group by uid",
        "select uid, max_features(score_map_double) from features where uid='f' group by uid",

        "select uid, sum_features(score) from features where uid='b' group by uid",
        "select uid, sum_features(score_double) from features where uid='b' group by uid",
        "select uid, sum_features(score_list) from features where uid='c' group by uid",
        "select uid, sum_features(score_list_double) from features where uid='d' group by uid",
        "select uid, sum_features(score_map) from features where uid='e' group by uid",
        "select uid, sum_features(score_map_double) from features where uid='f' group by uid",

        "select uid, avg_features(score) from features where uid='a' group by uid",
        "select uid, avg_features(score) from features where uid='b' group by uid",
        "select uid, avg_features(score_double) from features where uid='a' group by uid",
        "select uid, avg_features(score_double) from features where uid='b' group by uid",
        "select uid, avg_features(score_list) from features where uid='a' group by uid",
        "select uid, avg_features(score_list) from features where uid='c' group by uid",
        "select uid, avg_features(score_list_double) from features where uid='a' group by uid",
        "select uid, avg_features(score_list_double) from features where uid='d' group by uid",
        "select uid, avg_features(score_map) from features where uid='a' group by uid",
        "select uid, avg_features(score_map) from features where uid='e' group by uid",
        "select uid, avg_features(score_map_double) from features where uid='a' group by uid",
        "select uid, avg_features(score_map_double) from features where uid='f' group by uid",

        # most_freq_features(feature, rnd, threshold, prefer)
        "select uid, most_freq_features(uid_type) from features where uid='a' group by uid",
        "select uid, most_freq_features(cast(score as int)) from features where uid='a' group by uid",
        "select uid, most_freq_features(uid_type, cast(0 as int), cast(0.2 as float), cast('OKID' as string)) from features where uid='g' group by uid",
        "select uid, most_freq_features(cast(score as int), cast(0 as int), cast(0.2 as float), cast(3 as int)) from features where uid='h' group by uid",
        "select uid, most_freq_features(uid_type, cast(0 as int), cast(0.3 as float), cast(null as string)) from features where uid='g' group by uid",
        "select uid, most_freq_features(cast(score as int), cast(0 as int), cast(0.3 as float), cast(null as int)) from features where uid='h' group by uid",
        "select uid, most_freq_features(uid_type, cast(null as int), cast(null as float), cast(null as string)) from features where uid='g' group by uid",
        "select uid, most_freq_features(cast(score as int), cast(null as int), cast(null as float), cast(null as int)) from features where uid='h' group by uid",
        "select uid, most_freq_features(cast(score as int), null, null, null) from features where uid='h' group by uid",
    ]:
        df = spark.sql(qry)
        df.printSchema()
        df.show(truncate=False)


def assert_qry_result(qry, expected_type, expected_result):
    df = spark.sql(qry)
    res = df.collect()
    print("\nquery: `{}`\nres.dataType: `{}`, res.size: `{}`, res[0]: `{}`, res[0][x]: `{}`\nexpected.type: `{}`, expected.res[0][x]: `{}`\n".format(
        qry, df.schema[0].dataType.__repr__(), len(res), res[0], res[0]["x"].__repr__(), expected_type, expected_result
    ))
    assert df.schema[0].dataType.__repr__() == expected_type
    assert len(res) == 1
    assert res[0]["x"].__repr__() == expected_result


def get_result(qry):
    df = spark.sql(qry)
    res = df.collect()
    return res[0]["x"].__repr__()


def assert_rnd(qry, repeat=33):
    xs = set()
    for i in range(repeat):
        xs.add(get_result(qry))
        if len(xs) > 1:
            break
    print("\ncheck rnd, qry: `{}`, results: `{}`\n".format(qry, xs))
    assert len(xs) > 1


def check_four(qry, expected_type, expected_min, expected_max, expected_sum, expected_avg):
    for fn, er in [("min_features", expected_min), ("max_features", expected_max), ("sum_features", expected_sum), ("avg_features", expected_avg)]:
        assert_qry_result(qry.format(fn), expected_type, er)


def check_most_freq():
    # - all values is null => null, string|int
    assert_qry_result("select most_freq_features(uid_type) as x from features where uid='i' group by uid", "StringType", "None")
    assert_qry_result("select most_freq_features(uid_type, cast(0 as int), cast(0.0001 as float), cast('foo' as string)) as x from features where uid='i' group by uid", "StringType", "None")
    assert_qry_result("select most_freq_features(cast(score as int)) as x from features where uid='i' group by uid", "IntegerType", "None")
    assert_qry_result("select most_freq_features(cast(score as int), cast(0 as int), cast(0.0001 as float), cast(42 as int)) as x from features where uid='i' group by uid", "IntegerType", "None")

    # - all values but one is null => not null value, string|int
    assert_qry_result("select most_freq_features(uid_type) as x from features where uid='j' group by uid", "StringType", "u'HID'")
    assert_qry_result("select most_freq_features(uid_type, cast(0 as int), cast(0.0001 as float), cast('foo' as string)) as x from features where uid='j' group by uid", "StringType", "u'HID'")
    assert_qry_result("select most_freq_features(cast(score as int)) as x from features where uid='j' group by uid", "IntegerType", "42")
    assert_qry_result("select most_freq_features(cast(score as int), cast(0 as int), cast(0.0001 as float), cast(33 as int)) as x from features where uid='j' group by uid", "IntegerType", "42")

    # - fixed rnd => selected top value
    assert_qry_result("select most_freq_features(uid_type, cast(1 as int), cast(0.0001 as float), cast('foo' as string)) as x from features where uid='k' group by uid", "StringType", "u'HID'")
    assert_qry_result("select most_freq_features(uid_type, cast(0 as int), cast(0.0001 as float), cast('foo' as string)) as x from features where uid='k' group by uid", "StringType", "u'AAA'")
    assert_qry_result("select most_freq_features(cast(score as int), cast(1 as int), cast(0.0001 as float), cast(99 as int)) as x from features where uid='k' group by uid", "IntegerType", "42")
    assert_qry_result("select most_freq_features(cast(score as int), cast(0 as int), cast(0.0001 as float), cast(99 as int)) as x from features where uid='k' group by uid", "IntegerType", "33")

    # - rnd is null => different values from top for repeating calls
    assert_rnd("select most_freq_features(uid_type) as x from features where uid='k' group by uid")
    assert_rnd("select most_freq_features(cast(score as int)) as x from features where uid='k' group by uid")

    # - threshold is not null => null value if threshold is too high
    assert_qry_result("select most_freq_features(uid_type, cast(null as int), cast(0.41 as float), cast(null as string)) as x from features where uid='k' group by uid", "StringType", "None")
    assert_qry_result("select most_freq_features(cast(score as int), cast(null as int), cast(0.41 as float), cast(null as int)) as x from features where uid='k' group by uid", "IntegerType", "None")
    assert_qry_result("select most_freq_features(uid_type, cast(0 as int), cast(0.4 as float), cast(null as string)) as x from features where uid='k' group by uid", "StringType", "u'AAA'")
    assert_qry_result("select most_freq_features(cast(score as int), cast(0 as int), cast(0.4 as float), cast(null as int)) as x from features where uid='k' group by uid", "IntegerType", "33")

    # - prefer is not null => top value if no prefer in set; prefer value if prefer in set ignoring the threshold.
    assert_qry_result("select most_freq_features(uid_type, cast(0 as int), cast(0.4 as float), cast('foo' as string)) as x from features where uid='k' group by uid", "StringType", "u'AAA'")
    assert_qry_result("select most_freq_features(cast(score as int), cast(0 as int), cast(0.4 as float), cast(77 as int)) as x from features where uid='k' group by uid", "IntegerType", "33")
    assert_qry_result("select most_freq_features(uid_type, null, cast(1 as float), cast('000' as string)) as x from features where uid='k' group by uid", "StringType", "u'000'")
    assert_qry_result("select most_freq_features(cast(score as int), null, cast(1 as float), cast(1 as int)) as x from features where uid='k' group by uid", "IntegerType", "1")


def check_min_max_sum_avg():
    # - all null => null
    check_four("select {}(score) as x from features where uid='i' group by uid", "FloatType", "None", "None", "None", "None")
    check_four("select {}(score_double) as x from features where uid='i' group by uid", "DoubleType", "None", "None", "None", "None")
    check_four("select {}(score_list) as x from features where uid='i' group by uid", "ArrayType(FloatType,true)", "None", "None", "None", "None")
    check_four("select {}(score_list_double) as x from features where uid='i' group by uid", "ArrayType(DoubleType,true)", "None", "None", "None", "None")
    check_four("select {}(score_map) as x from features where uid='i' group by uid", "MapType(StringType,FloatType,true)", "None", "None", "None", "None")
    check_four("select {}(score_map_double) as x from features where uid='i' group by uid", "MapType(StringType,DoubleType,true)", "None", "None", "None", "None")

    # - all null and nan => nan
    check_four("select {}(score) as x from features where uid='l' group by uid", "FloatType", "nan", "nan", "nan", "nan")
    check_four("select {}(score_double) as x from features where uid='l' group by uid", "DoubleType", "nan", "nan", "nan", "nan")
    check_four("select {}(score_list) as x from features where uid='l' group by uid", "ArrayType(FloatType,true)", "[nan]", "[nan]", "[nan]", "[nan]")
    check_four("select {}(score_list_double) as x from features where uid='l' group by uid", "ArrayType(DoubleType,true)", "[nan]", "[nan]", "[nan]", "[nan]")
    check_four("select {}(score_map) as x from features where uid='l' group by uid", "MapType(StringType,FloatType,true)", "{u'nan': nan}", "{u'nan': nan}", "{u'nan': nan}", "{u'nan': nan}")
    check_four("select {}(score_map_double) as x from features where uid='l' group by uid", "MapType(StringType,DoubleType,true)", "{u'nan': nan}", "{u'nan': nan}", "{u'nan': nan}", "{u'nan': nan}")

    # - null, nan, valid values => valid min/max/sum/avg value
    check_four("select {}(score) as x from features where uid='m' group by uid", "FloatType", "1.0", "2.0", "3.0", "1.5")  # min, max, sum, avg
    check_four("select {}(score_double) as x from features where uid='m' group by uid", "DoubleType", "3.0", "4.0", "7.0", "3.5")
    check_four("select {}(score_list) as x from features where uid='m' group by uid", "ArrayType(FloatType,true)", "[5.0, 6.0]", "[7.0, 8.0]", "[12.0, 14.0]", "[6.0, 7.0]")
    check_four("select {}(score_list_double) as x from features where uid='m' group by uid", "ArrayType(DoubleType,true)", "[2.0, 1.0]", "[9.0, 3.0]", "[11.0, 4.0]", "[5.5, 2.0]")
    check_four("select {}(score_map) as x from features where uid='m' group by uid", "MapType(StringType,FloatType,true)", "{u'nan': 4.0}", "{u'nan': 5.0}", "{u'nan': 9.0}", "{u'nan': 4.5}")
    check_four("select {}(score_map_double) as x from features where uid='m' group by uid", "MapType(StringType,DoubleType,true)", "{u'nan': 6.0}", "{u'nan': 7.0}", "{u'nan': 13.0}", "{u'nan': 6.5}")

    # - null and empty collection => empty collection
    check_four("select {}(score_list) as x from features where uid='n' group by uid", "ArrayType(FloatType,true)", "[]", "[]", "[]", "[]")
    check_four("select {}(score_list_double) as x from features where uid='n' group by uid", "ArrayType(DoubleType,true)", "[]", "[]", "[]", "[]")
    check_four("select {}(score_map) as x from features where uid='n' group by uid", "MapType(StringType,FloatType,true)", "{}", "{}", "{}", "{}")
    check_four("select {}(score_map_double) as x from features where uid='n' group by uid", "MapType(StringType,DoubleType,true)", "{}", "{}", "{}", "{}")


# all aggregations in one query
def all_agg():
    qry = """
    select 
        dt, 
        min_features(score) as min_float, min_features(score_double) as min_double, 
            min_features(score_list) as min_float_list, min_features(score_list_double) as min_double_list,
            min_features(score_map) as min_float_map, min_features(score_map_double) as min_double_map, 
        max_features(score) as max_float, max_features(score_double) as max_double, 
            max_features(score_list) as max_float_list, max_features(score_list_double) as max_double_list,
            max_features(score_map) as max_float_map, max_features(score_map_double) as max_double_map, 
        sum_features(score) as sum_float, sum_features(score_double) as sum_double, 
            sum_features(score_list) as sum_float_list, sum_features(score_list_double) as sum_double_list,
            sum_features(score_map) as sum_float_map, sum_features(score_map_double) as sum_double_map,
        avg_features(score) as avg_float, avg_features(score_double) as avg_double, 
            avg_features(score_list) as avg_float_list, avg_features(score_list_double) as avg_double_list,
            avg_features(score_map) as avg_float_map, avg_features(score_map_double) as avg_double_map,
        most_freq_features(uid_type) as mf_str, most_freq_features(cast(score as int)) as mf_int 
        from features group by dt
    """
    df = spark.sql(qry)
    df.printSchema()
    df.show(truncate=False)


def null_map_keys():
    # null key in map, impossible in spark
    # catalyst says: no
    # ndf = df.where("uid = 'o'").withColumn("fmap", sqlfn.expr("map(cast(score as string), cast(1 as float))"))
    # ndf.createOrReplaceTempView("null_map_keys")
    # ndf.show(truncate=False)

    for qry in [
        """select sum_features(
            map(cast(coalesce(score, 'NULL') as string), cast(1 as float))
        ) from features where uid = 'o' group by uid
        """,
        "select map(cast(score as string), cast(1 as float)) from features where uid = 'o'",  # java.lang.RuntimeException: Cannot use null as map key!
        "select sum_features(map(cast(score as string), cast(1 as float))) from features where uid = 'o' group by uid",  # java.lang.RuntimeException: Cannot use null as map key!
    ]:
        df = spark.sql(qry).cache()
        df.count()
        df.show(truncate=False)
        df.printSchema()
        df.unpersist()
    # 2021-04-22 10:59:48 - Executor - ERROR - Exception in task 0.0 in stage 11.0 (TID 6)
    # java.lang.RuntimeException: Cannot use null as map key!
    # 	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
    # 	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
    # 	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$13$$anon$1.hasNext(WholeStageCodegenExec.scala:636)
    # 	at org.apache.spark.sql.execution.columnar.CachedRDDBuilder$$anonfun$1$$anon$1.hasNext(InMemoryRelation.scala:125)
    # 	at org.apache.spark.storage.memory.MemoryStore.putIterator(MemoryStore.scala:221)
    # 2021-04-16 17:54:33 - Executor - ERROR - Exception in task 0.0 in stage 5.0 (TID 3)
    # java.lang.RuntimeException: Cannot use null as map key!
    # 	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.CreateMap_0$(Unknown Source)
    # 	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
    # 	at org.apache.spark.sql.hive.HiveUDAFFunction.update(hiveUDFs.scala:423)
    # 	at org.apache.spark.sql.hive.HiveUDAFFunction.update(hiveUDFs.scala:314)
    # 	at org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate.update(interfaces.scala:532)
    # https://github.com/apache/spark/search?q=%22Cannot+use+null+as+map+key%21%22

    # builtin functions won't work with null keys also
    spark.sql(
        """SELECT 
            element_at(
                map(cast(score as string), cast(1 as float)),
                "a"
            )
        from features where uid = 'o'
        """
    ).show(truncate=False)  # java.lang.RuntimeException: Cannot use null as map key!

    spark.sql(
        """SELECT
            map_keys(
                map(cast(score as string), cast(1 as float))
            )
        from features where uid = 'o'
        """
    ).show(truncate=False)  # java.lang.RuntimeException: Cannot use null as map key!

    spark.sql(
        """SELECT
            map_values(
                map(cast(score as string), cast(1 as float))
            )
        from features where uid = 'o'
        """
    ).show(truncate=False)  # java.lang.RuntimeException: Cannot use null as map key!

    spark.sql(
        """SELECT
            map_entries(
                map(cast(score as string), cast(1 as float))
            )
        from features where uid = 'o'
        """
    ).show(truncate=False)  # since 3.0.0


# catalyst "builtin" aggregate functions
def catalyst_udaf():
    spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.register(spark._jsparkSession, True)
    spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.registerAs("generic_sum", "gsum", spark._jsparkSession, True)
    for qry in [
        """select sum_features(score_map) from features where uid = 'o' group by uid""",
        """select generic_sum(score) from features where uid = 'o' group by uid""",
        """select gsum(score) from features where uid = 'o' group by uid""",
    ]:
        df = spark.sql(qry).cache()
        df.count()
        df.show(truncate=False)
        df.printSchema()
        df.unpersist()


def catalyst_udf():
    spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.register(spark._jsparkSession, True)
    spark._jvm.org.apache.spark.sql.catalyst.vasnake.udf.functions.registerAs("generic_isinf", "isinf", spark._jsparkSession, True)
    for qry in [
        """select score, isinf(score) from features where uid = 'b'""",
        """select score_double, generic_isinf(score_double) from features where uid = 'b'""",
        """select score, score_double, isinf(score * score_double) from features where uid = 'b'""",
    ]:
        df = spark.sql(qry).cache()
        df.count()
        df.show(truncate=False)
        df.printSchema()
        df.unpersist()

# @formatter:on

smoke_test()
# check_most_freq()
# check_min_max_sum_avg()
# all_agg()

# null_map_keys()
# catalyst_udaf()
# catalyst_udf()

spark.stop()
