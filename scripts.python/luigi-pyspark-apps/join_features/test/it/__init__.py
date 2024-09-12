# -*- mode: txt; coding: utf-8 -*-

"""Test cases, build ARRAY, MAP domains:

- can't find required fields
- null in not-null fields
- empty collections
- only null in collections
- mix of empty and not-empty collections
- arrays of different size
All that cases for:
- one input field
- few input fields
All that cases for:
- only primitive values
- only collections
- primitive and collections
All that cases for:
- different data types, with null/nan for float/double in collections
"""

def show_df(df, msg="Spark DataFrame", lines=100, truncate=False):
    """Print df as table, with metadata.

    :param df: pyspark.sql.DataFrame
    :type df: :class:`pyspark.sql.DataFrame`
    :param msg: title
    :param lines: lines to show
    :param truncate: truncate wide lines if True
    :return: pyspark.sql.DataFrame
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    from pyspark.storagelevel import StorageLevel

    # df.explain(extended=True)

    _df = df.persist(StorageLevel.MEMORY_ONLY)
    count = _df.count()
    num_parts = _df.rdd.getNumPartitions()
    print("\n# {}\nrows: {}, partitions: {}".format(msg, count, num_parts))

    _df.printSchema()

    _df.show(n=lines, truncate=truncate)

    return _df.unpersist()
