# TODO: remove after debug

__all__ = ["show_df"]


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
