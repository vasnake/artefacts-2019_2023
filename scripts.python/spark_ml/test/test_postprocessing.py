import pytest
import pandas.testing as pdt

from pyspark.sql.utils import IllegalArgumentException

from ..postprocessing import (
    ScoreEqualizeTransformer,
    NEPriorClassProbaTransformer,
    ScoreQuantileThresholdTransformer,
)


class TestScoreEqualizeTransformer(object):
    def test_fit_transform(self, local_spark):
        df = self._load_df(local_spark).persist()

        model = ScoreEqualizeTransformer(
            inputCol="score_raw_train",
            numBins=10000,
            epsValue=1e-3,
            noiseValue=1e-4,
            sampleSize=100,
            randomValue=0.5,
            groupColumns=[],
            sampleRandomSeed=3.14,
        ).fit(df)

        out_pdf = model.setInputCol("score_raw").setOutputCol("score").transform(df).toPandas()
        assert out_pdf.shape == (5, 5)
        self._check_score(out_pdf)

    def test_groups_equalize(self, local_spark):
        df = self._load_df(local_spark, large=True).persist()

        eq = ScoreEqualizeTransformer(
            inputCol="score_raw_train", groupColumns=["category", "uid_type"], randomValue=0.5
        ).fit(df)

        out_pdf = eq.setInputCol("score_raw").setOutputCol("score").transform(df).toPandas()
        assert out_pdf.shape == (32, 7)
        self._check_score(out_pdf)

    def test_default_output_column(self, local_spark):
        df = self._load_df(local_spark).persist()
        model = ScoreEqualizeTransformer(inputCol="score_raw").fit(df)

        with pytest.raises(IllegalArgumentException) as ex:
            model.transform(df).show()

        assert "requirement failed: Output column name can't be empty" in str(ex.value)

    @staticmethod
    def _check_score(pdf):
        pdt.assert_frame_equal(
            pdf[["uid", "score"]],
            pdf[["uid", "expected"]].rename(columns={"expected": "score"}),
            check_like=True,
            check_less_precise=True,
        )

    @staticmethod
    def _load_df(spark, large=False):
        if large:
            return spark.createDataFrame(
                [
                    ("a", "OK", "foo", 0.3, 0.1, 0.0),
                    ("b", "OK", "foo", 0.7, 3.14, 0.27968233),
                    ("c", "OK", "foo", 13.0, 26.0, 0.74796144),
                    ("d", "OK", "foo", 17.0, 28.0, 1.0),
                    ("e", "OK", "foo", 27.0, 15.0, 0.4982242),
                    ("uid_0", "VK", "bar", 99.0, 0.1, 0.0),
                    ("uid_1", "VK", "bar", 55.0, 3.14, 0.223051),
                    ("uid_2", "VK", "bar", 1.0, 26.0, 0.292044),
                    ("uid_3", "VK", "bar", 2.0, 28.0, 0.293951),
                    ("uid_4", "VK", "bar", 3.0, 15.0, 0.272982),
                    ("uid_5", "VK", "bar", 73.0, 33.0, 0.297835),
                    ("uid_6", "VK", "bar", 95.0, 90.0, 0.544477),
                    ("uid_7", "VK", "bar", 91.0, 80.0, 0.478477),
                    ("uid_8", "VK", "bar", 97.0, 7.0, 0.243779),
                    ("uid_0", "OK", "bar", 19.0, 0.1, 0.0),
                    ("uid_1", "OK", "bar", 25.0, 3.14, 0.0),
                    ("uid_2", "OK", "bar", 31.0, 26.0, 0.130266),
                    ("uid_3", "OK", "bar", 42.0, 28.0, 0.170497),
                    ("uid_4", "OK", "bar", 53.0, 15.0, 0.0),
                    ("uid_5", "OK", "bar", 63.0, 33.0, 0.246967),
                    ("uid_6", "OK", "bar", 75.0, 90.0, 0.816186),
                    ("uid_7", "OK", "bar", 81.0, 80.0, 0.763845),
                    ("uid_8", "OK", "bar", 97.0, 7.0, 0.0),
                    ("uid_0", "VK", "foo", 91.0, 0.1, 0.0),
                    ("uid_1", "VK", "foo", 52.0, 3.14, 0.0),
                    ("uid_2", "VK", "foo", 13.0, 26.0, 0.131936),
                    ("uid_3", "VK", "foo", 24.0, 28.0, 0.153365),
                    ("uid_4", "VK", "foo", 35.0, 15.0, 0.006922),
                    ("uid_5", "VK", "foo", 76.0, 33.0, 0.204792),
                    ("uid_6", "VK", "foo", 97.0, 90.0, 0.544612),
                    ("uid_7", "VK", "foo", 98.0, 80.0, 0.467587),
                    ("uid_8", "VK", "foo", 99.0, 7.0, 0.0),
                ]
            ).toDF("uid", "uid_type", "category", "score_raw_train", "score_raw", "expected")
        else:
            return spark.createDataFrame(
                [
                    ("a", 0.3, 0.1, 0.0),
                    ("b", 0.7, 3.14, 0.27968233),
                    ("c", 13.0, 26.0, 0.74796144),
                    ("d", 17.0, 28.0, 1.0),
                    ("e", 27.0, 15.0, 0.4982242),
                ]
            ).toDF("uid", "score_raw_train", "score_raw", "expected")


class TestScoreQuantileThresholdTransformer(object):
    def test_fit_transform(self, local_spark):
        df = self._load_df(local_spark).persist()

        model = ScoreQuantileThresholdTransformer(
            inputCol="x",
            priorValues=[1, 2, 3, 4],
            labels=["foo", "bar", "baz", "qux"],
            groupColumns=[],
            sampleSize=1000,
            sampleRandomSeed=3.14,
        ).fit(df)

        out_df = model.setOutputCol("category").setRankCol("score").transform(df)
        assert out_df.schema["category"].__repr__() == "StructField(category,StringType,true)"
        assert out_df.schema["score"].__repr__() == "StructField(score,DoubleType,true)"

        pdf = out_df.toPandas()
        assert pdf.shape == (10, 6)
        self._check_score(pdf, df.toPandas())

    def test_default_output_column(self, local_spark):
        df = self._load_df(local_spark).persist()
        model = ScoreQuantileThresholdTransformer(inputCol="x", priorValues=[1, 2, 3, 4]).fit(df)

        with pytest.raises(IllegalArgumentException) as ex:
            model.transform(df).show()

        assert "requirement failed: Output column name can't be empty" in str(ex.value)

    @staticmethod
    def _check_score(actual_pdf, expected_pdf):
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_category", "expected_score"}]],
            expected_pdf.rename(columns={"expected_category": "category", "expected_score": "score"}),
            check_like=True,
            check_exact=False,
            check_less_precise=3,
        )

    @staticmethod
    def _load_df(spark):
        return spark.createDataFrame(
            [
                ("a", 0.0, "foo", 1.0001),
                ("b", 1.0, "bar", 0.1111),
                ("c", 2.0, "bar", 0.7778),
                ("d", 3.0, "baz", 0.2222),
                ("e", 4.0, "baz", 0.9629),
                ("f", 5.0, "baz", 0.2962),
                ("g", 6.0, "qux", 0.1666),
                ("h", 7.0, "qux", 0.4444),
                ("i", 8.0, "qux", 0.7222),
                ("j", 9.0, "qux", 1.0001),
            ]
        ).toDF("uid", "x", "expected_category", "expected_score")


class TestNEPriorClassProbaTransformer(object):
    def test_fit_transform(self, local_spark):
        df = self._load_df(local_spark).persist()

        model = NEPriorClassProbaTransformer(
            inputCol="scores_raw", priorValues=[3, 5], groupColumns=[], sampleSize=100, sampleRandomSeed=3.14
        ).fit(df)

        out_df = model.setOutputCol("scores_trf").transform(df)
        assert out_df.schema["scores_trf"].__repr__() == "StructField(scores_trf,ArrayType(DoubleType,true),true)"

        actual_pdf = out_df.toPandas()
        expected_pdf = df.toPandas().rename(columns={"expected_scores_trf": "scores_trf"})
        assert actual_pdf.shape == (4, 4)
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_scores_trf"}]],
            expected_pdf,
            check_like=True,
            check_exact=False,
            check_less_precise=5,
        )

    def test_default_output_column(self, local_spark):
        df = self._load_df(local_spark).persist()
        model = NEPriorClassProbaTransformer(inputCol="scores_raw", priorValues=[1, 1]).fit(df)

        with pytest.raises(IllegalArgumentException) as ex:
            model.transform(df).show()

        assert "requirement failed: Output column name can't be empty" in str(ex.value)

    @staticmethod
    def _load_df(spark):
        return spark.createDataFrame(
            [
                ("a", [0.0, 0.1], [0.0, 1.0]),
                ("b", [11.0, 2.0], [0.567440, 0.432560]),
                ("c", [2.0, 3.0], [0.137193, 0.862807]),
                ("d", [33.0, 4.0], [0.663041, 0.336959]),
            ]
        ).toDF("uid", "scores_raw", "expected_scores_trf")
