import json

import numpy as np
import pytest
import scipy.sparse as ss
import pandas.testing as pdt

from ..postprocessing import (
    ScoreEqualizeTransformer,
    ArgMaxClassScoreTransformer,
    NEPriorClassProbaTransformer,
    ScoreQuantileThresholdTransformer,
)


class TestScoreEqualizeTransformer(object):
    def test_fit_transform(self, local_spark):
        df = self._load_df(local_spark)
        model = ScoreEqualizeTransformer(
            inputCol="score_raw_train",
            numBins=10000,
            epsValue=1e-3,
            noiseValue=1e-4,
            sampleSize=100,
            randomValue=0.5,
            groupColumns="",
            sampleRandomSeed=3.14,
        ).fit(df)
        output = model.setInputCol("score_raw").setOutputCol("score").transform(df).toPandas()
        assert output.shape == (5, 5)
        self._check_score(output)

    def test_groups_equalize(self, local_spark):
        df = self._load_df(local_spark, large=True).selectExpr(
            "uid",
            "uid_type",
            "category",
            "cast(score_raw_train as double) as score_raw_train",
            "cast(score_raw as double) as score_raw",
            "cast(expected as double) as expected",
        )
        eq = ScoreEqualizeTransformer(
            inputCol="score_raw_train", groupColumns="category, uid_type", randomValue=0.5
        ).fit(df)
        output = eq.setInputCol("score_raw").setOutputCol("score").transform(df).toPandas()
        assert output.shape == (32, 7)
        self._check_score(output)

    @staticmethod
    def _check_score(pdf):
        pdt.assert_frame_equal(
            pdf[["uid", "score"]],
            pdf[["uid", "expected"]].rename(columns={"expected": "score"}),
            check_less_precise=True,
            check_like=True,
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
        df = (
            local_spark.createDataFrame(
                [
                    ("a", 0, 1, 1.0001),
                    ("b", 1, 2, 0.1111),
                    ("c", 2, 2, 0.7778),
                    ("d", 3, 3, 0.2222),
                    ("e", 4, 3, 0.9629),
                    ("f", 5, 3, 0.2962),
                    ("g", 6, 4, 0.1666),
                    ("h", 7, 4, 0.4444),
                    ("i", 8, 4, 0.7222),
                    ("j", 9, 4, 1.0001),
                ]
            )
                .toDF("uid", "x", "expected_index", "expected_rank")
                .selectExpr(
                "uid",
                "cast(x as double) as x",
                "cast(expected_index as int) as expected_index",
                "cast(expected_rank as double) as expected_rank",
            )
        )
        estimator = ScoreQuantileThresholdTransformer(
            inputCol="x", priorValues=[1, 2, 3, 4], groupColumns="", sampleSize=1000, sampleRandomSeed=3.14
        )
        transformer = estimator.fit(df)
        output = (
            transformer.setInputCol("x")
                .setOutputCol("class_index")
                .setRankCol("rank")
                .setGroupColumns("")
                .transform(df)
        )
        assert output.schema["class_index"].__repr__() == "StructField(class_index,IntegerType,true)"
        assert output.schema["rank"].__repr__() == "StructField(rank,DoubleType,true)"

        pdf = output.toPandas()
        assert pdf.shape == (10, 6)
        self._check_score(pdf, df.toPandas())

    @staticmethod
    def _check_score(actual_pdf, expected_pdf):
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_index", "expected_rank"}]],
            expected_pdf.rename(columns={"expected_index": "class_index", "expected_rank": "rank"}),
            check_exact=False,
            check_less_precise=3,
            check_like=True,
        )


class TestArgMaxClassScoreTransformer(object):
    def test_fit_transform(self, local_spark):
        df = (
            local_spark.createDataFrame(
                [("a", [0, 1], "2", 0.5), ("b", [11, 2], "1", 0.0), ("c", [2, 3], "2", 0.0), ("d", [33, 4], "1", 0.5)]
            )
                .toDF("uid", "xs", "expected_label", "expected_rank")
                .selectExpr(
                "uid",
                "cast(xs as array<double>) as xs",
                "cast(expected_label as string) as expected_label",
                "cast(expected_rank as double) as expected_rank",
            )
        )
        estimator = ArgMaxClassScoreTransformer(
            inputCol="xs",
            numClasses=2,
            groupColumns=[],
            sampleSize=100,
            sampleRandomSeed=3.14,
            randomValue=0.5,
            labels=["1", "2"],
        )
        transformer = estimator.fit(df)
        output = (transformer.setInputCol("xs").setOutputCol("label").setRankCol("rank").setGroupColumns([])).transform(
            df
        )
        assert output.schema["label"].__repr__() == "StructField(label,StringType,true)"
        assert output.schema["rank"].__repr__() == "StructField(rank,DoubleType,true)"

        actual_pdf = output.toPandas()
        expected_pdf = df.toPandas().rename(columns={"expected_label": "label", "expected_rank": "rank"})
        assert actual_pdf.shape == (4, 6)
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_label", "expected_rank"}]],
            expected_pdf,
            check_exact=False,
            check_less_precise=3,
            check_like=True,
        )


class TestNEPriorClassProbaTransformer(object):
    def test_fit_transform(self, local_spark):
        df = (
            local_spark.createDataFrame(
                [
                    ("a", [0.0, 0.1], [0.0, 1.0]),
                    ("b", [11.0, 2.0], [0.567440, 0.432560]),
                    ("c", [2.0, 3.0], [0.137193, 0.862807]),
                    ("d", [33.0, 4.0], [0.663041, 0.336959]),
                ]
            )
                .toDF("uid", "probs", "expected")
                .selectExpr(
                "uid",
                "cast(probs as array<double>) as probs",
                "cast(expected as array<double>) as expected_probs_aligned",
            )
        )
        estimator = NEPriorClassProbaTransformer(
            inputCol="probs", numClasses=2, priorValues=[3, 5], groupColumns=[], sampleSize=100, sampleRandomSeed=3.14
        )
        transformer = estimator.fit(df)
        output = (transformer.setInputCol("probs").setOutputCol("probs_aligned").setGroupColumns([])).transform(df)
        assert output.schema["probs_aligned"].__repr__() == "StructField(probs_aligned,ArrayType(DoubleType,true),true)"

        actual_pdf = output.toPandas()
        expected_pdf = df.toPandas().rename(columns={"expected_probs_aligned": "probs_aligned"})
        assert actual_pdf.shape == (4, 4)
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_probs_aligned"}]],
            expected_pdf,
            check_exact=False,
            check_less_precise=5,
            check_like=True,
        )

    # TODO: remove tests below
    def test_reference_simple(self):
        from dmcore.transformers.postprocessing import NEPriorClassProbaTransformer

        for X, prior, expected in [
            (
                    np.array([[1.1, 2.2]]),  # smoke
                    [3, 7],
                    np.array([[0.5, 0.5]])
            ),
            (
                    np.array([
                        [1.1, 2.2],
                        [44.4, 33.3]
                    ]),
                    [5, 6],
                    np.array([
                        [0.272727, 0.727273],
                        [0.5, 0.5]
                    ])),
            (
                    np.array([
                        [1.1, 2.2, 3.3],
                        [33.3, 44.4, 43.21],
                        [111.1, 222.2, 333.3]
                    ]),
                    [3, 7, 11],
                    np.array([
                        [0.25000, 0.375, 0.375],
                        [0.37753, 0.37753, 0.244941],
                        [0.25, 0.375, 0.375]
                    ]),
            ),
            (
                    np.array([
                        [0.5, 0.5],
                        [0.5, 0.5],
                        [0.5, 0.5]
                    ]),
                    [1.0, 1.0],
                    np.array([
                        [0.524083, 0.475917],
                        [0.524083, 0.475917],
                        [0.524083, 0.475917]
                    ]),
            ),
            (
                    np.array([
                        [0.51, 0.49],
                        [0.51, 0.49],
                        [0.51, 0.49]
                    ]),
                    [11.0, 11.0],
                    np.array([
                        [0.53405, 0.46595],
                        [0.53405, 0.46595],
                        [0.53405, 0.46595]
                    ]),
            ),
            (
                    np.array([
                        [0.51, 0.52],
                        [0.51, 0.52],
                        [0.51, 0.52]
                    ]),
                    [11.0, 11.0],
                    np.array([
                        [0.471077, 0.528923],
                        [0.471077, 0.528923],
                        [0.471077, 0.528923]
                    ]),
            ),
            (
                    np.array([
                        [0.51, 0.52],
                        [0.52, 0.51],
                        [0.51, 0.52]]),
                    [5, 3],
                    np.array([
                        [0.5, 0.5],
                        [0.509708, 0.490292],
                        [0.5, 0.5]
                    ]),
            ),
            (
                    np.array([
                        [0, 1],
                        [11, 2],
                        [2, 3],
                        [33, 4]
                    ], dtype=np.float_),
                    [3, 5],
                    np.array(
                        [[0, 1],
                         [0.567440, 0.432560],
                         [0.137193, 0.862807],
                         [0.663041, 0.336959]
                         ], dtype=np.float_),
            ),
        ]:
            estimator = NEPriorClassProbaTransformer(prior=prior)
            model = estimator.fit(X)
            output = model.transform(X)
            print("\nactual:\n{}\nexpected:\n{}".format(pd.DataFrame(output), pd.DataFrame(expected)))
            np.testing.assert_allclose(output, expected, atol=0.001)

    def test_NEPriorClassProbaTransformer__fit_transform__correct(self):
        from dmcore.transformers.postprocessing import NEPriorClassProbaTransformer

        p = [3, 1, 9, 6, 1]
        probs = np.array(p, dtype=float) / sum(p)
        tr = NEPriorClassProbaTransformer(prior=p)

        brick = np.dot(np.arange(1001)[1:, np.newaxis], np.arange(6)[np.newaxis, 1:])
        x = brick ** 1 - brick ** 0.5

        x_tr = tr.fit_transform(x)
        pred = np.argmax(x_tr, axis=1)
        uu, cc = np.unique(pred, return_counts=True)
        tprobs = np.array(cc, dtype=float) / sum(cc)
        np.testing.assert_allclose(tprobs, probs, atol=1e-4)

        x = np.random.rand(1000, 5)
        tr = tr.fit(x.copy())
        x_tr = tr.transform(x.copy())

        with open("/tmp/DM-8183.refdata.json", "wb") as f:
            f.write(
                json.dumps(
                    {
                        "prior": list(tr.prior),
                        "xs": [row.tolist() for row in x],
                        "probs": [row.tolist() for row in x_tr],
                        "aligner": tr._aligner.tolist(),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )

        pred = np.argmax(x_tr, axis=1)
        uu, cc = np.unique(pred, return_counts=True)
        tprobs = np.array(cc, dtype=float) / sum(cc)
        np.testing.assert_allclose(tprobs, probs, atol=0.15)

    def test_NEPriorClassProbaTransformer__invalid_input__raises(self):
        from dmcore.transformers.postprocessing import NEPriorClassProbaTransformer

        x_not_matrix = np.ones((10, 4, 4))
        x_neg_value = np.array([[1, 2, 3, 4], [6, -2, 4, 7]])
        x_allnan = np.zeros((2, 3))
        one_prob = [100]
        self.probs = np.array(
            [[0.0, 0.001, 0.999], [0.33, 0.32, 0.35], [0.8, 0.05, 0.15], [0.13, 0.8, 0.07], [np.nan, np.nan, np.nan]]
        )
        tr = NEPriorClassProbaTransformer(prior=[1, 2, 3]).fit(self.probs)

        with pytest.raises(TypeError) as e:
            tr.fit_transform(ss.csr_matrix([[1, 2, 3]]))
        assert "A sparse matrix was passed, but dense data is required" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.fit_transform(x_not_matrix)
        assert "Found array with dim 3. Estimator expected <= 2" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.fit_transform(x_neg_value)
        assert "'X' must contain only non-negative values" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.fit_transform(x_allnan)
        assert "No valid samples found to learn aligner vector" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.fit_transform(np.array([[0.0, 0.0, 0.0], [0.33, np.nan, 0.35]]))
        assert "No valid samples found to learn aligner vector" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.set_params(prior=[1.1, 2.2, 0.0])
        assert "'prior' must contain only positive values" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.set_params(prior=one_prob)
        assert "Found array with 1 sample(s) (shape=(1,)) while a minimum of 2 is required" in str(e.value)

        with pytest.raises(ValueError) as e:
            tr.transform(np.abs(x_neg_value))
        assert "'X' must contain amount of columns as of 'prior' shape" in str(e.value)
