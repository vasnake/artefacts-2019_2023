import json

import six
import numpy as np
import pandas as pd
import pandas.testing as pdt

from ..postprocessing import ScoreEqualizeTransformer, ArgMaxClassScoreTransformer, ScoreQuantileThresholdTransformer


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
            sampleRandomSeed=3.14
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
                [("a", [0, 1], "2", 1.0), ("b", [11, 2], "1", 1.0), ("c", [2, 3], "2", 1.0), ("d", [33, 4], "1", 1.0)]
            )
                .toDF("uid", "xs", "expected_label", "expected_rank")
                .selectExpr(
                "uid",
                "cast(xs as array<double>) as xs",
                "cast(expected_label as string) as expected_label",
                "cast(expected_rank as double) as expected_rank",
            )
        )
        df.printSchema()
        df.show(truncate=False)

        estimator = ArgMaxClassScoreTransformer(
            inputCol="x",
            numClasses=3,
            groupColumns=["uid_type", "category"],
            sampleSize=100000,
            sampleRandomSeed=float("nan"),
            randomValue=float("nan"),
            labels=["foo", "bar"],
        )
        transformer = (
            estimator.setInputCol("xs")
                .setNumClasses(2)
                .setGroupColumns([])
                .setSampleSize(10)
                .setSampleRandomSeed(2021.03)
                .setRandomValue(0.5)
                .setLabels(["1", "2"])
        ).fit(df)

        res = (
            (transformer.setInputCol("xs").setOutputCol("label").setRankCol("rank").setGroupColumns([]))
                .transform(df)
                .cache()
        )
        res.printSchema()
        res.show(truncate=False)

        assert res.schema["label"].__repr__() == "StructField(label,StringType,true)"
        assert res.schema["rank"].__repr__() == "StructField(rank,DoubleType,true)"

        actual_pdf = res.toPandas()
        expected_pdf = df.toPandas().rename(columns={"expected_label": "label", "expected_rank": "rank"})
        assert actual_pdf.shape == (4, 6)
        pdt.assert_frame_equal(
            actual_pdf[[x for x in actual_pdf.columns.values if x not in {"expected_label", "expected_rank"}]],
            expected_pdf,
            check_exact=False,
            check_less_precise=3,
            check_like=True,
        )

    def test_reference_simple(self):
        # TODO: delete this test before release
        from dmcore.transformers.postprocessing import ArgMaxClassScoreTransformer

        nan = np.nan
        for train, xs, expected in [
            (
                    np.array(
                        [
                            [1.1, 2.2],
                            [33.3, 22.2],
                            [50.0, 50.0],
                        ]
                    ),
                    np.array(
                        [
                            [1.1, 2.2],
                            [33.3, 22.2],
                            [50.0, 50.0],
                        ]
                    ),
                    np.array(
                        [
                            [2.0, 0.5],
                            [1.0, 0.2],
                            [2.0, 0.0],
                        ]
                    ),
            ),
            (
                    np.array([[1.1, 1.2], [2.1, 2.2], [11.2, 11.1], [22.2, 22.1]]),
                    np.array(
                        [
                            [1, 2],
                            [22, 11],
                            [3, np.nan],
                            [np.nan, np.nan],
                        ]
                    ),
                    np.array([[2.0, 1.0], [1.0, 1.0], [nan, nan], [nan, nan]]),
            ),
            (
                    np.array(
                        [
                            [0.0, 0.001, 0.999],
                            [0.33, 0.32, 0.35],
                            [0.8, 0.05, 0.15],
                            [0.13, 0.8, 0.07],
                            [nan, nan, nan],
                            [-1.0, 12, -11],
                            [1, 0.4, nan],
                            [0.1, 0.01, 0.001],
                            [-1, -5, -0.3],
                            [0, 0, 0],
                        ]
                    ),
                    np.array(
                        [
                            [0.0, 0.001, 0.999],
                            [0.33, 0.32, 0.35],
                            [0.8, 0.05, 0.15],
                            [0.13, 0.8, 0.07],
                            [nan, nan, nan],
                            [-1.0, 12, -11],
                            [1, 0.4, nan],
                            [0.1, 0.01, 0.001],
                            [-1, -5, -0.3],
                            [0, 0, 0],
                        ]
                    ),
                    np.array(
                        [
                            [3.0, 0.666667],
                            [3.0, 0.000000],
                            [1.0, 0.000000],
                            [2.0, 0.500000],
                            [nan, nan],
                            [2.0, 0.000000],
                            [nan, nan],
                            [1.0, 0.500000],
                            [3.0, 0.333333],
                            [nan, nan],
                        ]
                    ),
            ),
            (
                    np.array([[-1, 12, -11], [1, 3, 2], [100, 10, 1]]),
                    np.array([[-1, 12, -11], [1, 3, 2], [100, 10, 1]]),
                    np.array([[2.0, 0.5], [2.0, 0.0], [1.0, 0.810811]]),
            ),
        ]:
            model = ArgMaxClassScoreTransformer().fit(train)
            # model = estimator.fit(xs, ["a", "b"])  # custom class labels
            # print("config:\n{}".format(json.dumps(self._get_config(model), indent=4, sort_keys=True)))
            res = model.transform(xs)
            print("\ngot:\n{}\nexpected:\n{}".format(pd.DataFrame(res), pd.DataFrame(expected)))
            np.testing.assert_allclose(res, expected, atol=0.0001)

    def test_reference_labels(self):
        # TODO: delete this test before release
        from dmcore.transformers.postprocessing import ArgMaxClassScoreTransformer

        nan = np.nan
        NaN = float("nan")

        train = np.array([[1.1, 1.2], [2.1, 2.2], [11.2, 11.1], [22.2, 22.1]])
        xs = np.array(
            [
                [1, 2],
                [22, 11],
                [3, nan],
                [nan, nan],
            ]
        )
        expected = np.array([[u"foo", 1.0], [u"bar", 1.0], [NaN, NaN], [NaN, NaN]], dtype=np.object_)

        model = ArgMaxClassScoreTransformer().fit(train, ["foo", "bar"])  # assigned sorted labels
        # print("config:\n{}".format(json.dumps(self._get_config(model), indent=4, sort_keys=True)))
        res = model.transform(xs)
        print("\ngot:\n{}\nexpected:\n{}".format(pd.DataFrame(res), pd.DataFrame(expected)))
        pdt.assert_frame_equal(
            pd.DataFrame(res),
            pd.DataFrame(expected),
            check_exact=False,
            check_less_precise=3,
            check_like=True,
        )

    def test_reference_disabled_eq(self):
        # TODO: delete this test before release
        from dmcore.transformers.postprocessing import ArgMaxClassScoreTransformer

        nan = np.nan

        for xs, expected in [
            (
                    np.array([[1.1, 2.2], [33.3, 22.2], [50, 50]]),
                    np.array(
                        [
                            [2.0, 0.33333],
                            [1.0, 0.2],
                            [2.0, 0.0],
                        ]
                    ),
            ),
            (
                    np.array([[-1.1, 2.2], [1.1, -2.2], [33.3, -22.2], [50, -50], [-50, -50], [-50, 50]]),
                    np.array(
                        [
                            [2.0, 1.0],
                            [1.0, 1.0],
                            [1.0, 1.0],
                            [1.0, 1.0],
                            [2.0, 0.0],
                            [2.0, 1.0],
                        ]
                    ),
            ),
            (
                    np.array(
                        [
                            [0.0, 0.0],
                            [-0.0, 0.0],
                            [0.0, -0.0],
                            [-0.0, -0.0],
                        ]
                    ),
                    np.array(
                        [
                            [nan, nan],
                            [nan, nan],
                            [nan, nan],
                            [nan, nan],
                        ]
                    ),
            ),
            (
                    np.array(
                        [
                            [0.1, 0.2],
                            [nan, 0.2],
                            [0.1, nan],
                            [nan, nan],
                        ]
                    ),
                    np.array(
                        [
                            [2.0, 0.33333],
                            [nan, nan],
                            [nan, nan],
                            [nan, nan],
                        ]
                    ),
            ),
            (
                    np.array([[1.1, 2.2, 3.3], [33.3, 22.2, 11.1], [50, 50.1, 50]]),
                    np.array(
                        [
                            [3.0, 0.1666667],
                            [1.0, 0.166667],
                            [2.0, 0.000666],
                        ]
                    ),
            ),
            (
                    np.array([[1.1, 2.2, 3.3, 4.4], [44.4, 33.3, 22.2, 11.1], [50, 50.1, 50, 50], [50, 50.1, 50.2, 50]]),
                    np.array(
                        [
                            [4.0, 0.1],
                            [1.0, 0.1],
                            [2.0, 0.0005],
                            [3.0, 0.000499],
                        ]
                    ),
            ),
            (
                    np.array([[0.0, 0.001, 0.999], [0.13, 0.8, 0.07], [0.1, 0.01, 0.001]]),
                    np.array(
                        [
                            [3.0, 0.998],
                            [2.0, 0.67],
                            [1.0, 0.8108],
                        ]
                    ),
            ),
        ]:
            model = ArgMaxClassScoreTransformer().fit(xs)
            model._equalizers = {}
            res = model.transform(xs)
            print("\ngot:\n{}\nexpected:\n{}".format(pd.DataFrame(res), pd.DataFrame(expected)))
            np.testing.assert_allclose(res, expected, atol=0.0001)

    def test_reference_assign(self):
        # TODO: delete this test before release
        from dmcore.transformers.postprocessing import ArgMaxClassScoreTransformer

        tr = ArgMaxClassScoreTransformer()
        probs = np.array(
            [[0.0, 0.001, 0.999], [0.33, 0.32, 0.35], [0.8, 0.05, 0.15], [0.13, 0.8, 0.07], [np.nan, np.nan, np.nan]]
        )
        neg_scores = np.array([[-1.0, 12, -11], [1, 0.4, np.nan], [0.1, 0.01, 0.001], [-1, -5, -0.3], [0, 0, 0]])
        pos_labels = np.array([[3.0], [3.0], [1.0], [2.0], [np.nan]])
        neg_labels = np.array([[2.0], [np.nan], [1.0], [3.0], [np.nan]])

        assert tr.fit_transform(probs).shape[1] == 2
        np.testing.assert_equal(tr.fit_transform(probs)[:, 0].flatten(), pos_labels.flatten())
        np.testing.assert_equal(tr.fit_transform(neg_scores)[:, 0].flatten(), neg_labels.flatten())

    def test_reference_align(self):
        # TODO: delete this test before release
        from dmcore.transformers.postprocessing import ArgMaxClassScoreTransformer

        tr = ArgMaxClassScoreTransformer()

        x_matr = np.random.rand(1000, 3)
        x_matr[:, 1] *= 2
        model = tr.fit(x_matr.copy())
        # model._equalizers = {}  # disable equalizers
        rez = model.transform(x_matr.copy())

        with open("/tmp/DM-8182.refdata.json", "wb") as f:
            f.write(
                json.dumps(
                    {
                        "xs": [row.tolist() for row in x_matr],
                        "label": rez[:, 0].flatten().tolist(),
                        "rank": rez[:, 1].flatten().tolist(),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )

        np.testing.assert_allclose([np.nanmin(rez[:, 1]), np.nanmax(rez[:, 1])], [0.0, 1.0], atol=1e-3)

        for c in np.unique(rez[:, 0]):
            scores = rez[rez[:, 0] == c][:, 1]
            for q in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
                np.testing.assert_allclose(q, np.quantile(scores, q=q), atol=0.01)

    @classmethod
    def _get_config(cls, model):
        return {
            "n_classes": model._n_classes,  # number of x columns
            "equalizers": {k: cls._eq_repr(v) for k, v in six.iteritems(model._equalizers)},
            "classes": list(model._classes),  # sorted unique labels or numbers [1, n_classes]
        }

    @staticmethod
    def _eq_repr(equalizer):
        from dmgrinder.interface.models.utils.sa_repr import ScoreEqualizeTransformerRepr

        return ScoreEqualizeTransformerRepr(equalizer).as_dict()
