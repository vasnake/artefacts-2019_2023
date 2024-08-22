import six
import copy
import sklearn.base as sb

from collections import OrderedDict

from dmcore.base import (
    BasePredictor, BaseCompositePredictor, requires_fit_on_fit,
    requires_fit_on_transform, requires_fit_on_fit_transform
)

from dmcore.utils.common import make_list, get_prefixed_subparams


class MLPipeline(BaseCompositePredictor):
    """
    Extended and modified 'sklearn' pipeline.
    Main improvements:

    #. Added unsupervised post-processing steps.
    #. Correct support of validation set parameter for early stopping.
    #. Support for pre-trained and always-fitted pipeline stages.

    Overall pipeline looks like this (any steps could be skipped):
    |     (unsupervised)       |  (supervised)  |        (unsupervised)     |
    [pre-processor_1 --> ...] -->  [predictor] --> [post-processor_1 --> ...]

    Features validation set is pre-processed (unlike standard 'sklearn' pipeline) with the same transformers chain
    as for main learning features, which ensures correct early stopping.

    One can pre-train some estimator (eg., feature extractor), then construct pipeline with it
    and fit the whole pipeline, making actual fit for only remaining unfitted pipeline steps.
    To enable using pre-trained step set 'use_prefitted=True' within that step.

    Also it is possible to mark any unsupervised step as requiring always fitting, which is
    equivalent to replacing 'transform' with 'fit_transform' for that step.
    To enable always-fitted step set 'always_fit=True' within that step.

    .. note:: Any estimator with basic 'sklearn' API can be easily extended to have flags 'use_prefitted' and
       (only for unsupervised) 'always_fit' using 'dmcore' wrappers.

       - 'SklearnPredictorWrapper' or 'SklearnNumericPredictorWrapper' (for supervised).
       - 'SklearnTransformerWrapper' (for unsupervised).

    Parameters
    ----------

    * ``preprocessors``: None or a list of tuples (name, transformer). These transformers are chained
      in the given order. Each transformer must implement basic 'sklearn' interface for transformers.

    * ``predictor``: None or a tuple (name, estimator). Single (if any) supervised ML unit.
      Must implement basic 'sklearn' interface and also 'fit' and 'predict' methods.

    * ``postprocessors``: None or a list of tuples (name, transformer). These transformers are chained
      in the given order. Each transformer must implement basic 'sklearn' interface for transformers.
    """

    __doc__ += BasePredictor.__doc__

    COMPOSITION_ATTRIBUTES = [
        {
            "name": "preprocessors",
            "bases": [sb.BaseEstimator, sb.TransformerMixin],
            "methods": ["fit", "transform"],
            "nullable": True, "allow_none_step": True
        },
        {
            "name": "predictor",
            "bases": [sb.BaseEstimator],
            "methods": ["fit", "predict"],
            "max_steps": 1, "nullable": True, "allow_none_step": True
        },
        {
            "name": "postprocessors",
            "bases": [sb.BaseEstimator, sb.TransformerMixin],
            "methods": ["fit", "transform"],
            "nullable": True, "allow_none_step": True
        }
    ]

    def __init__(self, preprocessors=None, predictor=None, postprocessors=None, use_prefitted=False):
        self.preprocessors = preprocessors
        self.predictor = predictor
        self.postprocessors = postprocessors
        super(MLPipeline, self).__init__(use_prefitted)

    def fit(self, X, y, **params):
        """
        Fit the whole pipeline, avoiding unnecessary transforms/predicts, when next steps may not be fitted.

        Parameters
        ----------

        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Training data, must fulfill input requirements of the first step of the pipeline.

        y - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape
            with natural constraint X.shape[0] == y.shape[0].
            Target values, must fulfill label requirements for all steps of the pipeline.

        params - dict of string -> object
            Parameters passed to the 'fit' / 'transform' / 'predict' methods of each step (if any), where
            each parameter name is prefixed such that parameter 'p' for step 's' for method [fit | predict | transform]
            has key 's__[fit | pred | trf]__p'.

        Return
        ------
        self : MLPipeline, This estimator.

        NOTE: Supports the same transforms chain for validation set as for 'X', used for early stopping.
              Corresponding parameter name is assumed to be (appropriately prefixed) 'eval_set', which is a list
              of tuples (X, y), as it is supported in the most popular packages like XGBoost, LightGBM and CatBoost.
        """
        rest_params = self._fit_pipeline(test_params_only=True, X=X, y=y, **params)
        self._raise_on_extra_params(**rest_params)
        self._fit_pipeline(test_params_only=False, X=X, y=y, **params)
        return self

    def predict(self, X, apply_postprocessors=True, **params):
        """
        Make predictions with the whole pipeline.

        Parameters
        ----------
        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Training data, must fulfill input requirements of the first step of the pipeline.

        apply_postprocessors - bool, default=True
            Whether to apply configured postprocessors or not.

        params - dict of string -> object
            Parameters passed to the 'fit' / 'transform' / 'predict' methods of each step (if any), where
            each parameter name is prefixed such that parameter 'p' for step
            's' for method [fit | predict | transform] has key 's__[fit | pred | trf]__p'.

        Return
        ------
        array-like, predictions
        """
        self.check_fitted()
        rest_params = self._apply_pipeline(
            test_params_only=True, X=X, apply_postprocessors=apply_postprocessors, **params
        )
        self._raise_on_extra_params(**rest_params)
        return self._apply_pipeline(test_params_only=False, X=X, apply_postprocessors=apply_postprocessors, **params)

    def yield_from_steps(self, X, *step_names, **params):
        """
        Return generator object with items (step_name, Xt), where step_name is each of the requested step_names and
        Xt is X transformed by pipeline steps up until step_name, including step_name.
        Used for retrieving intermediate results of the pipeline.

        Parameters
        ----------
        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Training data, must fulfill input requirements of the first step of the pipeline.

        step_names - list of strings, selected pipeline steps.

        params - dict of string -> object
            Parameters passed to the 'fit' / 'transform' / 'predict' methods of each step (if any), where
            each parameter name is prefixed such that parameter 'p' for step
            's' for method [fit | predict | transform] has key 's__[fit | pred | trf]__p'.

        Return
        ------
        generator of tuples (step_name, Xt)
        """
        self.check_fitted()
        step_names = set(step_names)

        if len(step_names) == 0:
            self._raise_on_extra_params(**params)
            return

        all_step_names = set(
            s for attr in self.COMPOSITION_ATTRIBUTES for s, _ in make_list(getattr(self, attr["name"]))
        )

        if not step_names.issubset(all_step_names):
            raise ValueError("There is no such pipeline steps: {}".format(step_names.difference(all_step_names)))

        Xt = X
        rest_params = params

        all_steps = OrderedDict([
            (attr["name"], make_list(copy.deepcopy(getattr(self, attr["name"]))))
            for attr in self.COMPOSITION_ATTRIBUTES
        ])

        for attr_name, attr_steps in six.iteritems(all_steps):
            for i, (name, step) in enumerate(attr_steps):
                if step is not None:
                    if attr_name == "predictor":
                        predict_params, rest_params = self._get_step_subparams(
                            step_name=name,
                            method_prefix="pred",
                            **rest_params
                        )
                        Xt = step.predict(Xt, **predict_params)
                    else:
                        if requires_fit_on_transform(step):
                            fit_params, rest_params = self._get_step_subparams(
                                step_name=name,
                                method_prefix="fit",
                                **rest_params
                            )
                            step = step.fit(Xt, y=None, **fit_params)
                            attr_steps[i] = (name, step)

                        trf_params, rest_params = self._get_step_subparams(
                            step_name=name,
                            method_prefix="trf",
                            **rest_params
                        )
                        Xt = step.transform(Xt, **trf_params)

                if name in step_names:
                    yield name, Xt
                    step_names.remove(name)

                    if len(step_names) == 0:
                        self._raise_on_extra_params(**rest_params)
                        for _attr_name, _attr_steps in six.iteritems(all_steps):
                            setattr(self, _attr_name, _attr_steps)
                        return

    def apply_preprocessors(self, X, **params):
        """
        Apply only a chain of pipeline pre-processing steps.

        Parameters
        ----------
        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Must fulfill input requirements of the first step of the pipeline 'preprocessors'.

        params - dict of string -> object
            Parameters passed to the 'fit' and 'transform' methods of each step in 'preprocessors', where
            each parameter name is prefixed such that parameter 'p' for step 's' for method [fit | transform]
            has key 's__[fit | trf]__p'.

        Return
        ------
        array-like, pre-processed data
        """
        return self._apply_pre_or_post_only("preprocessors", X, **params)

    def apply_predictor(self, X, **params):
        """
        Apply only pipeline's inner 'predictor'. If it's not set, return unchanged input.

        Parameters
        ----------
        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Must fulfill input requirements of pipeline 'predictor'.

        params - dict of string -> object
            Parameters passed to 'predictor'.

        Return
        ------
        array-like, predictions
        """
        self.check_fitted()
        predictor = make_list(self.predictor)[0][1] if self.predictor else None
        if predictor is not None:
            X = predictor.predict(X, **params)
        return X

    def apply_postprocessors(self, X, **params):
        """
        Apply only a chain of pipeline's post-processing steps.

        Parameters
        ----------
        X - {array-like, sparse matrix}, potentially could be a tensor of arbitrary shape.
            Must fulfill input requirements of the first step of the pipeline 'postprocessors'.

        params - dict of string -> object
            Parameters passed to the 'fit' and 'transform' methods of each step in 'postprocessors', where
            each parameter name is prefixed such that parameter 'p' for step 's' for method [fit | transform]
            has key 's__[fit | trf]__p'.

        Return
        ------
        array-like, post-processed data
        """
        return self._apply_pre_or_post_only("postprocessors", X, **params)

    def _fit_pipeline(self, test_params_only, X, y, **params):
        rest_params = params
        last_fittable_postprocessor = self._last_fittable_estimator("postprocessors")

        if last_fittable_postprocessor is not None:
            Xt, rest_params = self._fit_transformers("preprocessors", None, test_params_only, X, y, **rest_params)
            rest_params = self._fit_predictor(test_params_only, Xt, y, **rest_params)
            Xt, rest_params = self._apply_predictor(test_params_only, Xt, **rest_params)
            Xt, rest_params = self._fit_transformers(
                "postprocessors", last_fittable_postprocessor, test_params_only, Xt, y, **rest_params
            )
        elif self._last_fittable_estimator("predictor") is not None:
            Xt, rest_params = self._fit_transformers("preprocessors", None, test_params_only, X, y, **rest_params)
            rest_params = self._fit_predictor(test_params_only, Xt, y, **rest_params)
        else:
            last_fittable_preprocessor = self._last_fittable_estimator("preprocessors")
            if last_fittable_preprocessor is not None:
                Xt, rest_params = self._fit_transformers(
                    "preprocessors", last_fittable_preprocessor, test_params_only, X, y, **rest_params
                )

        if test_params_only:
            return rest_params

    def _apply_pipeline(self, test_params_only, X, apply_postprocessors=True, **params):
        Xt, rest_params = self._apply_transformers("preprocessors", test_params_only, X, **params)
        Xt, rest_params = self._apply_predictor(test_params_only, Xt, **rest_params)
        if apply_postprocessors:
            Xt, rest_params = self._apply_transformers("postprocessors", test_params_only, Xt, **rest_params)
        if test_params_only:
            return rest_params
        else:
            return Xt

    def _last_fittable_estimator(self, attr):
        """Return a name of last estimator step at 'attr', which requires fitting at 'fit' stage"""
        for name, step in reversed(make_list(getattr(self, attr))):
            if step is not None:
                if requires_fit_on_fit(step):
                    return name

    def _fit_transformers(self, attr, last_step, test_params_only, X, y=None, **params):
        """
        Performs 'attr' steps fitting until 'last_step' transformer name.
        Returns transformed 'X' with a chain of transformers up to the previous to 'last_step'
        and also all the rest parameters to be used in later fitting.
        If 'last_step' is None, then performs 'fit' and 'transform' for a full chain of steps.

        Supports the same (as for 'X') transforms chain for validation set used for early stopping.
        Corresponding parameter name is assumed to be (appropriately prefixed) 'eval_set' (a list of tuples (X, y))
        as it is supported in the most popular packages like XGBoost, LightGBM and CatBoost.
        """
        Xt = X
        rest_params = params
        transformers = make_list(getattr(self, attr))

        if transformers:
            # Extract 'eval_set' to make the same feature transforms as for 'X'
            eval_set_name, eval_set = self._get_param_eval_set(**params)
            for i, (name, step) in enumerate(transformers):
                if step is None:
                    continue
                if (last_step is not None) and (name == last_step):
                    fit_params, rest_params = self._get_step_subparams(
                        step_name=name,
                        method_prefix="fit",
                        **rest_params
                    )
                    if not test_params_only:
                        step = step.fit(Xt, y, **fit_params)
                        transformers[i] = (name, step)
                    break
                params_step, rest_params = self._get_step_subparams(step_name=name, method_prefix=None, **rest_params)
                fit_params, trf_params = get_prefixed_subparams(
                    prefixes=["fit", "trf"],
                    raise_on_extra=True,
                    **params_step
                )
                if not test_params_only:
                    if requires_fit_on_fit_transform(step):
                        step = step.fit(Xt, y, **fit_params)
                    Xt = step.transform(Xt, **trf_params)
                    transformers[i] = (name, step)
                    for eval_idx, (eval_X, eval_y) in enumerate(eval_set):
                        eval_X = step.transform(eval_X, **trf_params)
                        eval_set[eval_idx] = (eval_X, eval_y)
            setattr(self, attr, transformers)
            if eval_set:
                rest_params.update({eval_set_name: eval_set})

        return Xt, rest_params

    def _apply_transformers(self, attr, test_params_only, X, **params):
        Xt = X
        rest_params = params
        transformers = make_list(getattr(self, attr))

        if transformers:
            for i, (name, step) in enumerate(transformers):
                if step is None:
                    continue
                elif requires_fit_on_transform(step):
                    fit_params, rest_params = self._get_step_subparams(
                        step_name=name,
                        method_prefix="fit",
                        **rest_params
                    )
                    if not test_params_only:
                        step = step.fit(Xt, y=None, **fit_params)
                        transformers[i] = (name, step)
                trf_params, rest_params = self._get_step_subparams(step_name=name, method_prefix="trf", **rest_params)
                if not test_params_only:
                    Xt = step.transform(Xt, **trf_params)
            setattr(self, attr, transformers)

        return Xt, rest_params

    def _fit_predictor(self, test_params_only, X, y, **params):
        rest_params = params
        if self._last_fittable_estimator("predictor") is not None:
            name, estimator = make_list(self.predictor)[0]
            fit_params, rest_params = self._get_step_subparams(step_name=name, method_prefix="fit", **rest_params)
            if not test_params_only:
                estimator = estimator.fit(X, y, **fit_params)
                self.predictor = (name, estimator)

        return rest_params

    def _apply_predictor(self, test_params_only, X, **params):
        Xt = X
        rest_params = params
        if self.predictor:
            name, estimator = make_list(self.predictor)[0]
            if estimator is not None:
                predict_params, rest_params = self._get_step_subparams(
                    step_name=name,
                    method_prefix="pred",
                    **rest_params
                )
                if not test_params_only:
                    Xt = estimator.predict(Xt, **predict_params)

        return Xt, rest_params

    def _apply_pre_or_post_only(self, attr, X, **params):
        self.check_fitted()
        rest_params = self._apply_transformers(attr, test_params_only=True, X=X, **params)[1]
        self._raise_on_extra_params(**rest_params)
        return self._apply_transformers(attr, test_params_only=False, X=X, **params)[0]

    def _get_param_eval_set(self, **params):
        """Deep search for 'eval_set' parameter (used in early stopping) passed to last simple 'predictor'"""
        param_name, eval_set = "", None
        estimator = self
        while isinstance(estimator, MLPipeline):
            if estimator.predictor:
                name, estimator = make_list(estimator.predictor)[0]
                param_name += "{}__fit__".format(name)
            else:
                param_name = ""
                break
        if param_name:
            param_name += "eval_set"
            eval_set = params.get(param_name)
        return param_name, make_list(eval_set)
