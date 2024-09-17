import traceback

import six

from prj.apps.utils.control.client.logs import ControlLoggingMixin
from prj.apps.utils.control.client.exception import FatalStatusException


class JoinRuleEvaluator(ControlLoggingMixin):
    """Join rule parser and evaluator.

    It takes a `dataframes` dict (name => DataFrame) and use it for `join_rule` evaluation, producing joined DataFrame.

    Join rule must be composed of data frame names and join types (see parameter `how` in spark.sql.DataFrame.join).

    Evaluator use a `checkpoint_service`, if defined, executing resulting data frame checkpoint after each join.

    :param dataframes: (name => DataFrame) dictionary as evaluator environment.
    :type dataframes: dict[str, :class:`pyspark.sql.DataFrame`]
    :param join_keys: list of join keys (see parameter `on` in spark.sql.DataFrame.join).
    :type join_keys: list[str]
    :param checkpoint_service: object with method `checkpoint(df)` that returns checkpointed data frame.
    :param str log_url: url for ControlLoggingMixin logic.
    """

    def __init__(self, dataframes, join_keys=("uid",), checkpoint_service=None, log_url=None):
        self.dataframes = dataframes
        self.join_keys = list(join_keys)
        self.checkpoint_service = checkpoint_service
        self.log_url = log_url

    def evaluate(self, join_rule):
        tokens = self._tokenize(join_rule)

        if len(tokens) < 3:
            df_name = tokens[0]
            if df_name:
                self.info(
                    "join_rule contains only one name, returning corresponding DataFrame. "
                    "join_rule `{}`, DF name `{}`".format(join_rule, df_name)
                )
                return self.dataframes[df_name]

            self.info("join_rule is empty, returning first DataFrame.")
            return self.dataframes.values()[0]

        return self._build_df(tokens)

    def _join(self, left, op, right):
        # Called from interpreter to compute primitive expression (leaf in the AST)

        def _dataframe(df_or_name):
            if isinstance(df_or_name, six.string_types):
                return self.dataframes[df_or_name]
            return df_or_name

        try:
            df = _dataframe(left).join(_dataframe(right), on=self.join_keys, how=op)
        except Exception:
            raise FatalStatusException(traceback.format_exc())

        if self.checkpoint_service is not None:
            return self.checkpoint_service.checkpoint(df)
        return df

    @staticmethod
    def _tokenize(expression):
        words = expression.replace("(", " ( ").replace(")", " ) ").split(" ")
        return [word.strip() for word in words if len(word.strip()) > 0]

    def _build_df(self, tokens):
        def _next_token(tokens):
            if len(tokens) < 1:
                raise ValueError("Not enough tokens in expression")
            return tokens[0], tokens[1:]

        def _eval_parenthesis(tokens):
            left, tokens = _next_token(tokens)
            if left == "(":
                left, tokens = _eval_parenthesis(tokens)

            while len(tokens) > 0:
                next_token, tokens = _next_token(tokens)
                if next_token == ")":
                    return left, tokens

                left, tokens = _eval_with_left(left, [next_token] + tokens)

            return left, tokens

        def _eval_with_left(left, tokens):
            op, tokens = _next_token(tokens)
            right, tokens = _next_token(tokens)
            if right == "(":
                right, tokens = _eval_parenthesis(tokens)

            return self._join(left, op, right), tokens  # Single line where real work is done

        left, tokens = _next_token(tokens)
        if left == "(":
            left, tokens = _eval_parenthesis(tokens)
        while len(tokens) > 0:
            left, tokens = _eval_with_left(left, tokens)

        return left
