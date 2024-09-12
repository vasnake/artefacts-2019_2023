# flake8: noqa

"""# Parser for infix expressions, two-stack-base

# Step 1: Create two stacks - the operand stack and the character stack.
# Step 2: Push the character to the operand stack if it is an operand.
# Step 3: If it is an operator, check if the operator stack is empty.
# Step 4: If the operator stack is empty, push it to the operator stack.
# Step 5: If the operator stack is not empty, compare the precedence of the operator and the top character in the stack.
# If the character's precedence is greater than or equal to the precedence of the stack top of the operator stack,
# then push the character to the operator stack.
# Otherwise, pop the elements from the stack until the character's precedence is less or the stack is empty.
# Step 6: If the character is '(', push it into the operator stack.
# Step 7: If the character is ')', then pop until '(' is encountered in the operator stack.
"""

from pprint import pformat


class TestJoinRule(object):

    def test_parser_class(self):
        # from prj.apps.joiner.features.join_rule import BinaryExprParser

        def _value(operand):
            return int(operand)

        def _eval(left, op, right):
            if op == "+":
                res = _value(left) + _value(right)
            elif op == "-":
                res = _value(left) - _value(right)
            else:
                raise ValueError("Unknown operation `{}`".format(op))
            return res

        # parser = BinaryExprParser(_eval)
        # assert parser.evaluate(parser.tokenize("1 + 2")) == 3
        # assert parser.evaluate(parser.tokenize("1 + 2 - 4 + 5 - 6 + 7 - 8 + 9")) == 6
        # assert parser.evaluate(parser.tokenize("7 - (3 + 4) - 5")) == -5
        # assert parser.evaluate(parser.tokenize("(((((((((((((1 + (2 - (((((((4 + (((5 - 6))) + 7))))))) - 8) + 9)))))))))))))")) == -6

    def test_parser(self):
        assert evaluate(parse("1 + 2")) == 3
        assert evaluate(parse("1 + 2 - 4")) == -1
        assert evaluate(parse("1 + 2 - 4 + 5 - 6 + 7 - 8 + 9")) == 6
        assert evaluate(parse("7 - (3 + 4)")) == 0
        assert evaluate(parse("(7 - 3) + 4")) == 8
        assert evaluate(parse("7 - (3 + 4) - 5")) == -5
        assert evaluate(parse("((((((((1 + 2)) - 4) + 5) - 6) + 7) - 8) + 9)")) == 6
        assert evaluate(parse("(1 + 2) - (4 + 5) - (6 + 7) - (8 + 9)")) == -36
        assert evaluate(parse("(1 + (2 - (4 + (5 - (6 + (7 - (8 + 9)))))))")) == -10
        assert evaluate(parse("1 + (2 - (4 + (5 - 6) + 7) - 8) + 9")) == -6
        assert evaluate(parse("(1 + (2 - (4 + (5 - 6) + 7) - 8) + 9)")) == -6
        assert evaluate(parse("(((((((((((((1 + (2 - (((((((4 + (((5 - 6))) + 7))))))) - 8) + 9)))))))))))))")) == -6


def parse(join_rule):
    words = join_rule.replace("(", " ( ").replace(")", " ) ").split(" ")
    return [word.strip() for word in words if len(word.strip()) > 0]


def evaluate(tokens):
    if len(tokens) < 3:
        raise ValueError("join_rule must contain at least 3 tokens, got '{}'".format(pformat(tokens)))

    def _log(message):
        pass
        # print("EVALUATE {}".format(message))

    def _value(operand):
        return int(operand)

    def _eval(left, op, right):
        if op == "+":
            res = _value(left) + _value(right)
        elif op == "-":
            res = _value(left) - _value(right)
        else:
            raise ValueError("Unknown operation `{}`".format(op))
        _log("_eval: {} {} {} = {}".format(left, op, right, res))
        return res

    def _next_token(tokens):
        if len(tokens) < 1:
            raise ValueError("Not enough tokens in join_rule")
        return tokens[0], tokens[1:]

    def _eval_parenthesis(tokens):
        _log("_eval_parenthesis: tokens {}".format(pformat(tokens)))
        left, tokens = _next_token(tokens)
        if left == "(":
            left, tokens = _eval_parenthesis(tokens)
        while len(tokens) > 0:
            next_token, tokens = _next_token(tokens)
            if next_token == ")":
                _log("_eval_parenthesis: result: `{}`, tokens {}".format(left, pformat(tokens)))
                return left, tokens
            left, tokens = _eval_with_left(left, [next_token] + tokens)
        _log("_eval_parenthesis: result: `{}`, tokens {}".format(left, pformat(tokens)))
        return left, tokens

    def _eval_with_left(left, tokens):
        _log("_eval_with_left: left `{}`, tokens {}".format(left, pformat(tokens)))
        op, tokens = _next_token(tokens)
        right, tokens = _next_token(tokens)
        if right == "(":
            right, tokens = _eval_parenthesis(tokens)
        res = _eval(left, op, right)
        _log("_eval_with_left: result `{}`, tokens {}".format(res, pformat(tokens)))
        return res, tokens

    _log("evaluate: tokens {}".format(pformat(tokens)))

    left, tokens = _next_token(tokens)
    if left == "(":
        left, tokens = _eval_parenthesis(tokens)
    while len(tokens) > 0:
        left, tokens = _eval_with_left(left, tokens)

    return left
