"""
Integration tests

pipenv run python -m pytest prj/apps/apply/test/it -k "not apply_speed" -x -svv --tb=short --disable-warnings --log-cli-level=WARN || exit
pipenv run python -m pytest prj/apps/apply/test/it -k "apply_speed" -x -svv --tb=short --disable-warnings --log-cli-level=WARN || exit

"""
