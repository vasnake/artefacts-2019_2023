"""Check names in storage.

Read names (db, table, columns) from file,
for each db: check db name, check each table and each column in table.

Print stats for each db: how many tables total, how many tables have wrong names.

Check: name is selected by RegExp.
"""

import re
import six
import json
from pprint import pformat as pf

# REG_EXP = r"^[a-z]+([a-z]|[0-9]|_)*$"  # alphanumeric + _, start with letter
# REG_EXP = r"^([a-z]|[0-9]|_)+$"  # alphanumeric + _, lowercase
REG_EXP = r"^([a-z]|[0-9]|_)*([a-z])+([a-z]|[0-9]|_)*$"  # at least one letter

INPUT_FILE_PATH = "/Users/vlk/Downloads/temp.temp.json"
OUTPUT_FILE_PATH_PREFIX = "/Users/vlk/Downloads/db_table_col_report"


def log(*args, **kwargs):
    print("{} {}".format(pf(args), pf(kwargs)))


def read_file(file_path):
    with open(file_path, "rt") as f:
        res = f.read()
    return res


def write_file(file_path, text):
    with open(file_path, "wb") as f:
        f.write(text)
    return None


def check(db, table, columns, reg_exp):
    def _check(text):
        res = reg_exp.match(text)
        if res and text == res.group():
            return True

        log(db, table, INVALID=text)
        return False

    db_check = _check(db)
    table_check = _check(table)
    columns_check = all(_check(column) for column in columns)

    return {"db": db_check, "table": table_check, "columns": columns_check}


def main():
    file_text = read_file(INPUT_FILE_PATH)
    # log(head=file_text[:10], tail=file_text[-10:])

    json_obj = json.loads(file_text)
    # log(type(json_obj), len(json_obj), head=json_obj[:3], tail=json_obj[-3:])
    # one record: {db.table: {columns: [], partitioned: []}}

    compiled_regexp = re.compile(REG_EXP, re.DEBUG)
    stats = {}

    for i, record in enumerate(json_obj):
        record = list(six.iteritems(record))
        assert len(record) == 1

        db_table, columns = record[0]

        db, table = db_table.encode("utf8").split(".", 1)

        columns = [
            field_name.encode("utf8")
            for field_name in (columns["columns"] + columns["partitioned"])
        ]

        # log(db, table, ",".join(columns))

        check_info = check(db, table, columns, compiled_regexp)
        # log(check_info)
        # {'columns': True, 'db': True, 'table': True}

        db_stats = stats.get(db, {})
        table_stats = db_stats.get(table, {})

        table_stats["TABLE_NAME"] = table_stats.get("TABLE_NAME", True) and check_info["table"]
        table_stats["COLUMN_NAME"] = table_stats.get("COLUMN_NAME", True) and check_info["columns"]
        db_stats["DB_NAME"] = db_stats.get("DB_NAME", True) and check_info["db"]

        db_stats[table] = table_stats
        stats[db] = db_stats

    report = json.dumps(stats)
    write_file("{}-full.json".format(OUTPUT_FILE_PATH_PREFIX), report)

    report = aggregate(stats)
    write_file("{}-aggregated.json".format(OUTPUT_FILE_PATH_PREFIX), report)

    report = aggregate(stats, csv=True)
    write_file("{}-aggregated.csv".format(OUTPUT_FILE_PATH_PREFIX), report)

    log("DONE")


def aggregate(stats, csv=False):
    # db: db_stats
    # DB_NAME: bool, table_name: table_stats
    # TABLE_NAME: bool, COLUMN_NAME: bool

    report = {}

    for db_name, db_stats in six.iteritems(stats):
        db_name_validity = True
        count_valid_tables = 0
        count_invalid_tables = 0
        count_invalid_table_names = 0

        for table_name, table_stats in six.iteritems(db_stats):
            if table_name == "DB_NAME":
                db_name_validity = table_stats and db_name_validity

            else:
                if table_stats["TABLE_NAME"] and table_stats["COLUMN_NAME"]:
                    count_valid_tables += 1
                else:
                    count_invalid_tables += 1

                if not table_stats["TABLE_NAME"]:
                    count_invalid_table_names += 1

        report[db_name] = {
            "DB_NAME_VALID": db_name_validity,
            "TABLE_VALID_CNT": count_valid_tables,
            "TABLE_INVALID_CNT": count_invalid_tables,
            "TABLE_NAME_INVALID_CNT": count_invalid_table_names,
        }

    if csv:
        lines = ["db_name,db_name_valid,table_valid_cnt,table_invalid_cnt,table_name_invalid_cnt"]
        for db_name, rec in six.iteritems(report):
            lines.append("{},{},{},{},{}".format(
                db_name,
                rec["DB_NAME_VALID"], rec["TABLE_VALID_CNT"], rec["TABLE_INVALID_CNT"], rec["TABLE_NAME_INVALID_CNT"]
            ))
        return "\n".join(lines)
    else:
        return json.dumps(report)


main()
