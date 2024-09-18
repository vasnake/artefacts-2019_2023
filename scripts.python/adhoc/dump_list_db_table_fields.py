""" Print all names from Hive MetaStore: db, table, fields, partitioned_by.

Run on hadoop devpoint:

truncate --size 0 ~/temp.temp; \
  /data/prj-anaconda/conda-prj-effective/bin/python \
  dump_list_db_table_fields.py | tee -a temp.temp
"""

import json
from prj.common.hive import HiveMetastoreClient


hmc = HiveMetastoreClient()

dbs = sorted(hmc.get_all_databases())  # type: list
print("{} databases ...".format(len(dbs)))

all_items = []
for db in dbs:
    tables = sorted(hmc.get_all_tables(db))
    for table in tables:
        columns = hmc.get_columns(table, db)
        partition_names = hmc.get_partition_names(table, db)

        obj = {"{}.{}".format(db, table): {"columns": columns, "partitioned": partition_names}}
        print(json.dumps(obj, sort_keys=True, separators=(",", ":")))
        all_items.append(obj)

print("collected {} records".format(len(all_items)))

with open("databases_tables_fields.json", "w") as f:
    json.dump(all_items, f, sort_keys=True, separators=(",", ":"))

print("DONE")
