#!/usr/bin/env python3
#
# Generate pg_alohadb_convert_sql, pg_alohadb_convert_db and
# pg_alohadb_convert_db.sql from rename_map.json and the *.in templates.
#
# Usage: generate.py SRCDIR OUTDIR
#
# Copyright (c) 2026, AlohaDB

import json
import os
import sys

src, out = sys.argv[1], sys.argv[2]
with open(os.path.join(src, "rename_map.json"), encoding="utf-8") as f:
    m = json.load(f)


def sql_values(d):
    return ",\n".join("  ('%s', '%s')" % (k, v) for k, v in sorted(d.items()))


compact = json.dumps(m, sort_keys=True, indent=1)
with open(os.path.join(src, "common.py.in"), encoding="utf-8") as f:
    common = f.read().replace("@MAP@", compact)

for name in ("pg_alohadb_convert_sql", "pg_alohadb_convert_db"):
    with open(os.path.join(src, name + ".in"), encoding="utf-8") as f:
        text = f.read().replace("@COMMON@", common)
    path = os.path.join(out, name)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    os.chmod(path, 0o755)

with open(os.path.join(src, "pg_alohadb_convert_db.sql.in"), encoding="utf-8") as f:
    text = f.read()
text = (text.replace("@EXTS@", sql_values(m["exts"]))
            .replace("@TOKENS@", sql_values(m["tokens"]))
            .replace("@GUCS@", sql_values(m["gucs"])))
with open(os.path.join(out, "pg_alohadb_convert_db.sql"), "w",
          encoding="utf-8", newline="\n") as f:
    f.write(text)
