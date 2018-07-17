#! /usr/bin/python

import csv
import sqlite3
import sys

# The first two arguments are the source file and the new db filename. These
# are mandatory.
if len(sys.argv) < 3:
    print("Usage:\n  tosqlite.py <input> <dbname> [<table>] [<col>...]")
    sys.exit(1)

# The third argument is the table name, or just "table" if it's not provided.
TABLE_NAME = "default"
if len(sys.argv) >= 4:
    TABLE_NAME = sys.argv[3]

# All arguments after the third are column names. If we have more columns of
# data than we have column names, the defaults will be "col1", "col2", etc.
COLUMNS = sys.argv[4:] or ["col1"]

input_file = open(sys.argv[1])
reader = csv.reader(input_file, delimiter=",")
conn = sqlite3.connect(sys.argv[2])

create_statement = "CREATE TABLE %s (%s)" % (
    TABLE_NAME,
    ", ".join("%s" % col for col in COLUMNS))
conn.execute(create_statement)
print("Created table", TABLE_NAME, "with columns:", ", ".join(COLUMNS))

# Create an index on the first column.
add_index_statement = "CREATE INDEX %s ON %s (%s)" % (TABLE_NAME + "_index", TABLE_NAME, COLUMNS[0])
conn.execute(add_index_statement)
print("Added an index on", COLUMNS[0])

count = 0
for row in reader:
    count += 1

    # Add more columns if needed.
    for i in range(len(COLUMNS), len(row)):
        new_col = "col" + str(i+1)
        COLUMNS.append(new_col)
        add_column_statement = "ALTER TABLE %s ADD %s" % (TABLE_NAME, new_col)
        conn.execute(add_column_statement)
        print("Added column:", new_col)

    # Add NULLs to the row for any missing columns.
    row += [None] * (len(COLUMNS) - len(row))

    # Insert the row.
    insert_statement = "INSERT INTO %s (%s) VALUES (%s)" % (
        TABLE_NAME,
        ", ".join(COLUMNS),
        ", ".join(["?"] * len(COLUMNS)))
    conn.execute(insert_statement, row)

print("Inserted", count, "rows.")

input_file.close()
conn.commit()
conn.close()
