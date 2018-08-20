#!/usr/bin/python
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql


connection = pg.connect("host=localhost dbname=edb user=enterprisedb password=!Honey01 port=5444")
#my_table   = pg.read_sql_table('table_name', connection)
my_table    = pd.read_sql('select * from scenario1', connection)
another_attempt= psql.read_sql("SELECT * FROM scenario1", connection)

print(my_table)

# OR
print(another_attempt)