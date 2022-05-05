import pandas as pd
import os
import time

schema = {
    'customer': ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'],
    'lineitem': ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'],
    'nation': ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'],
    'orders': ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'],
    'part': ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'],
    'partsupp': ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'],
    'region': ['R_REGIONKEY', 'R_NAME', 'R_COMMENT'],
    'supplier': ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
}

for table_name in schema:
    schema[table_name] = ','.join(schema[table_name]).split(',')

try:
    os.mkdir('jsons')
except:
    pass

tick_f = time.time()

for table_name in schema:
    tick = time.time()

    df = pd.read_table(f'tables/{table_name}.tbl',
                       sep='|', index_col=False, header=None)
    df.drop(df.columns[-1], axis=1, inplace=True)
    df.columns = schema[table_name]
    df.to_json(f'jsons/{table_name}.json', orient='records')

    tock = time.time()
    print("Time taken to convert %s table: %.3f s" % (table_name, tock - tick))

tock = time.time()

print("Total time taken: %.3f s" % (tock - tick_f))
