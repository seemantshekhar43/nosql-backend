import pandas as pd
import json

table_col_map = {}

table_col_map['part'] = ['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT']
table_col_map['partsupp'] = ['PS_PARTKEY','PS_SUPPKEY','PS_AVAILQTY','PS_SUPPLYCOST','PS_COMMENT']
table_col_map['nation'] = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
table_col_map['orders'] = ['O_ORDERKEY','O_CUSTKEY','O_ORDERSTATUS','O_TOTALPRICE','O_ORDERDATE','O_ORDERPRIORITY','O_CLERK','O_SHIPPRIORITY','O_COMMENT']
table_col_map['customer'] = ['C_CUSTKEY','C_NAME','C_ADDRESS','C_NATIONKEY','C_PHONE','C_ACCTBAL','C_MKTSEGMENT','C_COMMENT']
table_col_map['supplier'] = ['S_SUPPKEY','S_NAME','S_ADDRESS','S_NATIONKEY','S_PHONE','S_ACCTBAL','S_COMMENT']
table_col_map['lineitem'] = ['L_ORDERKEY','L_PARTKEY','L_SUPPKEY','L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT']
table_col_map['region'] = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']

def get_data_frame(name):
    col = table_col_map[name]
    col.append('dump')
    file_name = name + '.tbl'
    df = pd.read_table(file_name, sep='|', header=None)
    df.columns = col
    df.drop('dump', inplace=True, axis=1)
    return df

art_df = get_data_frame('part')
part_df['_id'] = part_df['P_PARTKEY'].apply(lambda x: str(x))
part_df.set_index("P_PARTKEY", drop=True, inplace=True)
part_list = part_df.to_dict(orient="index")

region_df = get_data_frame('region')
region_df['_id'] = region_df['R_REGIONKEY'].apply(lambda x: str(x))
region_df.set_index("R_REGIONKEY", drop=True, inplace=True)
region_list = region_df.to_dict(orient="index")

nation_df = get_data_frame('nation')
nation_df['N_REGION'] = nation_df['N_REGIONKEY'].apply(lambda x: region_list[x])
nation_df['_id'] = nation_df['N_NATIONKEY'].apply(lambda x: str(x))
nation_df.set_index("N_NATIONKEY", drop=True, inplace=True)
nation_df.drop('N_REGIONKEY', inplace=True, axis=1)
nation_list = nation_df.to_dict(orient="index")

customer_df = get_data_frame('customer')
customer_df['C_NATION'] = customer_df['C_NATIONKEY'].apply(lambda x: nation_list[x])
customer_df['_id'] = customer_df['C_CUSTKEY'].apply(lambda x: str(x))
customer_df.set_index("C_CUSTKEY", drop=True, inplace=True)
customer_df.drop('C_NATIONKEY', inplace=True, axis=1)
customer_list = customer_df.to_dict(orient="index")

supplier_df = get_data_frame('supplier')
supplier_df['S_NATION'] = supplier_df['S_NATIONKEY'].apply(lambda x: nation_list[x])
supplier_df['_id'] = supplier_df['S_SUPPKEY'].apply(lambda x: str(x))
supplier_df.set_index("S_SUPPKEY", drop=True, inplace=True)
supplier_df.drop('S_NATIONKEY', inplace=True, axis=1)
supplier_list = supplier_df.to_dict(orient="index")

partsupp_df = get_data_frame('partsupp')
partsupp_df['PS_SUPPLIER'] = partsupp_df['PS_SUPPKEY'].apply(lambda x: supplier_list[x])
partsupp_df['PS_PART'] = partsupp_df['PS_PARTKEY'].apply(lambda x: part_list[x])
partsupp_df['_id'] = partsupp_df[['PS_PARTKEY', 'PS_SUPPKEY']].apply(lambda x: str(x[0])+'#' + str(x[1]), axis=1)
partsupp_df.set_index(["PS_PARTKEY", "PS_SUPPKEY"], drop=True, inplace=True)
partsupp_list = partsupp_df.to_dict(orient="index")

lineitem_df = get_data_frame('lineitem')
lineitem_df['L_PARTSUPP'] = lineitem_df[['L_PARTKEY', 'L_SUPPKEY']].apply(lambda x: partsupp_list[(x[0], x[1])], axis=1)
lineitem_df['_id'] = lineitem_df[['L_PARTKEY', 'L_SUPPKEY']].apply(lambda x: str(x[0])+'#' + str(x[1]), axis=1)
lineitem_df.drop(['L_PARTKEY', 'L_SUPPKEY'], inplace=True, axis=1)
lineitem_list = lineitem_df.groupby('L_ORDERKEY')[lineitem_df.columns].apply(lambda x: x.to_dict(orient='records')).to_dict()

orders_df = get_data_frame('orders')
orders_df['O_CUSTOMER'] = orders_df['O_CUSTKEY'].apply(lambda x: customer_list[x])
orders_df.drop(['O_CUSTKEY'], inplace=True, axis=1)
orders_df['O_LINEITEMS'] = orders_df['O_ORDERKEY'].apply(lambda x: lineitem_list[x])
orders_df['_id'] = orders_df['O_ORDERKEY'].apply(lambda x: str(x))
orders_df.set_index("O_ORDERKEY", drop=True, inplace=True)
order_list = orders_df.to_dict(orient="records")

with open('./denorm.json', 'w') as fout:
    json.dump(order_list , fout)