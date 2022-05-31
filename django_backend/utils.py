import math
import enum

from pymongo import MongoClient
from pymongo.cursor import Cursor


class Operations(enum.Enum):
    get_table = 1
    where = 2
    sort = 3


payload = {'table': 'region', 'where_expr': {}, 'sortby': 'n_name', 'asc': False}


def get_db_client():
    client = MongoClient('localhost', 27017)
    db = client['tpch']
    return client, db


def transform_object(obj):
    obj['_id'] = str(obj['_id'])
    return obj


def cursor_to_list(cursor):
    return list(cursor)
    return list(map(transform_object, cursor))


def cursor_paginator(table_cursor: Cursor, page_size, page_num) -> Cursor:
    """
    returns a set of documents belonging to page number `page_num`
    where size of each page is `page_size`.
    """

    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)

    page_count = math.ceil(table_cursor.count() / page_size)

    # Skip and limit
    cursor = table_cursor.skip(skips).limit(page_size)

    return cursor, page_num, page_count


def sort_cursor(cursor: Cursor, sortby, asc=True) -> Cursor:
    return cursor.sort(sortby, 1 if asc else -1)


{
    'param': {
        'table': 'orders',
        'where_expr': {'O_TOTALPRICE': {'$lte': 1000}},
        'sortby': {'col': 'O_TOTALPRICE', 'is_asc': True},
        'page_num': 1
    }
}
