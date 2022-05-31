from rest_framework.response import Response
from rest_framework.decorators import api_view
from utils import *
import json

client, db = get_db_client()


@api_view(['GET'])
def get_table(request):
    request_dict = json.loads(request.GET.get('param'))
    table_name = str(request_dict.get('table'))
    page_num = int(request_dict.get('page_num', 1))
    where_expr = request_dict.get('where_expr', {})

    cursor = db[table_name].find(where_expr, {'_id': 0})

    if 'sortby' in request_dict:
        is_asc = request_dict['sortby']['is_asc']
        cursor = sort_cursor(cursor, sortby=request_dict['sortby']['col'], asc=is_asc)

    paginated_cursor, page_num, page_count = cursor_paginator(cursor, 10, page_num)

    response = {'data': list(paginated_cursor), 'table_name': table_name, 'page_num': page_num,
                'page_count': page_count}

    return Response(response)
