import json
import os
import re
import time
from pathlib import Path
from dotenv import load_dotenv
import requests
import urllib3.util
from websockets import Subprotocol
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect
from loguru import logger
load_dotenv()
uri = '{scheme}://{host}/v1/database/{module}/{endpoint}'
proto = Subprotocol('v1.json.spacetimedb')
logger.add("debug.log", rotation="5 MB")

def process_msg(msg):
    data = json.loads(msg)
    if 'Transaction' in msg:
        upd = data['TransactionUpdate']['status']['Committed']['tables']
        upd = upd[0]['updates']
        for u in upd:
            rows = [json.loads(row) for row in u['inserts']]
            for r in rows:
                mObj = {
                    'channel': r[3],
                    'name': r[1],
                    'text': r[5]
                }
                logger.info(json.dumps(mObj))
        # print(upd)
    # if 'InitialSubscription' in data:
    #     initial = data['InitialSubscription']['database_update']['tables']
    #     for table in initial:
    #         rows = table['updates'][0]['inserts']
    #         entries = [json.loads(row) for row in rows]
    #         for i in entries:
    #             print(f'{i['username']}: {i['text']}')
    # elif 'TransactionUpdate' in data and 'Failed' in data['TransactionUpdate']['status']:
    #     failure = data['TransactionUpdate']['status']['Failed']
    #     if bad_table := re.match(r'`(\w*)` is not a valid table', failure):
    #         bad_table = bad_table.group(1)
    #         print('Invalid table')
            
def dump_tables(host, module, queries, auth=None,query_strings=None):
    save_data = {}
    new_queries = None
    if isinstance(queries, str):
        queries = [queries]
    
    try:
        with connect(
                uri.format(scheme='wss', host=host, module=module, endpoint='subscribe'),
                # user_agent_header=None,
                additional_headers={"Authorization": auth} if auth else {},
                subprotocols=[proto],
                max_size=None,
                max_queue=None
        ) as ws:
            print('connected')
            ws.recv()
            sub = dict(Subscribe=dict(
                request_id=1,
                query_strings=[
                    f'SELECT * FROM {q};' if isinstance(q, str) else
                    f'SELECT * FROM {q[0]} WHERE {q[1]} = {q[2]};'
                    for q in queries
                ]
            ))
            if query_strings:
                sub['Subscribe']['query_strings'] = query_strings
            sub = json.dumps(sub)
            print(sub)
            ws.send(sub)
            while True:
                try:
                    msg = ws.recv(timeout=5) # This will block until a message is received
                    if msg:
                        process_msg(msg)
                except WebSocketException as e:
                   print(f"WebSocket error during reception: {e}")
                    # You might want to break or try to reconnect here
                    # pass
                except Exception as e:
                    # pass
                    print(f"An unexpected error occurred during message reception: {e}")
                    # Handle other potential exceptions here

        
    except WebSocketException as ex:
        raise ex

    if new_queries:
        return dump_tables(host, module, new_queries, auth=auth)

    return save_data


def get_schema(host, module):
    target = uri.format(scheme='https', host=host, module=module, endpoint='schema')
    res = requests.get(target, params=dict(version=9))
    return res.json() if res.status_code == 200 else None


def load_tables_names(table_file):
    with open(table_file, 'r') as f:
        return [t.strip() for t in f.readlines() if t.strip()]


def get_region_info(global_host, auth):
    res = dump_tables(global_host, 'bitcraft-3', 'region_connection_info', auth)
    obj = res['region_connection_info'][-1]
    return urllib3.util.parse_url(obj['host']).host, obj['module']


def save_tables(data_dir, subdir, tables):
    root = data_dir / subdir
    root.mkdir(exist_ok=True)

    def _get_sort(x):
        # incredibly ugly but ok
        return x.get('id', x.get('item_id', x.get('building_id', x.get('name', x.get('cargo_id', x.get('type_id', -1))))))

    for name, data in tables.items():
        data = sorted(data, key=_get_sort)
        with open(root / (name + '.json'), 'w') as f:
            json.dump(data, fp=f, indent=2)


def table_names_to_file(schema_glb, table_file):
    tables = schema_glb.get("tables", [])
    tables = {t['name']: 'Public' in t['table_access'] for t in tables}
    public = [k for k, v in tables.items() if v]
    private = [k for k, v in tables.items() if not v]
    with open(table_file, 'w') as f:
        json.dump(dict(public=public, private=private), fp=f, indent=2)


def main():
    data_dir = Path(os.getenv('DATA_DIR') or 'server')
    data_dir.mkdir(exist_ok=True)
    global_host = os.getenv('BITCRAFT_SPACETIME_HOST')
    if not global_host:
        raise ValueError('BITCRAFT_SPACETIME_HOST not set')
    auth = os.getenv('BITCRAFT_SPACETIME_AUTH') or None

    # schema_glb = get_schema(global_host, 'bitcraft-global')
    # if schema_glb:
    #     with open(data_dir / 'global_schema.json', 'w') as f:
    #         json.dump(schema_glb, fp=f, indent=2)
    #     table_file = data_dir / 'global_tables.json'
    #     table_names_to_file(schema_glb, table_file)

    # region_host, region_module = get_region_info(global_host, auth)

    # schema = get_schema(region_host, region_module)
    # if schema:
    #     with open(data_dir / 'schema.json', 'w') as f:
    #         json.dump(schema, fp=f, indent=2)
    #     table_file = data_dir / 'region_tables.json'
    #     table_names_to_file(schema_glb, table_file)

    # global_tables = load_tables_names(curr_dir / 'global_tables.txt')
    region_file = data_dir / 'region_tables.json'
    with open(region_file, 'r') as rf:
        file_content = rf.read()
        region_tables = json.loads(file_content)
    region_tables = region_tables['public']

    # if global_tables:
    #     global_res = dump_tables(global_host, 'bitcraft-3', global_tables, auth)
    #     save_tables(data_dir, 'global', global_res)
    # import time
    # if region_tables:
    #     region_res = dump_tables(global_host, 'bitcraft-3', region_tables, auth)
    #     print(region_res)

    tables = [['chat_message_state', 'username','\'raffian\'','timestamp']]
    query_strings = []
    query_strings.append("select * from chat_message_state where timestamp > 1753555151")
    region_res = dump_tables(global_host, 'bitcraft-3', tables, auth,query_strings=query_strings)
    # print(region_res)
    save_tables(data_dir, 'region', region_res)


    # exceptions get raised all the way, so if we're here, it should be successful
    # of course, if the tables were actually emptied on the DB, we'll get a bunch of blanks
    # but as long as it wasn't due to outages or whatever, that's an acceptable diff
    # if gho := os.getenv('GITHUB_OUTPUT'):
    #     with open(gho, 'a') as f:
    #         f.write(f'updated_data=true')


if __name__ == '__main__':
    main()