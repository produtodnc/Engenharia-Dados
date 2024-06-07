#!/home/lkeiti/Documents/dnc-scripts/.env/bin/python3 -u
# Note: the -u denotes unbuffered (running python in a streaming mode)

import json
import os
import sys
import time

FIELDS_TO_PARSE = ['holding_stock', 'holding_quantity']


def parse_create(payload_after):
    current_ts = time.time()
    out_tuples = []
    for field_to_parse in FIELDS_TO_PARSE:
        out_tuples.append(
            (
                "INSERT",
                payload_after.get('holding_id'),
                payload_after.get('user_id'),
                field_to_parse,
                None,
                payload_after.get(field_to_parse),
                payload_after.get('datetime_created'),
                None,
                None,
                current_ts
            )
        )

    return out_tuples


def parse_delete(payload_before, ts_ms):
    current_ts = time.time()
    out_tuples = []
    for field_to_parse in FIELDS_TO_PARSE:
        out_tuples.append(
            (
                "DELETE",
                payload_before.get('holding_id'),
                payload_before.get('user_id'),
                field_to_parse,
                None,
                payload_before.get(field_to_parse),
                None,
                None,
                ts_ms,
                current_ts
            )
        )

    return out_tuples


def parse_update(payload):
    current_ts = time.time()
    out_tuples = []
    for field_to_parse in FIELDS_TO_PARSE:
        out_tuples.append(
            (
                'UPDATE',
                payload.get('before', {}).get('holding_id', {}),
                payload.get('before', {}).get('user_id', {}),
                field_to_parse,
                payload.get('before', {}).get(field_to_parse),
                payload.get('after', {}).get(field_to_parse),
                None,
                payload.get('ts_ms'),
                None,
                current_ts
            )
        )

    return out_tuples


def parse_payload(input_raw_json):
    input_json = json.loads(input_raw_json)
    op_type = input_json.get('payload', {}).get('op')
    if op_type == 'c':
        return parse_create(input_json.get('payload', {}).get('after', {}))
    elif op_type == 'd':
        return parse_delete(
            input_json.get('payload', {}).get('before', {}),
            input_json.get('payload', {}).get('ts_ms', None)
        )
    elif op_type == 'u':
        return parse_update(input_json.get('payload', {}))
    return []


for line in sys.stdin:
    data = parse_payload(line)
    for log in data:
        log_str = ','.join(str(elt) for elt in log)
        print(log_str, flush=True)
