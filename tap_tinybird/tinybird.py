"""
Modules containing all Sumologic related features
"""
from typing import List, Dict

import backoff
import singer
import requests
from datetime import datetime
import time
from dateutil.relativedelta import *

from tb.api import API

LOGGER = singer.get_logger()
RECORD_FETCHING_LIMIT = 10000

def retry_pattern():
    """
    Retry decorator to retry failed functions
    :return:
    """
    return backoff.on_exception(backoff.expo,
                                requests.HTTPError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    """
    For logging attempts to connect with Amazon
    :param details:
    :return:
    """
    LOGGER.info("Error detected communicating with Tinybird, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def get_schema_for_table(config: Dict, table_spec: Dict) -> Dict:
    """
    Detects json schema using a record set of query
    :param config: Tap config
    :param table_spec: tables specs
    :return: detected schema
    """
    schema = {}
    LOGGER.info('Getting records for query to determine table schema.')

    params = table_spec.get('params') 
    query = table_spec.get('query') 
    time_property = table_spec.get('time_property') 
    
    if time_property:
        dt = datetime.utcnow()
        # truncate to the start of current day day
        from_time = (dt.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(days=-2)).strftime('%Y-%m-%d')
        time_query = time_property + ' > \'' + from_time + '\' '
        
        q = query.format(time_query = time_query)
    else:
        q = query
    
    q += ' limit 100 '

    fields = get_fields(config, q, params)

    key_properties = table_spec.get('keys')
    for field in fields:
        field_name = field['name']
        field_type = field['type']

        schema[field_name] = {
            'type': ['null', 'string']
        }

        if field_type in ['Int', 'Integer', 'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256']:
            schema[field_name]['type'].append('integer')
        elif field_type in ['Float32', 'Float64']:
            schema[field_name]['type'].append('number')
        elif field_type == 'Boolean':
            schema[field_name]['type'].append('boolean')
        elif field_type in ['DateTime', 'DateTime64', 'Date']:
            schema[field_name]['type'].append('datetime') #Is it right??

    return {
        'type': 'object',
        'properties': schema,
        'key_properties': key_properties
    }

def get_fields(config, q, params):
    fields = []

    response = run_query(config, q, params)

    if response.status_code == 200:
        fields = response.json()['meta']

    return fields

# TODO fix
def get_records(config, q, params, limit):
    records = []   

    now_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    custom_columns = {
        '_SDC_EXTRACTED_AT': now_datetime,
        '_SDC_BATCHED_AT': now_datetime,
        '_SDC_DELETED_AT': None
    }
    
    #LOGGER.debug("Get records %d of %d, limit=%d", count, record_count, limit)
    # TODO apply the limit
    response = run_query(config, q, params)

    if response.status_code == 200:
        response_json = response.json()
        record_count = response_json['rows']
        response_records = response_json['data']
        
        # TODO implement paging
        #count = 0
        #while count < record_count:
        # TODO move query in here to get the pages
            
            #LOGGER.debug("Got records %d of %d", count, record_count)

            #recs = response['records']
            # extract the result maps to put them in the list of records
        for rec in response_records:
            records.append({**rec, **custom_columns})

            #if len(recs) > 0:
            #    count = count + len(recs)
            #else:
            #    break # make sure we exit if nothing comes back

    return records

def run_query(config, q, params):
    tinybird_access_token = config['tinybird_access_token']
    tinybird_api_url = config['tinybird_api_url']

    LOGGER.info("Run query in tinybird with query: " + q)
    tb_api = API(tinybird_access_token, tinybird_api_url)
    
    response = tb_api.post("/sql", data=q + " FORMAT JSON", params=params)

    LOGGER.info(response.status_code)
    return response
    
        
        
 