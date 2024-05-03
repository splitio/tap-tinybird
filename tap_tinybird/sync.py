"""
Syncing related functions
"""

import sys
from typing import Dict, List
from singer import metadata, get_logger, Transformer, utils, get_bookmark, write_bookmark, write_state, write_record
from tap_tinybird import tinybird
from datetime import datetime
from dateutil.relativedelta import *

LOGGER = get_logger()

def sync_stream(config: Dict, state: Dict, table_spec: Dict, stream: Dict) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    time_property = table_spec.get('time_property')
    time_bucket = table_spec.get('time_bucket')
    if time_property:
        if time_bucket is None or time_bucket == "none":
            return sync_stream_incremental(config, state, table_spec, stream)
        else:
            return sync_stream_by_time(config, state, table_spec, stream)
    else:
        return sync_stream_full_table(config, state, table_spec, stream)
    
    
def sync_stream_full_table(config: Dict, state: Dict, table_spec: Dict, stream: Dict) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    table_name = table_spec.get("table_name") 
    q = table_spec.get('query')
    params = table_spec.get('params')
    
    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Syncing query "%s" with params %s.', q, params)
    
    records_synced = 0
    records = tinybird.get_records(config, q, params, limit=100000)

    for record in records:
        with Transformer() as transformer:
            to_write = transformer.transform(record, stream['schema'], metadata.to_map(stream['metadata']))

        write_record(table_name, to_write)
        records_synced += 1

    LOGGER.info('Wrote %s records for table "%s".', records_synced, table_name)

    return records_synced

def sync_stream_by_time(config: Dict, state: Dict, table_spec: Dict, stream: Dict) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    table_name = table_spec.get("table_name") 
    time_property = table_spec.get('time_property')
    time_bucket = table_spec.get('time_bucket')
    q = table_spec.get('query')
    params = table_spec.get('params')
    
    LOGGER.info('Syncing table "%s".', table_name)
    
    modified_since = get_bookmark(state, table_name, 'modified_since') or config['start_date']
    end_date = config.get('end_date')
    max_lookback_days = table_spec.get("max_lookback_days") or 90

    LOGGER.info('Config info - start_date: %s - end_date: %s', config['start_date'], end_date)

    dt = datetime.utcnow()
    # truncate based on time bucket
    end_time = dt
    if time_bucket == 'day': 
        end_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif time_bucket == 'hour': 
        end_time = dt.replace(minute=0, second=0, microsecond=0)
        
    # if we have an end_date use it
    if end_date:
        end_time = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
        
    max_lookback_date = (end_time + relativedelta(days=-max_lookback_days)).strftime('%Y-%m-%dT%H:%M:%S')
    from_time = modified_since if modified_since > max_lookback_date else max_lookback_date
    
    to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
        
    time_query = "("
    if from_time:
        time_query += time_property + " > toDate('" + from_time + "')"
        if to_time:
            time_query += " AND "
        
    if to_time:
        time_query += time_property + " <= toDate('" + to_time + "')"
    time_query += ")"
    
    LOGGER.info('Getting records since %s to %s.', modified_since, to_time)
    full_query = q.format(time_query=time_query)
            
    LOGGER.info('Syncing query "%s" with params %s.', full_query, params)
    
    records = tinybird.get_records(config, full_query, params, limit=100000)

    records_synced = 0
    max_time = "0"

    for record in records:
        # record biggest time property value to bookmark it
        max_time = max(max_time, record[time_property])

        with Transformer() as transformer:
            to_write = transformer.transform(record, stream['schema'], metadata.to_map(stream['metadata']))

        write_record(table_name, to_write)
        records_synced += 1

    state = write_bookmark(state, table_name, 'modified_since', max_time)
    write_state(state)
    LOGGER.info('Wrote state with modified_since=%s', max_time)
  
    LOGGER.info('Wrote %d records for table "%s".', records_synced, table_name)

    return records_synced


def sync_stream_incremental(config: Dict, state: Dict, table_spec: Dict, stream: Dict) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    table_name = table_spec.get("table_name") 
    time_property = table_spec.get('time_property')
    time_bucket = table_spec.get('time_bucket')
    q = table_spec.get('query')
    params = table_spec.get('params')
    
    LOGGER.info('Syncing table "%s".', table_name)
    
    modified_since = get_bookmark(state, table_name, 'modified_since') or config['start_date']
    end_date = config.get('end_date')
    max_lookback_days = table_spec.get("max_lookback_days") or 90

    LOGGER.info('Config info - start_date: %s - end_date: %s', config['start_date'], end_date)

    dt = datetime.utcnow()
    # truncate based on time bucket
    end_time = dt
    if time_bucket == 'day': 
        end_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif time_bucket == 'hour': 
        end_time = dt.replace(minute=0, second=0, microsecond=0)
        
    # if we have an end_date use it
    if end_date:
        end_time = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
        
    max_lookback_date = (end_time + relativedelta(days=-max_lookback_days)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    from_time = modified_since if modified_since > max_lookback_date else max_lookback_date
    
    to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    total_records_synced = 0
    records_synced = 1 # greater than 0 to enter the loop
    loop_count = 0
    #only loop if we have no time_bucket can correctly pick up where we left off
    while records_synced > 0:
        
        time_query = "("
        if from_time:
            time_query += time_property + " > toDate('" + from_time + "')"
            if to_time:
                time_query += " AND "
            
        if to_time:
            time_query += time_property + " <= toDate('" + to_time + "')"
        time_query += ")"
        
        LOGGER.info('Getting records since %s to %s.', modified_since, to_time)
        full_query = q.format(time_query=time_query)
                
        LOGGER.info('Syncing query "%s" with params %s.', full_query, params)
        
        records = tinybird.get_records(config, full_query, params, limit=100000)

        records_synced = 0
        max_time = "0"
        loop_count += 1

        for record in records:
            # record biggest time property value to bookmark it
            max_time = max(max_time, record[time_property])

            with Transformer() as transformer:
                to_write = transformer.transform(record, stream['schema'], metadata.to_map(stream['metadata']))

            write_record(table_name, to_write)
            records_synced += 1

        state = write_bookmark(state, table_name, 'modified_since', max_time)
        write_state(state)
        LOGGER.info('Wrote state with modified_since=%s', max_time)

        total_records_synced += records_synced
        from_time = max_time
        
        LOGGER.info('Wrote %d/%d records for table "%s". Loop count: %d.', records_synced, total_records_synced, table_name, loop_count)

    return total_records_synced

