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
    table_name = table_spec.get("table_name") 
    time_property = table_spec.get('time_property')
    q = table_spec.get('query')
    params = table_spec.get('params')
    
    LOGGER.info('Syncing table "%s".', table_name)
    
    if time_property:
        modified_since = get_bookmark(state, table_name, 'modified_since') or config['start_date']
        end_date = config.get('end_date')
        max_lookback_days = table_spec.get("max_lookback_days") or 90

        LOGGER.info('Config info - start_date: %s - end_date: %s', config['start_date'], end_date)
    
        # we stop at 5 minutes ago because data may not all be there yet in Sumo
        # if we do real time we would have gaps of data for the data that comes in later.
        dt = datetime.utcnow()
        # truncate to the start of current day day
        end_time = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_time = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
        max_lookback_date = (end_time + relativedelta(days=-max_lookback_days)).strftime('%Y-%m-%dT%H:%M:%S')
        from_time = modified_since if modified_since > max_lookback_date else max_lookback_date
        
        to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
            
        time_query = '('
        if from_time:
            time_query += time_property + ' > \'' + from_time + '\''
            if to_time:
                time_query += ' AND '
            
        if to_time:
            time_query += time_property + ' < \'' + to_time + '\''
        time_query += ')'
        
        q = q.format(time_query=time_query)
        
        LOGGER.info('Getting records since %s to %s.', modified_since, to_time)

    records_streamed = 0
    
    LOGGER.info('Syncing query "%s" with params %s.', q, params)

    # TODO need to get a pointer back to loop through records if more than one page
    records = tinybird.get_records(config, q, params, limit=1000000)

    records_synced = 0
    max_time = "0"
    

    for record in records:
        # record biggest time property value to bookmark it
        if time_property:
            max_time = max(max_time, record[time_property])

        with Transformer() as transformer:
            to_write = transformer.transform(record, stream['schema'], metadata.to_map(stream['metadata']))

        write_record(table_name, to_write)
        records_synced += 1

    if time_property:
        state = write_bookmark(state, table_name, 'modified_since', max_time)
        write_state(state)
        LOGGER.info('Wrote state with modified_since=%s', max_time)

    LOGGER.info('Wrote %s records for table "%s".', records_synced, table_name)

    return records_streamed

