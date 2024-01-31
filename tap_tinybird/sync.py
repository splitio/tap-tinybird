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
    #modified_since = get_bookmark(state, table_name, 'modified_since') or config['start_date']
    #end_date = config.get('end_date')

    #LOGGER.info('Config info - start_date: %s - end_date: %s', config['start_date'], end_date)
    LOGGER.info('Syncing table "%s".', table_name)

    #max_lookback_days = table_spec.get("max_lookback_days") or 7
    # we stop at 5 minutes ago because data may not all be there yet in Sumo
    # if we do real time we would have gaps of data for the data that comes in later.
    #end_time = datetime.utcnow() + relativedelta(minutes=-5)
    #if end_date:
    #    end_time = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
    #max_lookback_date = (end_time + relativedelta(days=-max_lookback_days)).strftime('%Y-%m-%dT%H:%M:%S')
    #from_time = modified_since if modified_since > max_lookback_date else max_lookback_date
    
    #to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    #time_zone = 'UTC'
    
    #LOGGER.info('Getting records since %s to %s.', from_time, to_time)

    q = table_spec.get('query')
    params = table_spec.get('params')
    records_streamed = 0

    LOGGER.info('Syncing query "%s" with params %s.', q, params)

    # TODO need to get a pointer back to loop through records if more than one page
    records = tinybird.get_records(config, q, params, limit=1000000)

    records_synced = 0
    #max_time = "0"
    #time_property = table_spec.get('time_property')

    for record in records:
        # record biggest time property value to bookmark it
        #if time_property:
        #    max_time = max(max_time, record[time_property])

        with Transformer() as transformer:
            to_write = transformer.transform(record, stream['schema'], metadata.to_map(stream['metadata']))

        write_record(table_name, to_write)
        records_synced += 1

    #if time_property:
    #    end = datetime.utcfromtimestamp(int(max_time)/1000)
    #    state = write_bookmark(state, table_name, 'modified_since', end.strftime('%Y-%m-%dT%H:%M:%S'))
    #    write_state(state)
    #    LOGGER.info('Wrote state with modified_since=%s', end.strftime('%Y-%m-%dT%H:%M:%S'))

    LOGGER.info('Wrote %s records for table "%s".', records_synced, table_name)

    return records_streamed

