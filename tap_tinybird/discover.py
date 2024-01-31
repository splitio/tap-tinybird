"""
Discovery mode is connecting to the data source and collecting information that is required for running the tap.
"""
from typing import List, Dict

import singer
from singer import metadata

from tap_tinybird import tinybird 

LOGGER = singer.get_logger()

def discover_streams(config: Dict)-> List[Dict]:
    """
    Run discovery mode for every stream in the tap configuration
    :param config: connection and streams configuration
    :return: list of information  about every stream
    """
    streams = []

    for table_spec in config['tables']:
        schema = discover_schema(config, table_spec)

        streams.append({'stream': table_spec['table_name'],
                        'tap_stream_id': table_spec['table_name'],
                        'schema': schema,
                        'metadata': load_metadata(table_spec, schema)
                        })
    return streams


def discover_schema(config: Dict, table_spec: Dict) -> Dict:
    """
    Detects the json schema of the given table/stream
    :param config: connection and streams configuration
    :param table_spec: table specs
    :return: detected schema
    """
    schema = tinybird.get_schema_for_table(config, table_spec)

    return schema


def load_metadata(table_spec: Dict, schema: Dict)-> List:
    """
    Creates metadata for the given stream using its specs and schema
    :param table_spec: stream/table specs
    :param schema: stream's json schema
    :return: metadata as a list
    """
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', schema.get('key_properties', []))
    
    for field_name in schema.get('properties', {}).keys():
        if field_name in schema.get('key_properties', []):
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)
