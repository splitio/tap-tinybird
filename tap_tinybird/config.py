"""
Tap configuration related stuff
"""
from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema([{
    Required('table_name'): str,
    Optional('params'): str, 
    Required('query'): str,
    Required('keys'): [str],
    Optional('time_property'): str,
    Optional('max_lookback_days'): int,
    Optional('start_date'): str,
    Optional('end_date'): str,
}])
