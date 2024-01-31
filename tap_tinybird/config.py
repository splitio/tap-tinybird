"""
Tap configuration related stuff
"""
from voluptuous import Schema, Required, Optional

CONFIG_CONTRACT = Schema([{
    Required('table_name'): str,
    Required('params'): str, 
    Required('query'): str,
    Required('keys'): [str],
}])
