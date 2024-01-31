# tap-sumologic

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Tinybird](http://tinybird.co)
- Extracts the aggregated data based on a query 
- Outputs the schema for each table
- [TODO] Incrementally pulls data based on the input state

## Config

*config.json*
```json
 {
    "tinybird_access_token": "ACCESS_TOKEN",
    "tinybird_api_url": "https://api.split.tinybird.co",
    "tables": [{
        "table_name": "my_table",
        "params": "orgId=aaaaaaaa-bbbb-cccc-dddd-eeeeffffgggg",
        "query": "SELECT orgId, environmentId, sdk, sdkVersion, count() as count FROM sdk_usage__v2 group by orgId, environmentId, sdk, sdkVersion",
        "keys": ["orgId", "environmentId", "sdk", "sdkVersion"]
    }]
}
```
