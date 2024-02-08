# tap-tinybird

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Tinybird](http://tinybird.co)
- Extracts the aggregated data based on a query 
- Outputs the schema for each table
- Incrementally pulls data based on the input state

## Config

*config.json*
```json
 {
    "tinybird_access_token": "ACCESS_TOKEN",
    "tinybird_api_url": "https://api.tinybird.co",
    "start_date": "2024-01-01T00:00:00",
    // "end_date": "2024-02-07T00:00:00",
    "tables": [{
        "table_name": "my_table",
        "params": "orgId=aaaaaaaa-bbbb-cccc-dddd-eeeeffffgggg",
        // it is important to put  "{time_query}" in the query if you want the query to be incremental
        // this will be automatically replace with the proper condition on time_property
        // by default we also collect all the data until 5 minutes ago to leave a buffer for data to going through a pipeline appropriately
        "query": "SELECT day, orgId, sdk, sdkVersion, count() as count FROM my_materialized_view_v1 WHERE {time_query} GROUP BY day, orgId, sdk, sdkVersion",
        "keys": ["orgId", "environmentId", "sdk", "sdkVersion"],
        "time_property": "day",
        "max_lookback_days": 3
    }]
}
```
