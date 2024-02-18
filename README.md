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
    // "end_date": "2024-02-07T00:00:00", // optional: by default the end_date is now (or the end of the last closest bucket)
    "tables": [{
        "table_name": "my_table",  // table name in the destination
        "params": "orgId=aaaaaaaa-bbbb-cccc-dddd-eeeeffffgggg", // parameters to be added on the URL to query for the data
        // it is important to put  "{time_query}" in the query if you want the query to be incremental
        // this will be automatically replace with the proper condition on time_property
        "query": "SELECT day, orgId, sdk, sdkVersion, count() as count FROM my_materialized_view_v1 WHERE {time_query} GROUP BY day, orgId, sdk, sdkVersion",
        "keys": ["date". "orgId", "environmentId", "sdk", "sdkVersion"],
        "time_property": "date",
        // When time_bucket to none, it loads data incrementally using the time_property so make sure to order the results
        // When time_bucket is set to none, it also extract data by chunk of 100,000 and continue extracting data iteratively 
        // When time_bucket is set to day or hour, it limits results at 100,000 but don't iterate, so make sure your queries don't go over the limit.
        "time_bucket": "day",  // support value of 'day', 'hour' or 'none'. By default assumes 'none'. 
        "max_lookback_days": 3 // indicate how far behind the end date it can go
    }]
}
```
