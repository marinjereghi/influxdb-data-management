from influxdb_client import InfluxDBClient
from config import influx_debug, influx_org, influx_token, influx_url, influx_bucket
import pandas as pd
from datetime import datetime

if __name__ == "__main__":
    startTime = datetime.now()

    _start = datetime.fromisoformat("2018-12-31T23:59:59.000")
    _stop = datetime.fromisoformat("2019-12-31T00:00:00.000")

    params = {
        "_bucket": influx_bucket,
        "_start": _start,
        "_stop": _stop
        # "_desc": True
    }

    '''
    for each card, get the next swipe time and elapsed time
    '''
    query2 = 'from(bucket: "veronacard1")'\
        '|> range(start: 2013-12-31T23:59:59Z, stop: 2014-05-31T23:59:59Z)'\
        '|> group(columns: ["_value"])'\
        '|> sort(columns: ["_time"])'\
        '|> elapsed()'

    query1 = 'from(bucket: "veronacard")'\
        '|> range(start: 2020-11-01T00:00:00Z, stop: 2020-11-03T00:00:00Z)'\
        '|> window(every: 24h, period: 24h)'\
        '|> group(columns: ["_measurement"])'\
        '|> count(column: "_field")'\
        '|> keep(columns: ["_time", "_value", "_measurement", "_field"])'

    '''
    for each day, for each point of interest get swiped cards
    '''
    query = 'from(bucket: _bucket)' \
        '|> range(start: _start, stop: _stop)' \
        '|> group(columns: ["_measurement"])'\
        '|> aggregateWindow(every: 24h, fn: count, column: "_value")'
    # '|> sort(columns:["_time"], desc:_desc)'
    # '|> keep(columns: ["_time", "_value", "_measurement"])'\

    client = InfluxDBClient(url=influx_url,
                            token=influx_token, org=influx_org, debug=influx_debug)
    # result = client.query_api().query(query=query)
    # data_frame_index=["_time"]
    result = client.query_api().query_data_frame(
        query=query, org=influx_org, params=params)

    # for df in result:
    # print(result)
    # if df.empty:
    #     print('empty df\t', df)
    # else:
    #     df.set_index(df["_time"])
    print(result.head(7))
    #result.sort_values('_time', inplace=True)
    result.to_csv('./_result_2019.csv', mode='a', sep=";", header=False)
    client.close()
    print(f'Export finished in: {datetime.now() - startTime}')
