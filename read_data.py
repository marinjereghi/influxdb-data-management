from influxdb_client import InfluxDBClient
from config import influx_debug, influx_org, influx_token, influx_url
import pandas as pd
from datetime import datetime

if __name__ == "__main__":
    startTime = datetime.now()

    '''
    for each card, get the next swipe time and elapsed time
    '''
    query2 = 'from(bucket: "veronacard1")'\
        '|> range(start: 2020-12-01T00:00:00Z, stop: 2020-12-01T23:59:59Z)'\
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
    query = 'from(bucket:"veronacard1")' \
        '|> range(start: 2020-12-01T00:00:00Z, stop: 2020-12-03T00:00:00Z) ' \
        '|> keep(columns: ["_time", "_value", "_measurement", "_field", "activation_date", "profile"])'\
        '|> group(columns: ["_measurement"])'\
        '|> aggregateWindow(every: 24h, fn: count, column: "_value")'

    client = InfluxDBClient(url=influx_url,
                            token=influx_token, org=influx_org, debug=influx_debug)
    #result = client.query_api().query(query=query2)
    result = client.query_api().query_data_frame(
        query=query2, org=influx_org)
    # for table in result:
    #     for data in table.records:
    #         print(data["elapsed"])
    #         print(data["table"], data["_measurement"],
    #               data["_value"], data["_time"])
    for df in result:
        if df.empty:
            print('empty df\t', df)
        else:
            df.set_index(df["_time"])
            df.to_csv('./out.csv', mode='a', sep=";", header=False)

    print(f'Import finished in: {datetime.now() - startTime}')
