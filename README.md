# influxdb-data-management

import and query temporal data to/from Influxdb

## initial config (required)

```python
from config import influx_bucket, influx_debug, influx_org, influx_token, influx_url
```

In order to works fine you must define all configuration variables. In this case i'm using configuration python file `config.py`:

```python
influx_url = "http://localhost:8086"
influx_debug = True
influx_org = "my-org"
influx_bucket = 'my-bucket'
influx_token = "my-token"
```

## verify written data

```flux
from(bucket: "veronacard")
    |> range(start: 2020-12-02T00:00:00Z)
```
