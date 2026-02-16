# Interact with the KOF Time Series Database API

The **tsdbapi** package is a Python wrapper for the [KOF Time Series DB API](https://tsdb-api.kof.ethz.ch/v2/docs). The functionality of the package includes reading time series data, metadata and release information from the KOF time series database. At the moment, the functionality of the package is quite limited (in contrast to the [R package](https://github.com/KOF-ch/tsdbapi-R)). If you miss a function for a particular API endpoint, check for updates regularly. The package functions convert the JSON data returned by the KOF Time Series DB API to either a Python dict or a [polars.DataFrame](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html).

## Installation
Install the package with
``` bash
pip install tsdbapi
```

## Basic Usage

Use the function `read_ts` to read time series from the KOF time series database. The code below reads the time series with the key **ch.kof.barometer** and returns it as a [polars.DataFrame](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html).
``` py
from tsdbapi import read_ts

read_ts(ts_keys="ch.kof.barometer")
```
Read multiple time series at the same time by providing a list of keys:
``` py
read_ts(ts_keys=["ch.kof.globalbaro.leading","ch.kof.globalbaro.leading"])
```

### Authorization

When running the code above, you will be redirected to KOF's identity provider (Keycloak), where you have to log in with your KOF credentials (unless you are already logged in). If you do not have a KOF account, you can still access the public time series in the KOF time series database by setting the access_type to **public**:
``` py
from tsdbapi import read_ts, set_config

set_config(access_type = "public")
read_ts("ch.kof.barometer")
```
The time series **ch.kof.barometer** is a public time series.

If you want to avoid user login, for example in a non-interactive session, you must use an offline token. An offline token is a refresh token that does not expire and can be used to retrieve access tokens. Request an offline token with:
``` py
from tsdbapi import get_offline_token

mytoken = get_offline_token()
print(mytoken)
```
The returned offline token must be treated like a secret!

Use the offline token by setting the environment variable `TSDBAPI_OAUTH_OFFLINE_TOKEN` before running a Python script. Alternatively, you can set the corresponding package configuration option:
``` py
from tsdbapi import read_ts, set_config

set_config(oauth_offline_token="mytoken")
read_ts(ts_keys="ch.kof.barometer")
```

### Vintages

Every time series can have multiple vintages (or versions). A time series vintage is based on the data available at its **vintage date**.

By default, `read_ts` returns the most recent vintage (or version) of the time series. 
To specify a different vintage, use the `valid_on` parameter. The code below reads the KOF barometer vintage based on the data available at January 15, 2026.
``` py
from tsdbapi import read_collection_ts
from datetime import date

read_ts("ch.kof.barometer", valid_on = date(2026, 1, 15))
```
For users with role **extern** (everyone not employed at KOF), a time series vintage is only visible once it has been officially released, hence its data, including its vintage date, can only be read after release. A time series vintage is usually released several days after its vintage date.

### Release information

The release information, including the release time, of the most recent time series vintage can be read with
``` py
from tsdbapi import read_ts_release

read_ts_release(ts_keys="ch.kof.barometer")
```
Use the `valid_on` parameter to specify a different vintage. For users with the role **extern**, only vintages that have been released are visible. However, the release information of future, yet to be released time series vintages can be read with
``` py
from tsdbapi import read_ts_release_future

read_ts_release_future(ts_keys="ch.kof.barometer")
```
Note that he release times of future vintages are not guaranteed and are subject to change (although changes are rare).

### Download Quota

If you are a KOF data service subscriber, the number of time series downloads (reads) per year is limited by a quota. You can check your annual download quota and the number of time series downloads remaining in the current subscription year with
``` py
from tsdbapi import read_user_quota

read_user_quota()
```

### Collections
To read an entire collection of time series use
``` py
from tsdbapi import read_collection_ts

read_collection_ts(collection="bs_indicator", owner="public")
```
Every time series collection has an owner. By default, the owner is assumed to be yourself (`owner="self"`).

You can list all collections visible to you with
``` py
from tsdbapi import read_collection_ts

list_collections()
```
For users with the role **extern**, the includes all collections owned by the user himself and by the user **public**.

### Metadata

The metadata of one or multiple time series can be read with
``` py
from tsdbapi import read_metadata_ts

read_ts_metadata(ts_keys="ch.kof.barometer")
```
To read the metadata of an entire collection of time series:
``` py
from tsdbapi import read_collection_ts_metadata

read_collection_ts_metadata(collection="bs_indicator", owner="public")
```

## Advanced usage

Consult the package help for a detailed documentation of all available functions.