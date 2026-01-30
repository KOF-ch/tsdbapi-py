import base64
import threading
from queue import Queue

from werkzeug import Request
from werkzeug import Response
from werkzeug.serving import make_server
import webbrowser
from time import time
from datetime import date
import polars as pl

from requests_oauthlib import OAuth2Session
import os

# OAuth token storage
token = None

config = {
    "oauth_offline_token": os.getenv("TSDBAPI_OAUTH_OFFLINE_TOKEN", None),
    "oauth_client_id": os.getenv("TSDBAPI_OAUTH_CLIENT_ID", "tsdb-api"),
    "oauth_client_secret": os.getenv("TSDBAPI_OAUTH_CLIENT_SECRET", "TXUydEpTNERpeXk3eVRjZVFRbHhteEV3a3JGWGlid3c="),
    "oauth_token_url": os.getenv("TSDBAPI_OAUTH_TOKEN_URL", "https://keycloak.kof.ethz.ch/realms/main/protocol/openid-connect/token"),
    "oauth_auth_url": os.getenv("TSDBAPI_OAUTH_AUTH_URL", "https://keycloak.kof.ethz.ch/realms/main/protocol/openid-connect/auth"),
    "url_production": os.getenv("TSDBAPI_URL_PRODUCTION", "https://tsdb-api.kof.ethz.ch/v2/"),
    "url_staging": os.getenv("TSDBAPI_URL_STAGING", "https://tsdb-api.stage.kof.ethz.ch/v2/"),
    "url_test": os.getenv("TSDBAPI_URL_TEST", "http://localhost:3001/v2/"),
    "environment": os.getenv("TSDBAPI_ENVIRONMENT", "production"),
    "access_type": os.getenv("TSDBAPI_ACCESS_TYPE", "oauth"),
    "read_before_release": os.getenv("TSDBAPI_READ_BEFORE_RELEASE", True),
}

def _base_url():
    if config["environment"] == "production":
        return config["url_production"]
    elif config["environment"] == "staging":
        return config["url_staging"]
    elif config["environment"] == "test":
        return config["url_test"]
    else:
        raise ValueError(f"Unknown environment: {config['environment']}")
    
def _get_client_secret():
    return base64.b64decode(config["oauth_client_secret"]).decode("ascii")

def set_config(**kwargs: str) -> None:
    """Update module configuration.

    Args:
        **kwargs: Configuration values to set. Recognized keys:
            oauth_offline_token (str): Offline refresh token for non-interactive use.
            oauth_client_id (str): OAuth client identifier (default: "tsdb-api").
            oauth_client_secret (str): Base64-encoded OAuth client secret. Encode with base64.b64encode(your_client_secret.encode('ascii')).
            environment (str): Whether to use the production, staging or test API. Must be one of "production", "staging", "test".
            oauth_token_url (str): OAuth token URL.
            oauth_auth_url (str): OAuth authorization URL.
            url_production (str): Base URL of the production API.
            url_staging (str): Base URL of the staging API.
            url_test (str): Base URL of the test API.
            access_type (str): How to access time series data. Must be one of 'oauth' (the default), 'public' or 'preview'.
                The access types 'public' and 'preview' bypass authentication.
                Use the access type 'public' to read public time series and the access type 'preview' to read time series previews (latest 2 years of data missing).
                Use 'oauth' for authenticated access.
            read_before_release (bool): Whether to read time series vintages before their official release. Defaults to TRUE. This option will only have
                an effect if you have pre release access to the requested time series.

    """
    global config
    config = { **config, **kwargs }

def get_token():

    env_set = "OAUTHLIB_INSECURE_TRANSPORT" in os.environ
    # Enable redirect to loopback address (ok since HTTP request never leaves the device, see RFC 8252 section 8.3).
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    session = OAuth2Session(config["oauth_client_id"])
    url = _get_auth_code_url(session)
    token = session.fetch_token(config["oauth_token_url"],
                                client_secret=_get_client_secret(),
                                authorization_response=url)
    token["refresh_time"] = time()

    if not env_set:
        del os.environ["OAUTHLIB_INSECURE_TRANSPORT"]

    return token
 
def get_offline_token(set_config: bool = True) -> str:
    """Request an offline token.
    An offline token is a refresh token that does not expire.
    Use it in non-interactive sessions to avoid user login.
    The returned offline token must be treated like a secret.

    Args:
        set_config (bool, optional): If TRUE (default), the offline_token property in the module configuration will be set.
            That way, the module will use the offline token to retrieve access tokens.

    Returns:
        str: Offline token
    """
    session = OAuth2Session(config["oauth_client_id"])
    url = _get_auth_code_url(session)
    token = session.fetch_token(config["oauth_token_url"],
                                client_secret=_get_client_secret(),
                                authorization_response=url,
                                access_type="offline")
    return token["refresh_token"]

def _get_auth_code_url(session):

    @Request.application
    def app(request):
        q.put(request.url)
        return Response("Authentication complete. You can close this page now.", 200, content_type="text/plain")

    s = make_server("127.0.0.1", 0, app)
    session.redirect_uri = f"http://127.0.0.1:{s.port}/"
    authorization_url, state = session.authorization_url(config["oauth_auth_url"])
    print("Waiting for authentication in browser...")
    webbrowser.open(authorization_url)
    
    q = Queue()
    t = threading.Thread(target=s.serve_forever)
    t.start()
    print("waiting")
    url = q.get(block=True)
    s.shutdown()
    t.join()

    return url

def _refresh_token(refresh_token):
    global token
    session = OAuth2Session()
    token = session.refresh_token(
        config["oauth_token_url"],
        refresh_token=refresh_token,
        client_id=config["oauth_client_id"],
        client_secret=_get_client_secret()
    )
    token["refresh_time"] = time()

def _make_request(method, url, **kwargs):
    global token
    if token is None:
        if config["oauth_offline_token"] is not None:
            _refresh_token(config["oauth_offline_token"])
        else:
            token = get_token()
    else:
        if token["refresh_time"] + token["refresh_expires_in"] < time() + 10:
            token = get_token()
        if token["refresh_time"] + token["expires_in"] < time() + 10:
            _refresh_token(token["refresh_token"])

    session = OAuth2Session(config["oauth_client_id"], token=token)
    res = session.request(method, url, **kwargs)
    data = res.json()
    if not res.ok:
        print(res.status_code, res.reason)
        print(data["message"])
        raise RuntimeError(data["message"])
        
    return data

def _to_bool_query_param(value):
    "true" if value else ""

def read_ts(
        ts_keys: list[str],
        valid_on: date = date.today(),
        ignore_missing: bool = False) -> pl.DataFrame:
    """Read time series given by their unique identifiers (keys). The vintage is specified by the valid_on parameter.
    By default, the most recent vintage is read.

    Args:
        ts_keys (list[str]): Unique time series identifiers (keys)
        valid_on (date, optional): Selects the time series vintage with the vintage date equal to or before this date. Defaults to date.today().
        ignore_missing (bool, optional): Whether to ignore missing or forbidden time series when requesting time series data. Defaults to False.

    Returns:
        pl.DataFrame: Table with colunms ts_key, time, value
    """

    if isinstance(ts_keys, str):
        ts_keys = [ts_keys]
        
    data = _make_request(
        "GET", 
        _base_url() + "ts",
        params={
            "keys": ",".join(ts_keys),
            "df": "Y-m-d",
            "mime": "json",
            "valid_on": valid_on.strftime("%Y-%m-%d"),
            "ignore_missing": _to_bool_query_param(ignore_missing)
        }
    )

    return _ts_data_to_df(data)

def read_collection_ts(
        collection: str,
        owner: str = "self",
        valid_on: date = date.today(),
        ignore_missing: bool = False) -> pl.DataFrame:
    """Read the time series in a time series collection. The time series vintage is specified by the valid_on parameter.
    By default, the most recent vintage is read.

    Args:
        collection (str): Name of the time series collection
        owner (str, optional): Username of the owner of the time series collection. Defaults to "self".
        valid_on (date, optional): Selects the time series vintage with the vintage date equal to or before this date. Defaults to date.today().
        ignore_missing (bool, optional): Whether to ignore missing or forbidden time series when requesting time series data. Defaults to False.

    Returns:
        pl.DataFrame: Table with columns ts_key, time, value
    """
    data = _make_request(
        "GET", 
        _base_url() + f"collections/{owner}/{collection}/ts",
        params={
            "df": "Y-m-d",
            "mime": "json",
            "valid_on": valid_on.strftime("%Y-%m-%d"),
            "ignore_missing": _to_bool_query_param(ignore_missing)
        }
    )

    return _ts_data_to_df(data)

def read_ts_metadata(
        ts_keys: list[str],
        locale: str = None,
        ignore_missing: bool = False) -> pl.DataFrame:
    """Read the time series metadata of a particular locale.

    Args:
        ts_keys (list[str]): Unique time series identifiers (keys)
        locale (str, optional): The locale of the metadata. 
            Can be any string, but ISO codes are recommended (such as 'en', 'de', 'fr', 'it').
            Set to None for unlocalized metadata (default).
        ignore_missing (bool, optional): Whether to ignore missing or forbidden time series when requesting time series metadata. Defaults to False.

    Returns:
        pl.DataFrame: Table with columns ts_key, key, value
    """

    if isinstance(ts_keys, str):
        ts_keys = [ts_keys]
    
    data = _make_request(
        "GET", 
        _base_url() + "ts/metadata",
        params={
            "keys": ",".join(ts_keys),
            "locale": locale,
            "ignore_missing": _to_bool_query_param(ignore_missing) 
        }
    )

    return _ts_metadata_to_df(data)

def _ts_metadata_to_df(data):
    df_schema = {
        "ts_key": pl.String,
        "key": pl.String,
        "value": pl.String
    }
    dfs = []
    for elem in data:
        dfs.append(
            pl.DataFrame(
                {"ts_key": elem, "key": data[elem].keys(), "value": data[elem].values()},
                schema=df_schema,
            )
        )
    if len(dfs) == 0:
        return pl.DataFrame(
            {"ts_key": [], "key": [], "value": []},
            schema=df_schema,
        )
    return pl.concat(dfs)

def _ts_data_to_df(data):
    df_schema = {
        "ts_key": pl.String,
        "time": pl.Date,
        "value": pl.Float32
    }
    dfs = []
    for elem in data:
        dfs.append(
            pl.DataFrame(
                {"ts_key": elem["ts_key"], "time": elem["time"], "value": elem["value"]},
                schema=df_schema,
            )
        )
    if len(dfs) == 0:
        return pl.DataFrame(
            {"ts_key": [], "time": [], "value": []},
            schema=df_schema,
        )
    return pl.concat(dfs)