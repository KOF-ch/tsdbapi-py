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
from oauthlib.oauth2 import TokenExpiredError 
import os
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
token = None

config = {
    "offline_token": os.getenv("TSDBAPI_OFFLINE_TOKEN", None),
    "redirect_uri": os.getenv("TSDBAPI_OAUTH_REDIRECT_URI","http://127.0.0.1:1011"),
    "oauth_client_id": "tsdb-api",
    "oauth_client_secret": "Mu2tJS4Diyy7yTceQQlxmxEwkrFXibww",
    "oauth_token_url": "https://keycloak.kof.ethz.ch/realms/main/protocol/openid-connect/token",
    "oauth_auth_url": "https://keycloak.kof.ethz.ch/realms/main/protocol/openid-connect/auth",
    "base_url": "http://localhost:3001/"
}

def set_config(**kwargs):
    global config
    config = { **config, **kwargs }

def get_token():
    session = OAuth2Session(config['oauth_client_id'], redirect_uri=config['redirect_uri'])
    url = _get_auth_code_url(session)
    token = session.fetch_token(oauth_token_url, client_secret=oauth_client_secret, authorization_response=url)
    token['refresh_time'] = time()
    return token
 
def get_offline_token():
    session = OAuth2Session(oauth_client_id, redirect_uri=redirect_uri)
    url = _get_auth_code_url(session)
    token = session.fetch_token(oauth_token_url, client_secret=oauth_client_secret, authorization_response=url, access_type='offline')
    return token['refresh_token']

def _get_auth_code_url(session):
    
    @Request.application
    def app(request):
        q.put(request.url)
        return Response("Authentication complete. You can close this page now.", 200, content_type="text/plain")

    authorization_url, state = session.authorization_url(oauth_auth_url)
    print('Waiting for authentication in browser...')
    webbrowser.open(authorization_url)

    env_set = "OAUTHLIB_INSECURE_TRANSPORT" in os.environ
    # Enable redirect to loopback address (ok since HTTP request never leaves the device, see RFC 8252 section 8.3).
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    
    q = Queue()
    s = make_server("127.0.0.1", 1011, app)
    t = threading.Thread(target=s.serve_forever)
    t.start()
    print("waiting")
    url = q.get(block=True)
    s.shutdown()
    t.join()
    
    if not env_set:
        del os.environ['OAUTHLIB_INSECURE_TRANSPORT']

    return url

def refresh_token(refresh_token):
    global token
    session = OAuth2Session()
    token = session.refresh_token(
        oauth_token_url,
        refresh_token=refresh_token,
        client_id=oauth_client_id,
        client_secret=oauth_client_secret)
    token['refresh_time'] = time()

def make_request(method, url, **kwargs):
    global token
    if token is None:
        if offline_token is not None:
            refresh_token(offline_token)
        else:
            token = get_token()
    else:
        if token['refresh_time'] + token['refresh_expires_in'] < time() + 10:
            token = get_token()
        if token['refresh_time'] + token['expires_in'] < time() + 10:
            refresh_token(token['refresh_token'])

    session = OAuth2Session(oauth_client_id, token=token)
    res = session.request(method, url, **kwargs)
    data = res.json()
    if not res.ok:
        print(res.status_code, res.reason)
        print(data["message"])
        raise RuntimeError(data["message"])
        
    return data

def to_bool_query_param(value):
    "true" if value else ""

def read_ts(ts_keys, valid_on = date.today(), ignore_missing = False):
    data = make_request(
        "GET", 
        base_url + "ts",
        params={
            "keys": ",".join(ts_keys),
            "df": "Y-m-d",
            "mime": "json",
            "valid_on": valid_on.strftime("%Y-%m-%d"),
            "ignore_missing": to_bool_query_param(ignore_missing)
        }
    )

    return _ts_data_to_df(data)

def read_collection_ts(collection, owner, valid_on = date.today(), ignore_missing = False):
    data = make_request(
        "GET", 
        base_url + f"collections/{owner}/{collection}/ts",
        params={
            "df": "Y-m-d",
            "mime": "json",
            "valid_on": valid_on.strftime("%Y-%m-%d"),
            "ignore_missing": to_bool_query_param(ignore_missing)
        }
    )

    return _ts_data_to_df(data)

def read_ts_metadata(ts_keys, locale = "de", ignore_missing = False):
    data = make_request(
        "GET", 
        base_url + "ts/metadata",
        params={
            "keys": ",".join(ts_keys),
            "locale": locale,
            "ignore_missing": to_bool_query_param(ignore_missing) 
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
                {"ts_key": elem["ts_key"], "key": elem['ts_metadata'].keys(), "value": elem['ts_metadata'].values()},
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