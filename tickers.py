import json
import datetime
import os
import traceback

import requests
from functions import process_list_concurrently, load_module_config, combine_jsons, execute_update, execute_query
from history import TickerHistory


def load_ticker_details_concurrently(tickers):
    module_config = load_module_config("market_scanner")
    for i in range(0, len(tickers)):
        print(f"Loading details for ticker {i}/{len(tickers)}")
        detail_url = f"{module_config['api_endpoint']}/v3/reference/tickers/{tickers[i]['symbol']}?apiKey={module_config['api_key']}"
        r = requests.get(detail_url)
        raw_data = json.loads(r.text)
        # pass
        if 'sic_description' in raw_data['results']:
            tickers[i]['sector'] = raw_data['results']['sic_description']
        if 'description' in raw_data['results']:

            tickers[i]['description'] = raw_data['results']['description']

    with open(f"output/{os.getpid()}tickers.json", "w") as f:
        f.write(json.dumps(tickers))
def load_nyse_tickers_cached(module_config):
    with open("data/ticker_data_complete.json") as f:
        return json.loads(f.read())
def load_nyse_tickers(module_config):

    # ok so the idea here is start at 31 days DTE and look forward
    now = datetime.datetime.now()
    ticker_url = f"{module_config['api_endpoint']}/v3/reference/tickers?apiKey={module_config['api_key']}&type=CS&market=stocks&active=true&limit=1000"
    r = requests.get(ticker_url)
    raw_data = json.loads(r.text)
    tickers =  [{'symbol':  x['ticker'], 'name': x['name']} for x in raw_data['results']]
    # for entry  in raw_data['results']:
    #     pass
    while 'next_url' in raw_data:
        r = requests.get(f"{raw_data['next_url']}&apiKey={module_config['api_key']}")
        raw_data = json.loads(r.text)
        if len(raw_data['results']) == 0:
            break
        for x in raw_data['results']:
            tickers.append({'symbol': x['ticker'], 'name': x['name']})
    pids = process_list_concurrently(tickers, load_ticker_details_concurrently, int(len(tickers)/16)+1)
    combined = combine_jsons([f"output/{x}tickers.json" for x in pids])
    with open("data/ticker_data_complete.json", "w") as f:
        f.write(json.dumps(combined))
    #ok so now we need to load the sectors and descriotiobs
    # pass



    pass


def write_tickers_to_db(connection, tickers, module_config):
    for ticker in tickers:
        if len(execute_query(connection, f"select * from tickers_ticker where symbol='{ticker['symbol']}' limit 10", verbose=False)) > 1:
            continue
        name = ticker['name'].replace("'", "")
        if 'sector' in ticker and 'description' in ticker:
            description = ticker['description'].replace("'", "")
            sector = ticker['sector'].replace("'", "")
            ticker_sql = f"REPLACE INTO tickers_ticker (symbol, name, sector, description) VALUES ('{ticker['symbol']}', '{name}', '{sector}', '{description}')"
        else:
            ticker_sql = f"REPLACE INTO tickers_ticker (symbol, name, sector, description) VALUES ('{ticker['symbol']}', '{name}', '', '')"
        # ticker_sql = f"REPLACE INTO tickers_ticker (symbol, name, sector, description) VALUES ('A', '', '', '{ticker['description']}')"

        try:
            execute_update(connection, ticker_sql,auto_commit=True, verbose=False)
            print(f"Wrote entry to tickers_ticker for {tickers.index(ticker)}/{len(tickers)}")
        except:
            # traceback.print_exc()
            print(ticker_sql)
        pass

def load_ticker_symbol_by_id(connection,ticker_id, module_config):
    return execute_query(connection, f"select symbol from tickers_ticker where id='{ticker_id}'")[1][0]
def load_ticker_id_by_symbol(connection,symbol, module_config):
    return execute_query(connection, f"select id from tickers_ticker where id='{symbol}'")[1][0]

def load_ticker_last_updated(ticker, connection, module_config):
    # execute_update(connection, "lock tables history_tickerhistory read, tickers_ticker read", cache=False)
    ticker_last_updated =  int(execute_query(connection, f"select coalesce(max(timestamp), round(1000 * unix_timestamp(date_sub(now(), interval 365 day)))) from history_tickerhistory where ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'")[1][0])
    # connection.commit()
    # execute_update(connection, f"unlock tables")
    return ticker_last_updated

def load_ticker_history_by_id(connection, ticker_history_id, ticker, module_config):
    return TickerHistory(*[float(x) if '.' in x else int(x) for x in execute_query(connection, f"select open, close, high, low, volume, timestamp from history_tickerhistory where id='{ticker_history_id}' and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}') order by timestamp asc", verbose=False)[1]])
