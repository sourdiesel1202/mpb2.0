import json
import multiprocessing
import os
import time, polygon
import traceback

import mibian
import requests
from polygon import OptionSymbol
from polygon.rest import RESTClient

# from db_functions import combine_db_update_files, execute_bulk_update_file
from enums import OrderType, PositionType
import datetime,io
from zoneinfo import ZoneInfo
from functions import generate_csv_string, read_csv, write_csv, delete_csv, get_today, timestamp_to_datetime, \
    human_readable_datetime, execute_query, execute_update, obtain_db_connection, process_list_concurrently
import pandas as pd
# from indicators import process_ticker_alerts
# from validation import process_ticker_validation
# from db_functions import process_ticker_history
import polygon
from stockstats import wrap

# from tickers import load_ticker_id_by_symbol


# today =datetime.datetime.now().strftime("%Y-%m-%d")

class TickerHistory:
    open = 0
    close = 0
    high = 0
    low = 0
    volume = 0
    timestamp = 0
    dt = None
    # db_id = None
    def __init__(self, open, close, high, low, volume, timestamp):
        self.open = round(float(open), ndigits=2)
        self.close = round(float(close), ndigits=2)
        self.high = round(float(high), ndigits=2)
        self.low = round(float(low), ndigits=2)
        self.volume = volume
        self.timestamp =timestamp
        self.dt = timestamp_to_datetime(timestamp)

class ContractHistory(TickerHistory):
    implied_volatility = 0.0
    delta = 0.0
    theta = 0.0
    gamma = 0.0
    rho = 0.0
    underlying_close = 0.0
    contract = ''
    def __init__(self,contract,underlying_close, open, close, high, low, volume, timestamp):

        super().__init__( open, close, high, low, volume, timestamp)
        self.contract = contract
        self.underlying_close = underlying_close
        self.calculate_greeks()
        #ok so once we've called the constructor, we can go ahead and set our  greeks

    def calculate_greeks(self):
        if len(self.contract) == 0:
            raise  Exception("Cannot calculate greeks for a ContractHistory with no contract attribute specified")
        # contract_details = polygon.parse_option_symbol(self.contract)
        option_symbol = OptionSymbol(self.contract)
        dte = (datetime.datetime(*option_symbol.expiry.timetuple()[:-4]) - datetime.datetime.now()).days
        position_type = PositionType.LONG if option_symbol.call_or_put.upper() == PositionType.LONG_OPTION[0].upper() else PositionType.SHORT
        if position_type == PositionType.LONG:
            _tmp_iv = mibian.BS([self.underlying_close, float(option_symbol.strike_price), 0, dte],
                                callPrice=self.close).impliedVolatility

        else:
            _tmp_iv = mibian.BS([self.underlying_close, float(option_symbol.strike_price), 0, dte],
                                putPrice=self.close).impliedVolatility
        self.implied_volatility = _tmp_iv
        greeks = mibian.BS([self.underlying_close, float(option_symbol.strike_price), 0, dte],
                           volatility=_tmp_iv)
        if position_type == PositionType.LONG:
            self.theta = greeks.callTheta
            self.delta = greeks.callDelta
            self.gamma = greeks.gamma
            self.rho = greeks.callRho
        else:
            self.theta = greeks.putTheta
            self.delta = greeks.putDelta
            self.gamma = greeks.gamma
            self.rho = greeks.putRho

def write_ticker_history_db_entries(connection, ticker, ticker_history, module_config, cache=True, auto_commit=True):


        # write_ticker_history_db_entry(connection,ticker, th, module_config)
    #ok so dumb but before we run this let's do a select
    # ticker_id = execute_query(connection, f"select id from tickers_ticker where symbol='{ticker}'")[1][0]
    # if len(execute_query(connection, f"select * from history_tickerhistory where timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}')", verbose=False)) > 0:

        # for values in values_entries:\
    #ok i don't really believe this will work but let's try it
    # execute_update(connection, "lock tables history_tickerhistory write, tickers_ticker read ")
    # execute_update(connection, "start transaction")
        if len(ticker_history) > 0:
            # ticker_id = execute_query(connection, f"select id from tickers_ticker where symbol='{ticker}'")[1][0]
            values_entries = []
            for th in ticker_history:
                values_entries.append(f"((select id from tickers_ticker where symbol='{ticker}'), {th.open}, {th.close}, {th.high}, {th.low}, {th.volume},{th.timestamp},'{module_config['timespan']}','{module_config['timespan_multiplier']}')")
            history_sql = f"INSERT ignore INTO history_tickerhistory ( ticker_id,open, close, high, low, volume, timestamp, timespan, timespan_multiplier) VALUES {','.join(values_entries)}"
            # def process_ticker_history(connection, ticker,ticker_history, module_config, validate=True, process_alerts=True):
            #  process_ticker_history(connection, ticker,ticker_history, module_config, validate=False, process_alerts=True)
            # history_sql = f"INSERT ignore INTO history_tickerhistory ( ticker_id,open, close, high, low, volume, timestamp, timespan, timespan_multiplier) VALUES {values}"
            execute_update(connection,history_sql,verbose=False, auto_commit=auto_commit, cache=cache)

        # connection.commit()
    # execute_update(connection, "unlock tables")
    # execute_update(connection, "commit")
    # connection.commit()


def convert_ticker_history_to_csv(ticker, ticker_history):
    rows = [['o','c','h','l','v','t']]
    for history in ticker_history:
        rows.append([history.open, history.close, history.high, history.low, history.volume, history.timestamp])
    return rows

def  load_ticker_history_cached(ticker,module_config):
    '''
    Load from local cache (data/output/*)
    basically this should speed up the process a lot
    MUST call build_ticker_cache_first
    :param ticker:
    :param module_config:
    :return:
    '''
    ticker_history = []

    for entry in read_csv(
            f"{module_config['output_dir']}cached/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}.csv")[1:]:
        ticker_history.append(TickerHistory(*[float(x) if '.' in x else int(x) for x in entry]))

    return ticker_history
def load_ticker_history_db(ticker,module_config, connection=None):
    # ticker_history = []

    # if connection is None:
    # else:
    records =execute_query(connection, f"select open, close, high, low, volume, timestamp from history_tickerhistory where timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}') order by timestamp asc")
    ticker_history =[TickerHistory(*[float(x) if '.' in x else int(x) for x in records[i]]) for i in range(1, len(records))]

    if module_config['test_mode']:
        if module_config['test_use_test_time']:
            # print(f"using test time")
            # rn make this work with the hours only
            for i in range(0, len(ticker_history)):
                if timestamp_to_datetime(ticker_history[-i].timestamp).hour == module_config['test_time']:
                    return ticker_history[:-i + 1]
                    # break

    return ticker_history
def clear_ticker_history_cache(module_config):
    os.system (f" rm -rf {module_config['output_dir']}cached")
    os.mkdir(f"{module_config['output_dir']}cached/")
def clear_ticker_history_cache_entry(ticker, module_config):
    os.system(f"rm {module_config['output_dir']}cached/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}.csv")
    # os.mkdir(f"{module_config['output_dir']}cached/")

def dump_ticker_cache_entries(kwarg):
    '''
    Ok going to try something different here so we can be multi threaded
    pass in a dict with the data we need to run as a single process
    :param kwarg:
    :return:
    '''

    #ok so first things first let's load the db connection
    connection = obtain_db_connection(kwarg['module_config'])
    try:
        for ticker in kwarg['tickers']:
            dump_ticker_cache_entry(connection, ticker, kwarg['module_config'])
            print(f"Dumped ticker cache for {kwarg['tickers'].index(ticker)+1}/{len(kwarg['tickers'])}")

        connection.close()
    except Exception as e:
        connection.close()
        print(f"Cannot dump ticker cache for {len(kwarg['tickers'])} tickers: {traceback.format_exc()}" )
        raise e



def dump_ticker_cache_entry(connection, ticker, module_config):
    records = execute_query(connection,f"select open, close, high, low, volume, timestamp from history_tickerhistory where timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}') order by timestamp desc", verbose=False)
    # reversed(records)
    records = [records[0]]+[x for x in reversed(records[1:])]
    ticker_history = [TickerHistory(*[float(x) if '.' in x else int(x) for x in records[i]]) for i in range(1, len(records))]
    write_ticker_history_cached(ticker, ticker_history, module_config)
def dump_ticker_cache(module_config):
    # load_module_config()
    '''
    :param module_config:
    :return:
    '''
    connection=obtain_db_connection(module_config)
    try:
        if not module_config['test_mode'] or (module_config['test_mode'] and not module_config['test_use_input_tickers']):

            if module_config['test_mode']:
                if module_config['test_use_test_population']:
                    # tickers = read_csv(f"data/nyse.csv")[1:module_config['test_population_size']]
                    _tickers = [x[0] for x in execute_query(connection,"select distinct t.symbol from tickers_ticker t left join history_tickerhistory ht on t.id = ht.ticker_id where ht.id is not null")[1:module_config['test_population_size']]]
                else:
                    _tickers = [x[0] for x in execute_query(connection,"select distinct t.symbol from tickers_ticker t left join history_tickerhistory ht on t.id = ht.ticker_id where ht.id is not null")[1:]]
                # _tickers = [tickers[i][0] for i in range(0, len(tickers))]
                # tickers
            else:
                if module_config['test_use_input_tickers']:
                    _tickers = module_config['tickers']
                else:
                    _tickers = [x[0] for x in execute_query(connection,"select distinct t.symbol from tickers_ticker t left join history_tickerhistory ht on t.id = ht.ticker_id where ht.id is not null")[1:]]
                    # _tickers = [tickers[i][0] for i in range(0, len(tickers))]
        else:
            _tickers = module_config['tickers']
        connection.close()
    except Exception as e:
        connection.close()
        print(f"Cannot dump ticker cache : {traceback.format_exc()}" )
        raise e
    # for


    _keys = [x for x in _tickers]
    # n = int(module_config['num_processes']/12)+1
    n = int(len(_tickers)/module_config['num_processes'])+1
    loads = [{"tickers":_keys[i:i + n],"module_config":module_config} for i in range(0, len(_keys), n)]
    # for load in loads:
    #     load.insert(0, data[0])
    # for load in loads:
    #     print(f"Load size: {len(load)}")
    # return
    processes = {}
    for load in loads:
        # p = multiprocessing.Process(target=process_function, args=(load,))
        p = multiprocessing.Process(target=dump_ticker_cache_entries, args=(load,))
        p.start()

        processes[str(p.pid)] = p
    # pids = [x for x in processes.keys()]
    while any(processes[p].is_alive() for p in processes.keys()):
        # print(f"Waiting for {len([x for x in processes if x.is_alive()])} processes to complete. Going to sleep for 10 seconds")
        process_str = ','.join([str(v.pid) for v in processes.values() if v.is_alive()])
        print(f"The following child processes are still running: {process_str}")
        time.sleep(10)

    #ok so the idea with this one is that we dump a year worth of data to the cache
    pass
def load_ticker_history_raw(ticker,client, multiplier = 1, timespan = "hour", from_ = "2023-07-06", to = "2023-07-06", limit=500, module_config={}, cached=False, connection=None, single_timeframe=False):
    # ticker = ticker, multiplier = 1, timespan = "hour", from_ = today, to = today,
    # limit = 50000
    if timespan == 'hour' or timespan=='day':
        timespan = 'minute'
        multiplier = 30
        module_config['og_ts_multiplier'] = module_config['timespan_multiplier']
        # module_config['timespan_multiplier'] = multiplier
    if cached:
        return load_ticker_history_db(ticker, module_config)
    else:
        # if os.path.exists(f"{module_config['output_dir']}cached/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}.csv"):
        #     clear_ticker_history_cache_entry(ticker,module_config)
        minute_data =  []
        for entry in client.list_aggs(ticker=ticker,multiplier = multiplier, timespan = timespan, from_ = from_, to = to, limit=50000, sort='asc'):
            entry_date = datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
            # print(f"{entry_date}: {ticker}| Open: {entry.open} High: {entry.high} Low: {entry.low} Close: {entry.close} Volume: {entry.volume}")
            if (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern')).hour >= 9 if timespan =='minute' else 10) and (datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern')).hour <= 16 if timespan =='minute' else 15):
                if timespan == 'minute':
                    if (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern'))).hour == 9 and (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern'))).minute < 30:
                        continue
                    elif (datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))).hour >=16:
                        continue
                    else:
                        minute_data.append(TickerHistory(entry.open, entry.close, entry.high, entry.low, entry.volume,entry.timestamp))

                else:
                    minute_data.append(TickerHistory(entry.open, entry.close,entry.high, entry.low, entry.volume, entry.timestamp))

        if module_config['test_mode']:
            if module_config['test_use_test_time']:
                # print(f"using test time")
                #rn make this work with the hours only
                for i in range(0, len(minute_data)):
                    if timestamp_to_datetime(minute_data[-i].timestamp).hour == module_config['test_time']:
                        history_data = minute_data[:-i+1]
                        break
        if connection is not None:
            print(f"Writing DB history entries for ")
            # ??if not single_timeframe:
            if not single_timeframe or module_config['timespan'] =='minute':
                write_ticker_history_db_entries(connection, ticker, minute_data, {"timespan":timespan, "timespan_multiplier":multiplier})
            # connection.commit()
        if module_config['timespan'] == 'hour' or module_config['timespan'] == 'day':
            hour_data = normalize_history_data_for_hour(ticker, minute_data, module_config)
            module_config['timespan_multiplier'] = module_config['og_ts_multiplier']
            # if module_config['logging']:
            if module_config['timespan'] == 'day':
                day_data = normalize_history_data_for_day(ticker, hour_data, module_config)
                if connection is not None:
                    if not single_timeframe or module_config['timespan'] =='day':
                        write_ticker_history_db_entries(connection, ticker, day_data, module_config)
            else:
                if connection is not None:
                    write_ticker_history_db_entries(connection, ticker, hour_data, module_config)

        # write_ticker_history_cached(ticker, history_data, module_config)

        if module_config['timespan'] =='hour':
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Latest History Record ({module_config['timespan_multiplier']} {module_config['timespan']}): {datetime.datetime.fromtimestamp(hour_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Oldest History Record: {datetime.datetime.fromtimestamp(hour_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Total: {len(hour_data)}")

            return hour_data
        elif module_config['timespan']=='day':
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Latest History Record ({module_config['timespan_multiplier']} {module_config['timespan']}): {datetime.datetime.fromtimestamp(day_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Oldest History Record: {datetime.datetime.fromtimestamp(day_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Total: {len(day_data)}")
            return day_data
        else:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Latest History Record ({module_config['timespan_multiplier']} {module_config['timespan']}): {datetime.datetime.fromtimestamp(minute_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Oldest History Record: {datetime.datetime.fromtimestamp(minute_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Total: {len(minute_data)}")

            return minute_data

        # return history_data

def write_ticker_history_cached(ticker, ticker_history, module_config):
    write_csv(f"{module_config['output_dir']}cached/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}.csv",convert_ticker_history_to_csv(ticker, ticker_history))
def load_ticker_history_csv(ticker, ticker_history, convert_to_datetime=False, human_readable=False):

    # rows = load_ticker_history_raw(ticker,client,1, "hour", today,today,5000)
    rows = [['date', 'open', 'close', 'high', 'low', 'volume']]
    for entry in ticker_history:
        if convert_to_datetime:
            rows.append([timestamp_to_datetime(entry.timestamp) if not human_readable else human_readable_datetime(timestamp_to_datetime(entry.timestamp)) ,  entry.open, entry.close, entry.high, entry.low, entry.volume])
        else:
            rows.append([entry.timestamp, entry.open, entry.close, entry.high, entry.low, entry.volume])
    return  rows

def load_ticker_histories_data_frame(tickers, connection, module_config):
    '''
    Load multiple ticker histories into a multiindex dataframe
    :param ticker:
    :param ticker_history:
    :param convert_to_datetime:
    :param human_readable:
    :return:
    '''
    #ok so here we're going to generate the ticker data frame
    # _str = generate_csv_string(load_ticker_history_csv(ticker,ticker_history,convert_to_datetime=convert_to_datetime, human_readable=human_readable))
    # df = pd.read_csv(io.StringIO(_str), sep=",")
    # data = {'open': pd.Series([x.open for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history])}

    # data = {'open': pd.Series([x.open for x in ticker_history], index=[x.dt for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history],index=[x.dt for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history],index=[x.dt for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history],index=[x.dt for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history],index=[x.dt for x in ticker_history])}
    data={}
    for ticker in tickers:
        ticker_history = load_ticker_history_db(ticker, module_config,connection)
        data[ticker]={'open': pd.Series([x.open for x in ticker_history], index=[x.dt for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history],index=[x.dt for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history],index=[x.dt for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history],index=[x.dt for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history],index=[x.dt for x in ticker_history])}
    return pd.DataFrame({(outerKey, innerKey): values for outerKey, innerDict in data.items() for innerKey, values in innerDict.items()})
def convert_ticker_history_to_data_frame(ticker, ticker_history, convert_to_datetime=False, human_readable=False):
    '''
    A testament to a pain in the ass
    :param ticker:
    :param ticker_history:
    :param convert_to_datetime:
    :param human_readable:
    :return:
    '''
    #ok so here we're going to generate the ticker data frame
    # _str = generate_csv_string(load_ticker_history_csv(ticker,ticker_history,convert_to_datetime=convert_to_datetime, human_readable=human_readable))
    # df = pd.read_csv(io.StringIO(_str), sep=",")
    # data = {'open': pd.Series([x.open for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history])}

    # data = {'open': pd.Series([x.open for x in ticker_history], index=[x.dt for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history],index=[x.dt for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history],index=[x.dt for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history],index=[x.dt for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history],index=[x.dt for x in ticker_history])}
    data = {ticker:{'open': pd.Series([x.open for x in ticker_history], index=[x.dt for x in ticker_history]),'close': pd.Series([x.close for x in ticker_history],index=[x.dt for x in ticker_history]), 'high': pd.Series([x.high for x in ticker_history],index=[x.dt for x in ticker_history]),'low': pd.Series([x.low for x in ticker_history],index=[x.dt for x in ticker_history]), 'volume': pd.Series([x.volume for x in ticker_history],index=[x.dt for x in ticker_history])}}
    reform = {(outerKey, innerKey): values for outerKey, innerDict in data.items() for innerKey, values in innerDict.items()}
    tmp = pd.DataFrame(reform)
    print(tmp)


    return tmp
def load_ticker_history_pd_frame(ticker, ticker_history, convert_to_datetime=False, human_readable=False):
    _str = generate_csv_string(load_ticker_history_csv(ticker,ticker_history,convert_to_datetime=convert_to_datetime, human_readable=human_readable))
    df = pd.read_csv(io.StringIO(_str), sep=",")
    return df

# def load_ticker_

def normalize_history_data_for_day(ticker, ticker_history, module_config):
    days ={}
    #the day calculation should be the easiest
    for history in ticker_history:
        if history.dt.strftime("%Y-%m-%d") not in days:
            days[history.dt.strftime("%Y-%m-%d")] = []
        days[history.dt.strftime("%Y-%m-%d")].append(history)
    entries = []
    #ok so now we iterate over the days and get hte opens and closes
    #as well as the max high and the min low
    for day, histories in days.items():
        histories.sort(key=lambda x: x.timestamp)
        entries.append(TickerHistory(histories[0].open, histories[-1].close, max([x.high for x in histories]), min([x.low for x in histories]), sum([x.volume for x in histories]), histories[-1].timestamp))
        pass
    # entries
    return entries

def normalize_history_data_for_hour(ticker, ticker_history, module_config):
    print(f"Normalizing to timespans to hour for {ticker}")
    i = 1
    complete = False

    new_ticker_history = []
    # reversed(ticker_history)
    # start = timestamp_to_datetime(ticker_history[-1].timestamp)
    _th = []
    #ok so first let's go ahead and correct all the ticker histories
    # for i in range(1, len(ticker_history)):
    #     adjusted_date = timestamp_to_datetime(ticker_history[-i].timestamp) -datetime.timedelta(minutes=module_config['timespan_multiplier'])
    #     adjusted_timestsamp  = int(float(adjusted_date.strftime('%s.%f')) * 1e3)
    #     _th.append(TickerHistory(ticker_history[-i].open, ticker_history[-i].close, ticker_history[-i].high,ticker_history[-i].low, ticker_history[-i].volume, adjusted_timestsamp))
    # for i in range(1, len(_th)+1):
    #     new_ticker_history.append(_th[-i])
    # reversed(_th)
    # last_bar_of_day = 30 if module_config['timespan'] == 'minute' or module_config['timespan'] != 'minue'
    # reversed(_th)
    # dumber = [timestamp_to_datetime(x.timestamp) for x in new_ticker_history]
    # ticker_history = new_ticker_history
    # dumb = [timestamp_to_datetime(x.timestamp) for x in ticker_history]
    i = 1
    # for i in range(1, ):
    result = []
    while True:
        if i > (len(ticker_history) - int(60 / 30)):
            break
        normalized_time = timestamp_to_datetime(ticker_history[-i].timestamp)
        if module_config['logging']:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Normalizing History Record: {datetime.datetime.fromtimestamp(ticker_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
        # if ((normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).hour == 10  and  (normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).minute < module_config['timespan_multiplier']) or (normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).hour < 10:
        #     if module_config['logging']:
        #         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: History Record: {datetime.datetime.fromtimestamp(ticker_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}: Is PreMarket data, Skipping")
        #     i = i+1
        #     continue
        if (normalized_time + datetime.timedelta(minutes=30)).hour == 16 and (normalized_time + datetime.timedelta(minutes=30)).minute == 0:
            result.append(ticker_history[-i])
            i = i + 1
            continue
        hour = {'opens':[ticker_history[-(i)].open],'closes':[ticker_history[-i].close],'highs':[ticker_history[-i].high], 'lows':[ticker_history[-i].low], 'volumes':[ticker_history[-i].volume], 'timestamps': [ticker_history[-i].timestamp]}
        increment_idx = 0
        for ii in range(1,int(60/30)):
            if module_config['logging']:
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Processing Previous Bar For: {datetime.datetime.fromtimestamp(ticker_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}: {datetime.datetime.fromtimestamp(ticker_history[-(i+ii)].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
            hour['highs'].append(ticker_history[-(i+ii)].high)
            hour['lows'].append(ticker_history[-(i+ii)].low)
            hour['volumes'].append(ticker_history[-(i+ii)].volume)
            hour['timestamps'].append(ticker_history[-(i+ii)].timestamp)
            hour['opens'].append(ticker_history[-(i+ii)].open)
            hour['closes'].append(ticker_history[-(i+ii)].close)
            increment_idx = ii+1
            # i = i +1
        # if (timestamp_to_datetime(ticker_history[-i]) - timestamp_to_datetime(ticker_history[-(i+ii)])):
        #     pass
        now = datetime.datetime.now()
        # if normalized_time.day == now.day and normalized_time.month == now.month and normalized_time.year == now.year:#and now.minute > 30:
        # print()
        if normalized_time.minute > 30:
            # if "O:" in ticker
            th = TickerHistory(hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']), sum(hour['volumes']), hour['timestamps'][0])
        else:
            th = TickerHistory(hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']),
                               sum(hour['volumes']), hour['timestamps'][-1])
        # else:
        #     th = TickerHistory(hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']), sum(hour['volumes']), hour['timestamps'][-1])
        if module_config['logging']:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Normalized History Record: {datetime.datetime.fromtimestamp(th.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
        result.append(th)
        i = i + (increment_idx)
        # if timestamp_to_datetime(ticker_history[-i].timestamp).minute == 0 or timestamp_to_datetime(ticker_history[-i].timestamp).minute % module_config['timespan_multiplier'] == 0:
        #     new_ticker_history.append(ticker_history[-i])
        # else:
    # sorted(result, key=lambda x: x.timestamp)
    result.reverse()
    return  result
    # whilenot complete:
    #     if i == len(ticker_history):
    #         break

    pass


def load_options_history_raw(contract, ticker_history,client, multiplier = 1, timespan = "hour", from_ = "2023-07-06", to = "2023-07-06", limit=500, module_config={}, connection=None):
    # contract = contract, multiplier = 1, timespan = "hour", from_ = today, to = today,
    # limit = 50000
    if timespan == 'hour':
        timespan = 'minute'
        multiplier = 30
        module_config['og_ts_multiplier'] = module_config['timespan_multiplier']
        # module_config['timespan_multiplier'] = multiplier
    # if cached:
    #     return load_ticker_history_db(contract, module_config)
    # if os.path.exists(f"{module_config['output_dir']}cached/{contract}{module_config['timespan_multiplier']}{module_config['timespan']}.csv"):
    #     clear_ticker_history_cache_entry(contract,module_config)
    history_data =  []
    for entry in client.get_full_range_aggregate_bars(contract,from_, to,multiplier = multiplier, timespan = timespan, sort='asc', run_parallel=True):
        entry = ContractHistory(contract, ticker_history[-1].close,entry['o'],entry['c'],entry['h'],entry['l'],entry['v'],entry['t'])
        entry_date = datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
        # print(f"{entry_date}: {contract}| Open: {entry.open} High: {entry.high} Low: {entry.low} Close: {entry.close} Volume: {entry.volume}")
        if (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern')).hour >= 9 if timespan =='minute' else 10) and (datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern')).hour <= 16 if timespan =='minute' else 15):
            if timespan == 'minute':
                if (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern'))).hour == 9 and (datetime.datetime.fromtimestamp(entry.timestamp / 1e3,tz=ZoneInfo('US/Eastern'))).minute < 30:
                    continue
                elif (datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))).hour >=16:
                    continue
                else:
                    history_data.append(ContractHistory(contract, ticker_history[-1].close, entry.open, entry.close, entry.high, entry.low, entry.volume,entry.timestamp))

            else:
                history_data.append(ContractHistory(contract, ticker_history[-1].close,entry.open, entry.close,entry.high, entry.low, entry.volume, entry.timestamp))

    if module_config['test_mode']:
        if module_config['test_use_test_time']:
            # print(f"using test time")
            #rn make this work with the hours only
            for i in range(0, len(history_data)):
                if timestamp_to_datetime(history_data[-i].timestamp).hour == module_config['test_time']:
                    history_data = history_data[:-i+1]
                    break
    # if len(history_data) >0:
    #     print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${contract}: Latest History Record: {datetime.datetime.fromtimestamp(history_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Oldest History Record: {datetime.datetime.fromtimestamp(history_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Total: {len(history_data)}")
    #     write_ticker_history_cached(contract, history_data, module_config)
    #fix for no volume
    if len(history_data) == 0 or history_data[-1].timestamp < ticker_history[-1].timestamp:
        #def __init__(self,contract,underlying_close, open, close, high, low, volume, timestamp):
        #basically stub in a history entry
        # urls = [
        #     f"{module_config['api_endpoint']}/v3/reference/options/contracts?apiKey={module_config['api_key']}&underlying_ticker={ticker}&expiration_date={(now + datetime.timedelta(days=i)).strftime('%Y-%m-%d')}&{call_strike_price}&limit={module_config['contract_pullback_limit']}&contract_type=call&order=desc",
        # url = f"{module_config['api_endpoint']}/v1/open-close/{contract}/{(datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d')}?apiKey={module_config['api_key']}"
        url = f"{module_config['api_endpoint']}/v1/open-close/{contract}/{timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d')}?apiKey={module_config['api_key']}"

        r = requests.get(url)
        if r.status_code==200:
            raw_data = json.loads(r.text)
            history_data.append(ContractHistory(contract, ticker_history[-1].close, raw_data['open'],raw_data['open'] if 'close' not in raw_data else raw_data['close'],raw_data['high'],raw_data['low'],raw_data['volume'], ticker_history[-1].timestamp ))
    if len(history_data) > 0:
        if connection is not None:
            write_contract_history_db_entries(connection, contract, history_data,
                                            {"timespan": timespan, "timespan_multiplier": multiplier})
        if module_config['timespan'] == 'hour':
            history_data = normalize_contract_history_data_for_hour(contract, history_data,ticker_history, module_config)
            module_config['timespan_multiplier'] = module_config['og_ts_multiplier']
            if connection is not None:
                if len(history_data) > 0:
                    write_contract_history_db_entries(connection, contract, history_data, module_config)
            # if module_config['logging']:
    if len(history_data) > 0:
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:Contract ${contract}: Latest History Record: {datetime.datetime.fromtimestamp(history_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Oldest History Record: {datetime.datetime.fromtimestamp(history_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Total: {len(history_data)}")
    connection.commit()
    return history_data

def write_contract_history_db_entries(connection, ticker, contract_history, module_config):
    values_entries =[]
    for th in contract_history:
        values_entries.append(f"((select id from tickers_contract where symbol='{ticker}'), {th.open}, {th.close}, {th.high}, {th.low}, {th.volume},{th.timestamp},'{module_config['timespan']}','{module_config['timespan_multiplier']}', {th.implied_volatility}, {th.delta}, {th.theta},{th.gamma}, {th.rho})")
        # write_contract_history_db_entry(connection,ticker, th, module_config)
    #ok so dumb but before we run this let's do a select
    # if len(execute_query(connection, f"select * from history_contracthistory where timestamp >= {contract_history[0].timestamp} and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and contract_id=(select id from tickers_ticker where symbol='{ticker}')", verbose=False)) == 1:

    history_sql = f"INSERT ignore INTO history_contracthistory ( contract_id,open, close, high, low, volume, timestamp, timespan, timespan_multiplier, implied_volatility, delta, theta, gamma, rho) VALUES {','.join(values_entries)}"
    execute_update(connection,history_sql,verbose=False, auto_commit=False, cache=True)


def load_cached_option_tickers(ticker, module_config):
    return [x.split(f"{module_config['timespan_multiplier']}{module_config['timespan']}.csv")[0] for x in os.listdir(f"{module_config['output_dir']}cached/") if f"O:{ticker}" in x]

def normalize_contract_history_data_for_day(ticker, contract_history, ticker_history, module_config):
    days ={}
    #the day calculation should be the easiest
    for history in contract_history:
        if history.dt.strftime("%Y-%m-%d") not in days:
            days[history.dt.strftime("%Y-%m-%d")] = []
        days[history.dt.strftime("%Y-%m-%d")].append(history)
    entries = []
    #ok so now we iterate over the days and get hte opens and closes
    #as well as the max high and the min low
    for day, histories in days.items():
        histories.sort(key=lambda x: x.timestamp)
        entries.append(contract_history(histories[0].open, histories[-1].close, max([x.high for x in histories]), min([x.low for x in histories]), sum([x.volume for x in histories]), histories[-1].timestamp))
        pass
    # entries
    return entries

def normalize_contract_history_data_for_hour(ticker, contract_history,ticker_history, module_config):
    i = 1
    complete = False

    new_contract_history = []
    # reversed(contract_history)
    # start = timestamp_to_datetime(contract_history[-1].timestamp)
    _th = []
    #ok so first let's go ahead and correct all the ticker histories
    # for i in range(1, len(contract_history)):
    #     adjusted_date = timestamp_to_datetime(contract_history[-i].timestamp) -datetime.timedelta(minutes=module_config['timespan_multiplier'])
    #     adjusted_timestsamp  = int(float(adjusted_date.strftime('%s.%f')) * 1e3)
    #     _th.append(TickerHistory(contract_history[-i].open, contract_history[-i].close, contract_history[-i].high,contract_history[-i].low, contract_history[-i].volume, adjusted_timestsamp))
    # for i in range(1, len(_th)+1):
    #     new_contract_history.append(_th[-i])
    # reversed(_th)
    # last_bar_of_day = 30 if module_config['timespan'] == 'minute' or module_config['timespan'] != 'minue'
    # reversed(_th)
    # dumber = [timestamp_to_datetime(x.timestamp) for x in new_contract_history]
    # contract_history = new_contract_history
    # dumb = [timestamp_to_datetime(x.timestamp) for x in contract_history]
    i = 1
    # for i in range(1, ):
    result = []
    while True:
        if i > (len(contract_history) - int(60 / 30)):
            break

        try:
            underlying_entry = [x for x in ticker_history if x.timestamp == contract_history[-i].timestamp][0]
        except:
            i = i + 1
            print(f"Cannot determine underlying value for: {ticker}:{contract_history[-i].timestamp}")
            continue
        normalized_time = timestamp_to_datetime(contract_history[-i].timestamp)
        if module_config['logging']:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Normalizing History Record: {datetime.datetime.fromtimestamp(contract_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
        # if ((normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).hour == 10  and  (normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).minute < module_config['timespan_multiplier']) or (normalized_time + datetime.timedelta(minutes=module_config['timespan_multiplier'])).hour < 10:
        #     if module_config['logging']:
        #         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: History Record: {datetime.datetime.fromtimestamp(contract_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}: Is PreMarket data, Skipping")
        #     i = i+1
        #     continue
        if (normalized_time + datetime.timedelta(minutes=30)).hour == 16 and (normalized_time + datetime.timedelta(minutes=30)).minute == 0:
            result.append(contract_history[-i])
            i = i + 1
            continue
        hour = {'opens':[contract_history[-(i)].open],'closes':[contract_history[-i].close],'highs':[contract_history[-i].high], 'lows':[contract_history[-i].low], 'volumes':[contract_history[-i].volume], 'timestamps': [contract_history[-i].timestamp]}

        increment_idx = 0
        for ii in range(1,int(60/30)):
            if module_config['logging']:
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Processing Previous Bar For: {datetime.datetime.fromtimestamp(contract_history[-i].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}: {datetime.datetime.fromtimestamp(contract_history[-(i+ii)].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
            hour['highs'].append(contract_history[-(i+ii)].high)
            hour['lows'].append(contract_history[-(i+ii)].low)
            hour['volumes'].append(contract_history[-(i+ii)].volume)
            hour['timestamps'].append(contract_history[-(i+ii)].timestamp)
            hour['opens'].append(contract_history[-(i+ii)].open)
            hour['closes'].append(contract_history[-(i+ii)].close)
            increment_idx = ii+1
            # i = i +1
        # if (timestamp_to_datetime(contract_history[-i]) - timestamp_to_datetime(contract_history[-(i+ii)])):
        #     pass
        now = datetime.datetime.now()
        # if normalized_time.day == now.day and normalized_time.month == now.month and normalized_time.year == now.year:#and now.minute > 30:
        # print()
        if normalized_time.minute > 30:
            # if "O:" in ticker

            th = ContractHistory(ticker,underlying_entry.close,hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']), sum(hour['volumes']), hour['timestamps'][0])
        else:
            # underlying_entry = [x for x in ticker_history if x.timestamp == hour['timestamps'][-1]][0]
            th = ContractHistory(ticker,underlying_entry.close,hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']),
                               sum(hour['volumes']), hour['timestamps'][-1])
        # else:
        #     th = TickerHistory(hour['opens'][-1], hour['closes'][0], max(hour['highs']), min(hour['lows']), sum(hour['volumes']), hour['timestamps'][-1])
        if module_config['logging']:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Normalized History Record: {datetime.datetime.fromtimestamp(th.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:")
        result.append(th)
        i = i + (increment_idx)
        # if timestamp_to_datetime(contract_history[-i].timestamp).minute == 0 or timestamp_to_datetime(contract_history[-i].timestamp).minute % module_config['timespan_multiplier'] == 0:
        #     new_contract_history.append(contract_history[-i])
        # else:
    # sorted(result, key=lambda x: x.timestamp)
    result.reverse()
    return  result
    # whilenot complete:
    #     if i == len(ticker_history):
    #         break

    pass
# def load_ticker_histories(_tickers, module_config):
#     client = RESTClient(api_key=module_config['api_key'])
#     _module_config  =load_module_config(__file__.split("/")[-1].split(".py")[0])
#     connection= obtain_db_connection(_module_config)
#     try:
#         successes = []
#         failures = [['symbol']]
#         for ticker in _tickers:
#             print(f"{os.getpid()}: Loading {_tickers.index(ticker)+1}/{len(_tickers)} ticker datas")
#             try:
#                 # if not module_config['test_mode']:
#                 _ = load_ticker_history_raw(ticker, client,module_config['timespan_multiplier'], module_config['timespan'],get_today(module_config, minus_days=14), today, limit=50000, module_config=_module_config, connection=connection)
#                 # else:
#                     # _ = load_ticker_history_raw(ticker, client,1, module_config['timespan'],get_today(module_config, minus_days=365), 11, limit=50000, module_config=_module_config)
#                 successes.append(ticker)
#             except:
#                 # traceback.print_exc()
#                 failures.append([ticker])
#         # write_csv(f"{module_config['output_dir']}mpb_load_failures.csv",failures)
#     except:
#         traceback.print_exc()
#     # return  successes
#     # connection.close()
#     connection.close()
#     print(f"Closed ticker history load connection")

