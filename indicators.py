import operator
import os
import traceback
from itertools import chain

import pandas as pd
from iteration_utilities import chained
from functools import partial

# from db_functions import load_ticker_symbol_by_id, load_ticker_history_by_id
from enums import OrderType
import datetime
from zoneinfo import ZoneInfo
from history import load_ticker_history_pd_frame, load_ticker_history_cached, load_ticker_history_db
from stockstats import wrap
from enums import *
from shape import compare_tickers, compare_tickers_at_index
from functions import human_readable_datetime, timestamp_to_datetime, execute_query, execute_update
from support_resistance import find_support_resistance_levels
from profitable_lines import load_profitable_line_matrix

from tickers import load_ticker_symbol_by_id, load_ticker_history_by_id


# from validation import validate_dmi


# today =datetime.datetime.now().strftime("%Y-%m-%d")
def process_ticker_history(connection, ticker,ticker_history, module_config, validate=True, process_alerts=True):
    from validation import process_ticker_validation
    '''
    This function is where we're going to process our overall ticker history entries
    basically call each piece that we are writing to the db wtih as a fn, for example, process_ticker_validatino, process_ticker_alerts, etc
    :param ticker:
    :param ticker_results:
    :param ticker_history:
    :param module_config:
    :return:
    '''
    # _th = ticker_history
    #ok so first things first, we will go ahead and check the alerts
    # ticker_history = ticker_history[:-1] #since the last bar hasn't closed yet

    #ok so first we need to process any and all alerts
    # connection = obtain_db_connection()
    # def process_ticker_alerts(connection, ticker, ticker_history, module_config):
    if process_alerts:
        process_ticker_alerts(connection, ticker,ticker_history, module_config)
        print(f"Processed ticker alerts for {ticker}")
    #now we process validation
    if validate:
        process_ticker_validation(connection, ticker, ticker_history, module_config)
    # ticker_last_updated = load_ticker_last_updated(ticker, connection, module_config)
    # execute_update(connection, "lock tables lines_similarline write, history_tickerhistory read, tickers_ticker read")

    execute_update(connection, f"insert into lines_similarline (ticker_id, ticker_history_id, backward_range, forward_range) values ((select id from tickers_ticker where symbol='{ticker}'), (select id from history_tickerhistory where timestamp=(select coalesce(max(timestamp), round(1000 * unix_timestamp(date_sub(now(), interval 365 day)))) from history_tickerhistory where ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}) and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}')), 1,1)", auto_commit=False, cache=True)
def process_ticker_alerts(connection, ticker, ticker_history, module_config):
    '''
    Similar to above, this function is to process ticker history and flag any alerts
    :param ticker:
    :param ticker_history:
    :param module_config:
    :return:
    '''
    _th = ticker_history
    # ok so first things first, we will go ahead and check the alerts
    # ticker_history = ticker_history[:-1] #since the last bar hasn't closed yet

    #ok so we're going to try something different here
    indicator_inventory = get_indicator_inventory()
    values_list = []
    for indicator, function_dict in indicator_inventory.items():
        if function_dict[InventoryFunctionTypes.USE_N1_BARS]:
            ticker_history = _th[:-1]
        else:
            ticker_history = _th
        # def load_macd(ticker, ticker_history, module_config):
        # def did_golden_cross_alert(indicator_data, ticker, ticker_history, module_config)
        # def determine_macd_alert_type(indicator_data,ticker,ticker_history, module_config):
        # print(f"Running {indicator} on {ticker}")
        if function_dict[InventoryFunctionTypes.DID_ALERT](function_dict[InventoryFunctionTypes.LOAD](ticker, ticker_history,module_config, connection=connection), ticker, ticker_history, module_config, connection=connection):
            #alert did fire, so now we need to write the alert
            try:
                values_list.append(f"('{function_dict[InventoryFunctionTypes.DETERMINE_ALERT_TYPE](function_dict[InventoryFunctionTypes.LOAD](ticker, ticker_history,module_config, connection=connection), ticker, ticker_history, module_config, connection=connection)}',(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))")
                # write_ticker_alert(connection, function_dict[InventoryFunctionTypes.DETERMINE_ALERT_TYPE](function_dict[InventoryFunctionTypes.LOAD](ticker, ticker_history,module_config), ticker, ticker_history, module_config), ticker, _th,module_config )
            except:
                traceback.print_exc()

    # execute_update(connection, "lock tables alerts_tickeralert write, history_tickerhistory read, tickers_ticker read")
    if len(values_list) > 0:
        # execute_query(connection,f"select * from alerts_tickeralert where ticker_history_id=(select max(id) from history_tickerhistory where ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}')", verbose=False)
        execute_update(connection,f"insert ignore into alerts_tickeralert (alert_type, ticker_history_id) values {','.join(values_list)}",auto_commit=False, verbose=False, cache=True)
    #iterate back through, but this time we only fire the ignore functions, which should simply load the alerts for the period from the DB and then
    # ignore any alerts based upon other alerts
    #todo run ignores
    # for indicator, function_dict in indicator_inventory.items():
    #     # def ignore_golden_cross_alert(connection, alert_direction, ticker, ticker_history, module_config):
    #     #pass in as callable,
    #     function_dict[InventoryFunctionTypes.IGNORE](connection, function_dict[InventoryFunctionTypes.DETERMINE_ALERT_TYPE], ticker, _th, module_config)
    # connection.commit()
    # execute_update(connection, "unlock tables")

def get_indicator_inventory():
    '''
    The idea here is that we can maintain a listing of indicators to test when we process history as a dictionary
    did alert function checks if the indicator has flagged (i.e. crossover occurred, etc)
    determine alert type function determines the alert type that has fired
    ignroe sma alert function silences the alert based upon the conditions specified
    :param connection:
    :param ticker:
    :param ticker_history:
    :param module_config:
    :return:
    '''
    return {
        Indicator.SMA:{
            InventoryFunctionTypes.LOAD: load_sma,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_sma_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_sma_alert,
            InventoryFunctionTypes.IGNORE: ignore_sma_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },

        Indicator.RSI:{
            InventoryFunctionTypes.LOAD: load_rsi,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_rsi_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_rsi_alert,
            InventoryFunctionTypes.IGNORE: ignore_rsi_alert,
            InventoryFunctionTypes.USE_N1_BARS: False

        },
        Indicator.DMI: {
            InventoryFunctionTypes.LOAD: load_dmi_adx,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_dmi_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_dmi_alert,
            InventoryFunctionTypes.IGNORE: ignore_dmi_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.ADX: {
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_adx_alert_type,
            InventoryFunctionTypes.LOAD: load_dmi_adx,
            InventoryFunctionTypes.DID_ALERT: did_adx_alert,
            InventoryFunctionTypes.IGNORE: ignore_adx_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.MACD: {
            InventoryFunctionTypes.LOAD: load_macd,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_macd_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_macd_alert,
            InventoryFunctionTypes.IGNORE: ignore_macd_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.GOLDEN_CROSS: {
            InventoryFunctionTypes.LOAD: load_golden_cross,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_golden_cross_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_golden_cross_alert,
            InventoryFunctionTypes.IGNORE: ignore_golden_cross_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.DEATH_CROSS: {
            InventoryFunctionTypes.LOAD: load_death_cross,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_death_cross_alert_type,
            InventoryFunctionTypes.DID_ALERT: did_death_cross_alert,
            InventoryFunctionTypes.IGNORE: ignore_death_cross_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.PROFITABLE_LINE: {
            InventoryFunctionTypes.LOAD: load_profitable_lines,
            InventoryFunctionTypes.DID_ALERT: did_profitable_lines_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_profitable_lines_alert_type,
            InventoryFunctionTypes.IGNORE: ignore_profitable_lines_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.SUPPORT_RESISTANCE: {
            InventoryFunctionTypes.LOAD: load_support_resistance,
            InventoryFunctionTypes.DID_ALERT: did_support_resistance_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_sr_direction,
            InventoryFunctionTypes.IGNORE: ignore_support_resistance_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },

        Indicator.ADX_REVERSAL: {
            InventoryFunctionTypes.LOAD: load_dmi_adx,
            InventoryFunctionTypes.DID_ALERT: did_adx_reversal_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_adx_reversal_alert_type,
            InventoryFunctionTypes.IGNORE: ignore_adx_reversal_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.BREAKOUT: {
            InventoryFunctionTypes.LOAD: load_breakout,
            InventoryFunctionTypes.DID_ALERT: did_breakout_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_breakout_alert_type,
            InventoryFunctionTypes.IGNORE: ignore_breakout_reversal_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.VIX_RSI: {
            InventoryFunctionTypes.LOAD: load_vix_rsi,
            InventoryFunctionTypes.DID_ALERT: did_vix_rsi_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_vix_rsi_alert_type,
            InventoryFunctionTypes.IGNORE: ignore_vix_rsi_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        },
        Indicator.STOCHASTIC_RSI: {
            InventoryFunctionTypes.LOAD: load_stochastic_rsi,
            InventoryFunctionTypes.DID_ALERT: did_stochastic_rsi_alert,
            InventoryFunctionTypes.DETERMINE_ALERT_TYPE: determine_stochastic_rsi_alert_type,
            InventoryFunctionTypes.IGNORE: ignore_stochastic_rsi_alert,
            InventoryFunctionTypes.USE_N1_BARS: False
        }


    }


# def ignore_breakout_reversal_alert(connection, alert_direction, ticker, ticker_history, module_config):
def ignore_breakout_reversal_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_adx_reversal_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass

def ignore_support_resistance_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_profitable_lines_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_golden_cross_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_death_cross_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_vix_rsi_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_sma_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass

def ignore_macd_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass

def ignore_stochastic_rsi_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_rsi_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
    # todo code this up
    # if ticker_results[ticker]['rsi']:
    #     ticker_results[ticker]['directions'].append(
    #         determine_rsi_alert_type(rsi_data, ticker, ticker_history, module_config))
    #     # basically if any bullish alerst and overbought, ignore
    #     if ((ticker_results[ticker]['macd'] and determine_macd_alert_type(macd_data, ticker, ticker_history,
    #                                                                       module_config) == AlertType.MACD_MACD_CROSS_SIGNAL) or
    #         (ticker_results[ticker]['sma'] and determine_sma_alert_type(sma, ticker, ticker_history,
    #                                                                     module_config) == AlertType.SMA_CONFIRMATION_UPWARD) or
    #         (ticker_results[ticker]['dmi'] and determine_dmi_alert_type(dmi_adx_data, ticker, ticker_history,
    #                                                                     module_config) == AlertType.DMIPLUS_CROSSOVER_DMINEG)) and determine_rsi_alert_type(
    #         rsi_data, ticker, ticker_history, module_config) == AlertType.RSI_OVERBOUGHT:
    #         ticker_results[ticker]['rsi'] = False
    #         del ticker_results[ticker]['directions'][-1]
    #     elif ((ticker_results[ticker]['macd'] and determine_macd_alert_type(macd_data, ticker, ticker_history,
    #                                                                         module_config) == AlertType.MACD_SIGNAL_CROSS_MACD) or
    #           (ticker_results[ticker]['sma'] and determine_sma_alert_type(sma, ticker, ticker_history,
    #                                                                       module_config) == AlertType.SMA_CONFIRMATION_DOWNWARD) or
    #           (ticker_results[ticker]['dmi'] and determine_dmi_alert_type(dmi_adx_data, ticker, ticker_history,
    #                                                                       module_config) == AlertType.DMINEG_CROSSOVER_DMIPLUS)) and determine_rsi_alert_type(
    #         rsi_data, ticker, ticker_history, module_config) == AlertType.RSI_OVERSOLD:
    #         ticker_results[ticker]['rsi'] = False
    #         del ticker_results[ticker]['directions'][-1]
def ignore_adx_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
def ignore_dmi_alert(connection, alert_direction, ticker, ticker_history, module_config):
    pass
# def ignore_macd(ticker,ticker_history, module_config):
# def ignore_sma(ticker,ticker_history, module_config):
def load_macd(ticker,ticker_history, module_config, connection=None):
    df = wrap(load_ticker_history_pd_frame(ticker, ticker_history))
    return {'macd':df['macd'],'signal':df['macds'], 'histogram': df['macdh']}
def load_support_resistance(ticker, ticker_history, module_config, flatten=False,connection=None):

    if flatten:
        flattened_levels = list(chain.from_iterable(find_support_resistance_levels(ticker, ticker_history, module_config)))
        flattened_levels.sort(key=lambda x: x)
        return flattened_levels
    else:
        return find_support_resistance_levels(ticker, ticker_history, module_config)

def load_sma(ticker,ticker_history, module_config, window=0,connection=None):
    df = wrap(load_ticker_history_pd_frame(ticker, ticker_history))
    if window >0:
        # print(f"Returning {window} SMA")
        return df[f'close_{window}_sma']
    else:
        return df[f'close_{module_config["indicator_configs"][Indicator.SMA]["sma_window"]}_sma']


def load_golden_cross(ticker,ticker_history, module_config,connection=None):
    return {"sma_long":load_sma(ticker,ticker_history,module_config, window=module_config['indicator_configs'][Indicator.GOLDEN_CROSS]['gc_long_sma_window']), f"sma_short":load_sma(ticker,ticker_history,module_config, window=module_config['indicator_configs'][Indicator.GOLDEN_CROSS]['gc_short_sma_window'])}
def load_death_cross(ticker,ticker_history, module_config,connection=None):
    return {"sma_long":load_sma(ticker,ticker_history,module_config, window=module_config['indicator_configs'][Indicator.DEATH_CROSS]['dc_long_sma_window']), f"sma_short":load_sma(ticker,ticker_history,module_config, window=module_config['indicator_configs'][Indicator.DEATH_CROSS]['dc_short_sma_window'])}

# def load_sma(ticker, client,module_config, ticker_history, **kwargs):
#     sma = client.get_sma(ticker, **kwargs)
#     _sma = []
#     for entry in sma.values:
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(entry.timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             # entry_date.tzinfo = ZoneInfo('US/Eastern')
#             print(f"{entry_date}: {ticker}: SMA {entry.value}")
#         _sma.append(entry)
#
#
#
def load_stochastic_rsi(ticker, ticker_history, module_config,connection=None):
    df = wrap(load_ticker_history_pd_frame(ticker, ticker_history))
    return df['stochrsi']


def load_rsi(ticker, ticker_history, module_config,connection=None):
    df = wrap(load_ticker_history_pd_frame(ticker, ticker_history))
    return df['rsi']

def load_vix_rsi(ticker, ticker_history, module_config,connection=None):
    #load vix
    # def load_ticker_history_db(ticker,module_config, connection=None):
    return load_rsi(module_config['indicator_configs'][Indicator.VIX_RSI]['vix_source'], load_ticker_history_db(module_config['indicator_configs'][Indicator.VIX_RSI]['vix_source'], module_config,connection=connection), module_config)


def load_breakout(ticker, ticker_history, module_config,connection=None):
    df = wrap(load_ticker_history_pd_frame(ticker, ticker_history))

    # high = df.loc[:, df.columns.isin(['high'])]
    high = df['high']#.loc[:, df.columns.isin(['high'])]
    # dlow = df.loc[:, df.columns.isin(['low'])]
    low = df['low']#.loc[:, df.columns.isin(['low'])]
    # close = df.loc[:, df.columns.isin(['close'])]
    close = df['close']#.loc[:, df.columns.isin(['close'])]
    #
    upper = df['high'] * (1 + 4 * (df['high'] - df['low']) / (df['high'] + df['low']))
    lower = df['low'] * (1 - 4 * (df['high'] - df['low']) / (df['high'] + df['low']))
    upper_band = upper.rolling(20).mean()
    lower_band = lower.rolling(20).mean()
    sma20 = close.rolling(20).mean()
    #
    xo = (
        (close > upper_band)
        & (close.shift(1) > upper_band.shift(1))
        & (close > sma20)
    )
    xu = (
        (close < lower_band)
        & (close.shift(1) < lower_band.shift(1))
        & (close < sma20)
    )
    # find out if previous day was successful
    calls_hit = xo.shift(1) & (high > close.shift(1))
    puts_hit = xu.shift(1) & (low < close.shift(1))
    df = pd.concat([xo,xu,calls_hit,puts_hit], axis=1, keys=['xo','xu','calls_hit','puts_hit'])#.swaplevel(axis=1)
    return df
    # return df['rsi']


def load_obv(ticker, client,module_config,connection=None, **kwargs):
    pass
# def load_adx(ticker, client, **kwargs):
    # load_dmi(ticker,client,**kwargs)


def is_trading_in_sr_band(indicator_data, ticker, ticker_history, module_config, **kwargs):
    #basically here we determine if the current price is in a support/resistance band
    if len(indicator_data) == 0:
        return False
    plus_minus = sum([round(float((x[1] - x[0])/2), 2) for x in indicator_data if len(x) > 1]) / len(indicator_data)
    for sr_band in indicator_data:
        if len(sr_band) == 2:
            if sr_band[0] <= ticker_history[-1].close <= sr_band[1]:
                if module_config['logging']:
                    print(f"{human_readable_datetime(timestamp_to_datetime(ticker_history[-1].timestamp))}:${ticker}: (Last Close ${ticker_history[-1].close}) is trading within Support/Resistance Band (Low: {sr_band[0]} | Mark: {ticker_history[-1].close} | High: {sr_band[1]})")
                return True

        elif len(sr_band) == 1:
            # plus_minus = (0.25/100)*sr_band[0]
            _tmp_band = [sr_band[0] - plus_minus, sr_band[0] + plus_minus]
            if  _tmp_band[0] <= ticker_history[-1].close <= _tmp_band[1]:
                if module_config['logging']:
                    print(f"{human_readable_datetime(timestamp_to_datetime(ticker_history[-1].timestamp))}:${ticker}: (Last Close ${ticker_history[-1].close}) is trading within Support/Resistance Band (Low: {_tmp_band[0]} | Mark: {ticker_history[-1].close} | High: {_tmp_band[1]})")
                return True
    return False

def load_dmi_adx(ticker, ticker_history, module_config,connection=None, **kwargs):
    '''
    Returns a dict formatted like {'dmi+':<series_data>, 'dmi-':<series_data>, 'adx':<series_data>}
    where keys in the series are timestamps as loaded in load_ticker_history
    :param ticker:
    :param client:
    :param kwargs:
    :return:
    '''

    # print(f"{datetime.datetime.fromtimestamp(ticker_history[1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}")
    # print(f"{datetime.datetime.fromtimestamp(ticker_history[1][-1] / 1e3, tz=ZoneInfo('US/Eastern'))} {ticker_history[1][0]}")
    df= wrap(load_ticker_history_pd_frame(ticker, ticker_history))
    dmi= {"dmi+":df['pdi'],"dmi-":df['ndi'], "adx":df['adx']}
    if module_config['logging']:
        print(f"{datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: DMI+: {dmi['dmi+'][ticker_history[-1].timestamp]} DMI-:{dmi['dmi-'][ticker_history[-1].timestamp]} ADX: {dmi['adx'][ticker_history[-1].timestamp]}")
        print(f"{datetime.datetime.fromtimestamp(ticker_history[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: DMI+: {dmi['dmi+'][ticker_history[0].timestamp]} DMI-:{dmi['dmi-'][ticker_history[0].timestamp]} ADX: {dmi['adx'][ticker_history[0].timestamp]}")
    # for i in reversed(range(0, len(ticker_history))):
    #     if module_config['logging']:
    #     # if True:


    return dmi

def did_macd_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"Checking MACD Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}")
    # print(f"{ticker_history[-1]}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {indicator_data[ticker_history[-1].timestamp]} ")
    # if (data[0].value > data[0].signal and data[1].value < data[1].signal)  or (data[0].value < data[0].signal and data[1].value > data[1].signal):
    if (indicator_data['macd'][ticker_history[-1].timestamp] > indicator_data['signal'][ticker_history[-1].timestamp] and indicator_data['macd'][ticker_history[-2].timestamp] < indicator_data['signal'][ticker_history[-2].timestamp] and (indicator_data['histogram'][ticker_history[-1].timestamp] > indicator_data['histogram'][ticker_history[-2].timestamp] and indicator_data['histogram'][ticker_history[-1].timestamp] > 0) ) or \
            (indicator_data['macd'][ticker_history[-1].timestamp] < indicator_data['signal'][ticker_history[-1].timestamp] and indicator_data['macd'][ticker_history[-2].timestamp] > indicator_data['signal'][ticker_history[-2].timestamp] and (indicator_data['histogram'][ticker_history[-1].timestamp] < indicator_data['histogram'][ticker_history[-2].timestamp] and indicator_data['histogram'][ticker_history[-1].timestamp] < 0)):
        return True
    else:
        return  False

# def did_macd_alert(data, ticker,module_config):
#     if module_config['logging']:
#         print(f"checking macd for {ticker}")
#     #ok so the idea here is to look at the data for n vs  n-1 where n is the most recent macd reading
#     if (data[0].value > data[0].signal and data[1].value < data[1].signal)  or (data[0].value < data[0].signal and data[1].value > data[1].signal):
#
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             print(f"{entry_date}:{ticker}: MAC/Signal Crossover ")
#         return True
#     else:
#         return False
#     pass

def did_profitable_lines_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    matches = {}
    for line, line_data in indicator_data.items():
        positive_line_data_profit = line_data['profit'] >= 0
        try:
            compare_ticker = line_data['symbol']
        except:
            traceback.print_exc()
            print()
            raise Exception
        # print(f"Comparing {ticker} to {line} on {compare_ticker} at {human_readable_datetime(timestamp_to_datetime(line_data['timestamp']))}")
        loaded_histories ={compare_ticker: load_ticker_history_cached(compare_ticker, module_config)}

        history_entry = [x for x in loaded_histories[compare_ticker] if x.timestamp == int(line_data['timestamp'])][0]
        compare_index = next((i for i, item in enumerate(loaded_histories[compare_ticker]) if item.timestamp == history_entry.timestamp),-1)

        try:
            if len(ticker_history) >= module_config['line_profit_backward_range'] and len(loaded_histories[compare_ticker]) >= module_config['line_profit_backward_range']:
                match_likelihood = compare_tickers_at_index(compare_index, ticker, ticker_history,compare_ticker, loaded_histories[compare_ticker],module_config)

                matches[match_likelihood] = line
                # print(f"We actually found  a match: {ticker}:{ticker_history[-1].dt}=>{compare_ticker}:{loaded_histories[compare_ticker][:compare_index+1][-1].dt}: {line}:{line_data['profit']}%:{line_data['forward_range']} bars: {match_likelihood}")
                # profits[match_likelihood]= line_data['profit']

        except:
            traceback.print_exc()
            # print(f"Cannot calculate shape similarity between {ticker} (size: {len(ticker_history[0:indexes[0]])}) and {compare_ticker} (size: {len(loaded_histories[compare_ticker])}), not enough historical bars ")
            continue

    valid_matches = [x for x in matches.keys() if x > module_config['line_similarity_gt']]
    if len(valid_matches) > 0:
        return True

def did_sma_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"Checking SMA Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))} to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}")
    if (ticker_history[-1].close > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].low > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close > ticker_history[-1].open and ticker_history[-2].low <= indicator_data[ticker_history[-2].timestamp])  or \
       (ticker_history[-1].close < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].high < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close < ticker_history[-1].open and ticker_history[-2].high >= indicator_data[ticker_history[-2].timestamp]):
        return True
    else:
        return False

def determine_sma_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if (ticker_history[-1].close > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].open > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close > ticker_history[-1].open and ticker_history[-2].low <= indicator_data[ticker_history[-2].timestamp]):
        return AlertType.SMA_CONFIRMATION_UPWARD
    elif (ticker_history[-1].close < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close < ticker_history[-1].open and ticker_history[-2].high >= indicator_data[ticker_history[-2].timestamp]):
        return AlertType.SMA_CONFIRMATION_DOWNWARD
    else:
        raise Exception(f"Could not determine SMA Direction for {ticker}")


def did_golden_cross_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"Checking Golden Cross Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Long SMA {indicator_data['sma_long'][ticker_history[-1].timestamp]} Short SMA: {indicator_data['sma_short'][ticker_history[-1].timestamp]}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Long SMA {indicator_data['sma_long'][ticker_history[-2].timestamp]} Short SMA: {indicator_data['sma_short'][ticker_history[-2].timestamp]}:")
    return indicator_data['sma_short'][ticker_history[-1].timestamp] > indicator_data['sma_long'][ticker_history[-1].timestamp] and indicator_data['sma_short'][ticker_history[-2].timestamp] < indicator_data['sma_long'][ticker_history[-2].timestamp]

def did_death_cross_alert(indicator_data, ticker, ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(
            f"Checking Death Cross Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Long SMA {indicator_data['sma_long'][ticker_history[-1].timestamp]} Short SMA: {indicator_data['sma_short'][ticker_history[-1].timestamp]}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:Long SMA {indicator_data['sma_long'][ticker_history[-2].timestamp]} Short SMA: {indicator_data['sma_short'][ticker_history[-2].timestamp]}:")
    return indicator_data['sma_short'][ticker_history[-1].timestamp] < indicator_data['sma_long'][ticker_history[-1].timestamp] and indicator_data['sma_short'][ticker_history[-2].timestamp] > indicator_data['sma_long'][ticker_history[-2].timestamp]

    # if ((ticker_history[-1].close > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].low > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close > ticker_history[-1].open) and ticker_history[-2].open < indicator_data[ticker_history[-2].timestamp]) or\
    #         ((ticker_history[-1].close < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].high < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close < ticker_history[-1].open) and ticker_history[-2].open > indicator_data[ticker_history[-2].timestamp]):
    #     return True
    # else:
    #     return False
# def did_sma_alert(sma_data,ticker_data, ticker,module_config):
#     # ok so in the case of
#     entry_date = datetime.datetime.fromtimestamp(sma_data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#     entry_date_ticker = datetime.datetime.fromtimestamp(ticker_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#     # print(f"{entry_date}:{ticker}: SMA Alert Check ")
#     # print(f"{entry_date_ticker}:{ticker}: SMA Alert Check ")
#     if ((ticker_data[-1].close > sma_data[0].value and ticker_data[-1].open > sma_data[0].value and ticker_data[-1].close > ticker_data[-1].open) and ticker_data[-2].open  < sma_data[1].value ) or \
#        ((ticker_data[-1].close < sma_data[0].value and ticker_data[-1].open < sma_data[0].value and ticker_data[-1].close < ticker_data[-1].open)  and ticker_data[-2].open  > sma_data[1].value ):
#         if module_config['logging']:
#             print(f"{entry_date_ticker}:{ticker}: SMA Crossover Alert Fired on {ticker}")
#         return True
#     else:
#         return False


def did_adx_alert(dmi_data,ticker,ticker_data,module_config,connection=None):
    '''
    Pass in the data from the client and do calculations
    :param data:
    :return:
    '''

    valid_dmi = (dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]) or \
                (dmi_data['dmi+'][ticker_data[-1].timestamp] < dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi-'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp])
    if valid_dmi and dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]:
        if module_config['logging']:
            print(f"{datetime.datetime.fromtimestamp(ticker_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}:: ADX Alert Triggered  ADX Value: {dmi_data['adx'][ticker_data[-1].timestamp]} adx-1 Value: {dmi_data['adx'][ticker_data[-2].timestamp]} ")
        return True

    else:
        return False
def did_breakout_alert(breakout_data,ticker,ticker_data,module_config,connection=None):
    if module_config['logging']:
        print(f"Testing Breakout Alert for {ticker_data[-1].dt}")
    return breakout_data['xo'][ticker_data[-1].timestamp] or breakout_data['xu'][ticker_data[-1].timestamp]
def did_adx_reversal_alert(dmi_data,ticker,ticker_data,module_config,connection=None):
    '''
    Pass in the data from the client and do calculations
    :param data:
    :return:
    '''
    # print(f"{ticker_data[-1].dt}: ${ticker}: ADX: {dmi_data['adx'][ticker_data[-1].timestamp]} DMI+:{dmi_data['dmi+'][ticker_data[-1].timestamp]}: DMI-{dmi_data['dmi-'][ticker_data[-1].timestamp]}")
    # valid_dmi = (dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]) or \
    #             (dmi_data['dmi+'][ticker_data[-1].timestamp] < dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi-'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp])
    if dmi_data['adx'][ticker_data[-1].timestamp] > module_config['indicator_configs'][Indicator.ADX_REVERSAL]['adx_reversal_threshold'] and ((dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['dmi+'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp]) or (dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi-'][ticker_data[-1].timestamp] > dmi_data['dmi+'][ticker_data[-1].timestamp])):
    # if dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]:
        leading_dmi = 'dmi+' if  dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] else 'dmi-'
        trailing_dmi = 'dmi+' if  dmi_data['dmi+'][ticker_data[-1].timestamp] < dmi_data['dmi-'][ticker_data[-1].timestamp] else 'dmi-'

        return (dmi_data[leading_dmi][ticker_data[-1].timestamp] < dmi_data[leading_dmi][ticker_data[-2].timestamp] and dmi_data[leading_dmi][ticker_data[-2].timestamp] < dmi_data[leading_dmi][ticker_data[-3].timestamp]) and (dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp] and dmi_data['adx'][ticker_data[-2].timestamp] > dmi_data['adx'][ticker_data[-3].timestamp]) and dmi_data[leading_dmi][ticker_data[-1].timestamp] < dmi_data[leading_dmi][ticker_data[-2].timestamp] and dmi_data['adx'][ticker_data[-1].timestamp] >  dmi_data['adx'][ticker_data[-2].timestamp]

    else:
        return False


def did_dmi_alert(dmi_data,ticker,ticker_data,module_config,connection=None):

    # ok so check for dmi+ crossing over dmi- AND dmi+ over adx OR dmi- crossing over dmi+ AND dmi- over adx
    if (dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-2].timestamp] < dmi_data['dmi-'][ticker_data[-2].timestamp] and dmi_data['dmi+'][ticker_data[-1].timestamp] >  dmi_data['adx'][ticker_data[-1].timestamp]) or (dmi_data['dmi+'][ticker_data[-1].timestamp] < dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-2].timestamp] > dmi_data['dmi-'][ticker_data[-2].timestamp] and dmi_data['dmi-'][ticker_data[-1].timestamp] >  dmi_data['adx'][ticker_data[-1].timestamp]):
        if module_config['logging']:
            print(f"{datetime.datetime.fromtimestamp(ticker_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}::{ticker}: DMI Alert Triggered (DMI+: {dmi_data['dmi+'][ticker_data[-1].timestamp]} DMI-:{dmi_data['dmi-'][ticker_data[-1].timestamp]} ADX: {dmi_data['adx'][ticker_data[-1].timestamp]})")
        return True
    else:
        return False



def did_support_resistance_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if determine_sr_direction(indicator_data,ticker,ticker_history, module_config,connection=None) != AlertType.SUPPORT_RESISTANCE_CONSOLIDATON:
        return True
def did_vix_rsi_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"${ticker}: Checking RSI Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}:")
    try:
        if indicator_data[ticker_history[-1].timestamp] > module_config['indicator_configs'][Indicator.VIX_RSI]['rsi_overbought_threshold'] or indicator_data[ticker_history[-1].timestamp] < module_config['indicator_configs'][Indicator.VIX_RSI]['rsi_oversold_threshold']:
            return True
        else:
            return False
    except:
        return False


def did_stochastic_rsi_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"${ticker}: Checking STochastic RSI Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}:")
    if indicator_data[ticker_history[-1].timestamp] > module_config['indicator_configs'][Indicator.STOCHASTIC_RSI]['rsi_overbought_threshold'] or indicator_data[ticker_history[-1].timestamp] < module_config['indicator_configs'][Indicator.STOCHASTIC_RSI]['rsi_oversold_threshold']:
        return True
    else:
        return False
def did_rsi_alert(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"${ticker}: Checking RSI Alert, Comparing Value at {datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: to value at {datetime.datetime.fromtimestamp(ticker_history[-2].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}:")
    if indicator_data[ticker_history[-1].timestamp] > module_config['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold'] or indicator_data[ticker_history[-1].timestamp] < module_config['indicator_configs'][Indicator.RSI]['rsi_oversold_threshold']:
        return True
    else:
        return False

# def did_rsi_alert(data, ticker,module_config):
#     if data[0].value > module_config['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold'] or data[0].value < module_config['indicator_configs'][Indicator.RSI]['rsi_oversold_threshold']:
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             print(f"{entry_date}:{ticker}: RSI Alerted at {data[0].value} ")
#         return True
#     else:
#         return  False


def determine_macd_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if (indicator_data['macd'][ticker_history[-1].timestamp] > indicator_data['signal'][ticker_history[-1].timestamp] and indicator_data['macd'][ticker_history[-2].timestamp] < indicator_data['signal'][ticker_history[-2].timestamp] and (indicator_data['histogram'][ticker_history[-1].timestamp] > indicator_data['histogram'][ticker_history[-2].timestamp] and indicator_data['histogram'][ticker_history[-1].timestamp] > 0)) :
        return AlertType.MACD_MACD_CROSS_SIGNAL
    elif (indicator_data['macd'][ticker_history[-1].timestamp] < indicator_data['signal'][ ticker_history[-1].timestamp] and indicator_data['macd'][ticker_history[-2].timestamp] > indicator_data['signal'][ticker_history[-2].timestamp] and (indicator_data['histogram'][ticker_history[-1].timestamp] < indicator_data['histogram'][ticker_history[-2].timestamp] and indicator_data['histogram'][ticker_history[-1].timestamp] < 0)):
        return AlertType.MACD_SIGNAL_CROSS_MACD
    else:
        raise Exception("Unable to determine MACD alert type ")

# def determine_macd_alert_type(data, ticker,module_config):
#     # ok so the idea here is to look at the data for n vs  n-1 where n is the most recent macd reading
#     if (data[0].value > data[0].signal and data[1].value < data[1].signal) :
#
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             print(f"{entry_date}:{ticker}: MAC/Signal Crossover ")
#         return AlertType.MACD_MACD_CROSS_SIGNAL
#     elif (data[0].value < data[0].signal and data[1].value > data[1].signal):
#         return AlertType.MACD_SIGNAL_CROSS_MACD
#     else:
#         raise Exception(f"Could not determine MACD direction for: {ticker}")
def determine_breakout_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if module_config['logging']:
        print(f"Determining BREAKOUT alert type on {ticker_history[-1].dt}")
    if indicator_data['xo'][ticker_history[-1].timestamp]:
        return AlertType.BREAKOUT_CROSSOVER_BULLISH
    if indicator_data['xu'][ticker_history[-1].timestamp]:
        return AlertType.BREAKOUT_CROSSOVER_BEARISH
def determine_vix_rsi_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    alert_type = determine_rsi_alert_type(indicator_data, ticker, ticker_history, module_config)
    if alert_type == AlertType.RSI_OVERBOUGHT:
        return AlertType.VIX_RSI_OVERBOUGHT
    elif alert_type == AlertType.RSI_OVERSOLD:
        return AlertType.VIX_RSI_OVERSOLD
    else:
        return AlertType.VIX_RSI_NORMAL

def determine_stochastic_rsi_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if indicator_data[ticker_history[-1].timestamp] >= module_config['indicator_configs'][Indicator.STOCHASTIC_RSI]['rsi_overbought_threshold']:
        if module_config['logging']:
            entry_date = datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
            print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {indicator_data[ticker_history[-1].timestamp]} ")
        return AlertType.STOCHASTIC_RSI_OVERBOUGHT
    elif indicator_data[ticker_history[-1].timestamp] <= module_config['indicator_configs'][Indicator.STOCHASTIC_RSI]['rsi_oversold_threshold']:
        if module_config['logging']:
            entry_date = datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
            print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {indicator_data[ticker_history[-1].timestamp]} ")
        return AlertType.STOCHASTIC_RSI_OVERSOLD
    else:
        return AlertType.STOCHASTIC_RSI_NORMAL
def determine_rsi_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if indicator_data[ticker_history[-1].timestamp] >= module_config['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold']:
        if module_config['logging']:
            entry_date = datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
            print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {indicator_data[ticker_history[-1].timestamp]} ")
        return AlertType.RSI_OVERBOUGHT
    elif indicator_data[ticker_history[-1].timestamp] <= module_config['indicator_configs'][Indicator.RSI]['rsi_oversold_threshold']:
        if module_config['logging']:
            entry_date = datetime.datetime.fromtimestamp(ticker_history[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
            print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {indicator_data[ticker_history[-1].timestamp]} ")
        return AlertType.RSI_OVERSOLD
    else:
        return AlertType.RSI_NORMAL
        # raise Exception(f"Could not determine RSI Direction for {ticker}")
# def determine_rsi_direction(data, ticker, module_config):
#     if data[0].value > module_config['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold']:
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {data[0].value} ")
#         return AlertType.RSI_OVERBOUGHT
#     elif data[0].value < module_config['indicator_configs'][Indicator.RSI]['rsi_oversold_threshold']:
#         if module_config['logging']:
#             entry_date = datetime.datetime.fromtimestamp(data[0].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))
#             print(f"{entry_date}:{ticker}: RSI determined to be {AlertType.RSI_OVERSOLD}: RSI: {data[0].value} ")
#         return AlertType.RSI_OVERSOLD
#     else:
#         raise Exception(f"Could not determine RSI Direction for {ticker}")


def determine_profitable_lines_alert_type(indicator_data,ticker,ticker_history, module_config, connection=None):
    matches = {}
    for line, line_data in indicator_data.items():
        positive_line_data_profit = line_data['profit'] >= 0
        # so here we load the ticker histroy
        # if positive_profit != positive_line_data_profit:
        #     print(
        #         f"Skipping Historic Profit Line of {line_data['profit']}%, the trend is in the opposite direction of profit {profit}%")
        #     continue
        try:
            # compare_ticker = load_ticker_symbol_by_id(connection, [x for x in line_data['matches'][0].values()][0],module_config)
            compare_ticker = line_data['symbol']
        except:
            traceback.print_exc()
            print()
            raise Exception
        loaded_histories ={compare_ticker: load_ticker_history_cached(compare_ticker, module_config)}

        # history_entry = load_ticker_history_by_id(connection, [x for x in line_data['matches'][0].keys()][0],
        #                                           compare_ticker, module_config)
        history_entry = [x for x in loaded_histories[compare_ticker] if x.timestamp == int(line_data['timestamp'])][0]
        compare_index = next((i for i, item in enumerate(loaded_histories[compare_ticker]) if item.timestamp == history_entry.timestamp),-1)

        try:
            if len(ticker_history) >= module_config['line_profit_backward_range'] and len(loaded_histories[compare_ticker]) >= module_config['line_profit_backward_range']:
                match_likelihood = compare_tickers_at_index(compare_index, ticker, ticker_history,compare_ticker, loaded_histories[compare_ticker],module_config)
                matches[match_likelihood] = line
                # print(f"We actually found  a match")
                # profits[match_likelihood]= line_data['profit']

        except:
            # traceback.print_exc()
            # print(f"Cannot calculate shape similarity between {ticker} (size: {len(ticker_history[0:indexes[0]])}) and {compare_ticker} (size: {len(loaded_histories[compare_ticker])}), not enough historical bars ")
            continue

    valid_matches = [x for x in matches.keys() if x > module_config['line_similarity_gt']]
    if len(valid_matches) > 0:
        return f"{matches[max(valid_matches)]}|{indicator_data[matches[max(valid_matches)]]['forward_range']} bars|{indicator_data[matches[max(valid_matches)]]['profit']}%"
    else:
        raise Exception(f"Cannot determine profitable line type for {ticker}: No profitable lines were matched, how did you get here?")
def determine_adx_alert_type(data, ticker,ticker_data,  module_config,connection=None):
    return AlertType.ADX_THRESHOLD_UPWARD
#
# def determine_dmi_alert_type(data, ticker, ticker_data, module_config):

def determine_adx_reversal_alert_type(dmi_data, ticker, ticker_data, module_config,connection=None):
    if dmi_data['adx'][ticker_data[-1].timestamp] > module_config['indicator_configs'][Indicator.ADX_REVERSAL]['adx_reversal_threshold'] and ((dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['dmi+'][ticker_data[-1].timestamp] and dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp]) or (dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] and dmi_data['dmi-'][ticker_data[-1].timestamp] > dmi_data['dmi+'][ticker_data[-1].timestamp])):

        # if dmi_data['adx'][ticker_data[-1].timestamp] > module_config['adx_threshold'] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]:
        # leading_dmi = 'dmi+' if dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp] else 'dmi-'
        # leading_dmi = 'dmi+' if dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-2].timestamp] else 'dmi-'
        if dmi_data['dmi+'][ticker_data[-1].timestamp] > dmi_data['dmi-'][ticker_data[-1].timestamp]:
            return AlertType.ADX_REVERSAL_BEARISH
        else:
            return AlertType.   ADX_REVERSAL_BULLISH
        # trailing_dmi = 'dmi+' if dmi_data['dmi+'][ticker_data[-1].timestamp] < dmi_data['dmi-'][ticker_data[-2].timestamp] else 'dmi-'

        # if dmi_data[leading_dmi][ticker_data[-1].timestamp] < dmi_data[leading_dmi][ticker_data[-1].timestamp] and dmi_data['adx'][ticker_data[-1].timestamp] > dmi_data['adx'][ticker_data[-2].timestamp]:
        #     if

    else:
        return False


def determine_dmi_alert_type(data, ticker, ticker_data, module_config,connection=None):
    if (data['dmi+'][ticker_data[-1].timestamp] > data['dmi-'][ticker_data[-1].timestamp] and data['dmi+'][ticker_data[-2].timestamp] < data['dmi-'][ticker_data[-2].timestamp] and data['dmi+'][ticker_data[-1].timestamp] > data['adx'][ticker_data[-1].timestamp]):
        if module_config['logging']:
            print(f"{datetime.datetime.fromtimestamp(ticker_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: DMI Alert Determined Directio: {AlertType.DMIPLUS_CROSSOVER_DMINEG} (DMI+: {data['dmi+'][ticker_data[-1].timestamp]} DMI-:{data['dmi-'][ticker_data[-1].timestamp]} ADX: {data['adx'][ticker_data[-1].timestamp]})")
        return AlertType.DMIPLUS_CROSSOVER_DMINEG
    elif (data['dmi+'][ticker_data[-1].timestamp] < data['dmi-'][ticker_data[-1].timestamp] and data['dmi+'][ticker_data[-2].timestamp] > data['dmi-'][ticker_data[-2].timestamp] and data['dmi-'][ticker_data[-1].timestamp] > data['adx'][ticker_data[-1].timestamp]):
        if module_config['logging']:
            print(f"{datetime.datetime.fromtimestamp(ticker_data[-1].timestamp / 1e3, tz=ZoneInfo('US/Eastern'))}:{ticker}: DMI Alert Determined Directio: {AlertType.DMIPLUS_CROSSOVER_DMINEG} (DMI+: {data['dmi+'][ticker_data[-1].timestamp]} DMI-:{data['dmi-'][ticker_data[-1].timestamp]} ADX: {data['adx'][ticker_data[-1].timestamp]})")
        return AlertType.DMINEG_CROSSOVER_DMIPLUS


    else:
        raise Exception(f"Could not determine RSI Direction for {ticker}")


def determine_death_cross_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if indicator_data['sma_short'][ticker_history[-1].timestamp] < indicator_data['sma_long'][ticker_history[-1].timestamp] and indicator_data['sma_short'][ticker_history[-2].timestamp] > indicator_data['sma_long'][ticker_history[-2].timestamp]:
        return AlertType.DEATH_CROSS_APPEARED
    else:
        raise Exception(f"Could not determine Golden Cross Alert for {ticker}")
def determine_sr_direction(indicator_data,ticker,ticker_history, module_config,connection=None):
    if len(indicator_data) == 0:
        raise Exception("Cannot determine Support/Resistance Direction with no data!")
    plus_minus = sum([round(float((x[1] - x[0]) / 2), 2) for x in indicator_data if len(x) > 1]) / len(indicator_data)
    # ok so if we're trading in an SR band, we wan
    trending_positive = (ticker_history[-1].close >ticker_history[-1].open) and  (ticker_history[-2].open < ticker_history[-2].close) and ticker_history[-2].close < ticker_history[-1].close# sum([ticker_history[-ii].close - ticker_history[-(ii + 1)].close for ii in range(1, 2)]) >= 0
    for sr_band in indicator_data:
        if len(sr_band) == 2:
            if sr_band[0] <= ticker_history[-1].close <= sr_band[1]:

                # if module_config['logging']:
                    # print(
                        # f"{human_readable_datetime(timestamp_to_datetime(ticker_history[-1].timestamp))}:${ticker}: (Last Close ${ticker_history[-1].close}) is trading within Support/Resistance Band (Low: {sr_band[0]} | Mark: {ticker_history[-1].close} | High: {sr_band[1]})")

                #if we're in a band, find out whether it's heading toward the top or the bottom of the band
                #look back over last 3 bars, and see what the trend is, if it's down we're presumably headed to the support level,
                #if it's up presumably we're headed
                return AlertType.SUPPORT_RESISTANCE_CONSOLIDATON#+f" (LB: ${round(sr_band[0],2)} | Current: ${ticker_history[-1].close} | HB: ${round(sr_band[1],2)})"
                # return AlertType.SUPPORT_RESISTANCE_CONSOLIDATON+f" (LB: ${round(sr_band[0],2)} | Current: ${ticker_history[-1].close} | HB: ${round(sr_band[1],2)})"

        elif len(sr_band) == 1:
            # plus_minus = (0.25/100)*sr_band[0]
            _tmp_band = [sr_band[0] - plus_minus, sr_band[0] + plus_minus]
            if _tmp_band[0] <= ticker_history[-1].close <= _tmp_band[1]:
                return AlertType.SUPPORT_RESISTANCE_CONSOLIDATON#+f" (LB: ${round(_tmp_band[0],2)} | Current: ${ticker_history[-1].close} | HB: ${round(_tmp_band[1],2)})"
                # return AlertType.SUPPORT_RESISTANCE_CONSOLIDATON+f" (LB: ${round(_tmp_band[0],2)} | Current: ${ticker_history[-1].close} | HB: ${round(_tmp_band[1],2)})"
                pass

    #if we get here we are in a breakout, so we need to find out what band it's approaching
    #first get direction
    # deltas =
    #positive upward movement
    # ok so now we need to figure out what the nearest SR band is
    # _tmp_band = [sr_band[0] - plus_minus, sr_band[0] + plus_minus]
    distance_to_points = []

    flattened_levels = list(chain.from_iterable(indicator_data))
    flattened_levels.sort(key=lambda x: x)

    for level in flattened_levels:
        if trending_positive:
            if level < ticker_history[-1].close:
                continue
            else:
                distance_to_points.append(level - ticker_history[-1].close)
        else:
            if level > ticker_history[-1].close:
                continue
            else:
                distance_to_points.append(level - ticker_history[-1].close)


    if len(distance_to_points) == 0:
        if trending_positive:

            return AlertType.ABOVE_HIGHEST_SR_BAND#+f": ${flattened_levels[-1]}"
            # return AlertType.ABOVE_HIGHEST_SR_BAND+f": ${flattened_levels[-1]}"
        else:
            return AlertType.BELOW_LOWEST_SR_BAND#+f" ${flattened_levels[0]}"
            # return AlertType.BELOW_LOWEST_SR_BAND+f" ${flattened_levels[0]}"
    else:
        # sr_band = indicator_data[min_index] if len(indicator_data[min_index]) > 1 else [sr_band[0] - plus_minus, sr_band[0] + plus_minus]
        distance_to_points.sort(key=lambda x: x, reverse=not trending_positive)
        if trending_positive:
            # return AlertType.SUPPORT_RESISTANCE_BREAKOUT_UP+f"==>${round(distance_to_points[0]+ticker_history[-1].close,2)} (${round(distance_to_points[0],2)}/{round(float(distance_to_points[0] / ticker_history[-1].close)*100,2)}%)"
            return AlertType.SUPPORT_RESISTANCE_BREAKOUT_UP#+f"==>${round(distance_to_points[0]+ticker_history[-1].close,2)} (${round(distance_to_points[0],2)}/{round(float(distance_to_points[0] / ticker_history[-1].close)*100,2)}%)"
        else:
            # return AlertType.SUPPORT_RESISTANCE_BREAKOUT_DOWN+f"==>${round(distance_to_points[0]+ticker_history[-1].close,2)} (${round(distance_to_points[0],2)}/{round(float(distance_to_points[0] / ticker_history[-1].close)*100,2)}%)"
            return AlertType.SUPPORT_RESISTANCE_BREAKOUT_DOWN#+f"==>${round(distance_to_points[0]+ticker_history[-1].close,2)} (${round(distance_to_points[0],2)}/{round(float(distance_to_points[0] / ticker_history[-1].close)*100,2)}%)"
def determine_golden_cross_alert_type(indicator_data,ticker,ticker_history, module_config,connection=None):
    if did_golden_cross_alert(indicator_data,ticker,ticker_history,module_config):
        return AlertType.GOLDEN_CROSS_APPEARED
    else:
        raise Exception(f"Could not determine Golden Cross Alert for {ticker}")
# def determine_death_cross_alert_type(indicator_data,ticker,ticker_history, module_config):

def has_matching_trend_with_ticker(ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config):
    return compare_tickers(ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config) >= module_config['line_similarity_gt']

def load_profitable_lines(ticker,ticker_history, module_config,connection=None):
    return load_profitable_line_matrix(connection, module_config, ignore_cache=False)
def load_ticker_similar_trends(ticker, module_config,connection=None):
    ticker_history = load_ticker_history_cached(ticker, module_config)
    result = []
    # if module_config['logging']:
    print(f"{human_readable_datetime(datetime.datetime.now())}:${ticker}: Performing line comparison of ${ticker}")
    for compare_ticker in [x.split(f"{module_config['timespan_multiplier']}{module_config['timespan']}.csv")[0] for x in os.listdir(f"{module_config['output_dir']}cached/") if "O:" not in x]:
        try:
            if module_config['logging']:
                print(f"{human_readable_datetime(datetime.datetime.now())}:${ticker}: Performing line comparison of ${ticker} <==> ${compare_ticker}")
            similarity = compare_tickers(ticker, ticker_history, compare_ticker, load_ticker_history_cached(compare_ticker, module_config), module_config)
            if similarity >= module_config['line_similarity_gt']:
                result.append([compare_ticker, similarity])
        except:
            pass
            # traceback.print_exc()


    #ok so once we get here, we need to sort by similarity, take top 3?
    # itemgetter_int = chained(operator.itemgetter(1),
    #                          partial(map, float), tuple)
    result.sort(key=operator.itemgetter(1))
    result.reverse()
    return [x[0] for x in result[:module_config['similar_line_tickers_limit']]]