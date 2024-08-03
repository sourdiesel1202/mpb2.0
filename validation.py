import datetime
import json
import traceback

from functions import timestamp_to_datetime, execute_query, execute_update
from enums import PositionType, strategy_type_dict, ValidationType, Indicator
from indicators import load_sma,load_macd, load_dmi_adx, load_rsi
# from indicators import did_sma_alert, determine_sma_alert_type

def process_ticker_validation(connection, ticker, ticker_history, module_config):
    '''
    bassically iterate through the position_type:strategies dict and deterine for
    long, short and neutral positions whether the ticker is currently valid to enter the position type
    :param ticker:
    :param ticker_history:
    :param module_config:
    :return:
    '''

    # ticker_results[ticker]['long_validation'] = ','.join([k for k, v in validate_ticker(PositionType.LONG, ticker, _th, module_config).items() if v])
    # ticker_results[ticker]['short_validation'] = ','.join([k for k, v in validate_ticker(PositionType.SHORT, ticker, _th, module_config).items() if v])
    # ok so here we're going to do our validations and write validation for any position types that match the position type (i.e. long call, long put, etc)b
    if not module_config['validate']:
        print(f"Validation is turned off in module config, stubbing in")
#        # if len(execute_query(connection,f"select * from validation_tickervalidation where validation_type in ({','.join(indicator_list)}) and strategy_type in ({','.join(strategy_list)}) and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}')")) < 2:
        values = [f"('{ValidationType.VALIDATION_OFF}','{PositionType.LONG_OPTION}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))",
                f"('{ValidationType.VALIDATION_OFF}','{PositionType.SHORT_OPTION}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))"]
        #
        # execute_update(connection, "lock tables validation_tickervalidation write,backtests_backtest write, history_tickerhistory read, tickers_ticker read")
        execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values {','.join(values)} ",
                       auto_commit=False, verbose=False, cache=True)
        # execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values ('" + '{}' + f"', (select id from validation_tickervalidation where strategy_type='{strategy_type}' and validation_type='{indicator}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))",auto_commit=False, verbose=False)
            # execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values ('{indicator}','{strategy_type}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))", auto_commit=False, verbose=False)
        # # #second, stub in the backtest
        # # connection.commit()
        # # execute_update(connection, "unlock tables")
        #
        # # execute_update(connection, "lock tables backtests_backtest write, validation_tickervalidation read, history_tickerhistory read, tickers_ticker read")
        bt_values = [f"('" + '{}' + f"', (select id from validation_tickervalidation where strategy_type='{PositionType.LONG_OPTION}' and validation_type='{ValidationType.VALIDATION_OFF}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))",f"('" + '{}' + f"', (select id from validation_tickervalidation where strategy_type='{PositionType.LONG_OPTION}' and validation_type='{ValidationType.VALIDATION_OFF}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))"]
        # # if len(execute_query(connection,f"select * from backtests_backtest where validation_id in (select id from validation_tickervalidation where validation_type in ({','.join(indicator_list)}) and strategy_type in ({','.join(strategy_list)}) and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))")) < 2:
        # # execute_update(connection, "lock tables history_tickerhistory write, tickers_ticker read ")
        #
        execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values {','.join(bt_values)} ",auto_commit=False, verbose=False, cache=True)
        # connection.commit()
        # execute_update(connection, "unlock tables")

        return



    indicator_list = []
    strategy_list = []
    values = []
    bt_values = []
    for position_type, strategies in strategy_type_dict.items():
        for k, v in validate_ticker(position_type, ticker, ticker_history, module_config).items():
            # if module_config['logging']:
            if v:
                print(f"Validated {position_type} position in {ticker} ({k}), writing {len(strategies)} strategies")
                for strategy in strategies:
                    try:
                        indicator_list.append(f"'{k}'")
                        strategy_list.append(f"'{strategy}'")
                        values.append(f"('{k}','{strategy}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))")
                        bt_values.append(f"('"+'{}'+f"', (select id from validation_tickervalidation where strategy_type='{strategy}' and validation_type='{k}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))")
                        # write_ticker_validation(connection,strategy, k, ticker, ticker_history, module_config )
                    except:
                        traceback.print_exc()

    #ok first we need to run the sql
    # if len(execute_query(connection, f"select * from validation_tickervalidation where validation_type in ({','.join(indicator_list)}) and strategy_type in ({','.join(strategy_list)}) and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}')")) <2:
    # execute_update(connection, "lock tables  backtests_backtest write, validation_tickervalidation write, history_tickerhistory read, tickers_ticker read, ")
    execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values {','.join(values)} ",auto_commit=False, verbose=False, cache=True)
        # execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values ('" + '{}' + f"', (select id from validation_tickervalidation where strategy_type='{strategy_type}' and validation_type='{indicator}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))",auto_commit=False, verbose=False)
        # execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values ('{indicator}','{strategy_type}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))", auto_commit=False, verbose=False)
    # #second, stub in the backtest
    # if len(execute_query(connection, f"select * from backtests_backtest where validation_id in (select id from validation_tickervalidation where validation_type in ({','.join(indicator_list)}) and strategy_type in ({','.join(strategy_list)}) and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))")) < 2:
    execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values {','.join(bt_values)} ", auto_commit=False, verbose=False, cache=True)
    # connection.commit()
    # execute_update(connection, "unlock_tables")

    # execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values ('"+'{}'+f"', (select id from validation_tickervalidation where strategy_type='{strategy_type}' and validation_type='{indicator}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))", auto_commit=False, verbose=False)
    # execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values {','.join(values)} ",auto_commit=True, verbose=False)
    # execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values {','.join(values)} ",auto_commit=True, verbose=False)
    # execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values {','.join(bt_values)} ", auto_commit=True, verbose=False)
#

def write_ticker_validation(connection,  strategy_type,indicator, ticker, ticker_history,module_config):
    #first, write the validation
    if len(execute_query(connection, f"select * from validation_tickervalidation where validation_type='{indicator}' and strategy_type ='{strategy_type}' and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}')")) <2:
        execute_update(connection,f"insert into validation_tickervalidation (validation_type, strategy_type, ticker_history_id) values ('{indicator}','{strategy_type}', (select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))", auto_commit=False, verbose=False)
    #second, stub in the backtest
    if len(execute_query(connection, f"select * from backtests_backtest where validation_id = (select max(id) from validation_tickervalidation where validation_type='{indicator}' and strategy_type ='{strategy_type}' and ticker_history_id=(select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}') GROUP by validation_type, ticker_history_id, strategy_type)")) < 2:
        execute_update(connection,f"insert into backtests_backtest ( results, validation_id) values ('"+'{}'+f"', (select id from validation_tickervalidation where strategy_type='{strategy_type}' and validation_type='{indicator}' and ticker_history_id=((select id from history_tickerhistory where timestamp={ticker_history[-1].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}') and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}'))  ))", auto_commit=False, verbose=False)

def validate_tickers(position_type, tickers, module_config, client):
    pass


def validate_ticker(position_type, ticker, ticker_history, module_config):
    # if module_config['logging']:
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:${ticker}: Validating taking a {position_type} position in ${ticker}")
    #ok so we need to validate the position type against the indicators to ensure everything is valid to enter that type of position
    results ={
        "sma":validate_sma(position_type, ticker, ticker_history, load_sma(ticker , ticker_history, module_config), module_config),
        "macd":validate_macd(position_type, ticker, ticker_history, load_macd(ticker , ticker_history, module_config), module_config),
        "dmi":validate_dmi(position_type, ticker, ticker_history, load_dmi_adx(ticker , ticker_history, module_config), module_config),
        "adx":validate_adx(position_type, ticker, ticker_history, load_dmi_adx(ticker , ticker_history, module_config), module_config),
        "rsi":validate_rsi(position_type, ticker, ticker_history, load_rsi(ticker , ticker_history, module_config), module_config)

    }
    # if module_config['logging']:
    if module_config['logging']:
        print(json.dumps({k:str(v) for k,v in results.items()}))
    return results
def validate_rsi(position_type, ticker, ticker_history, indicator_data, module_config):
    if module_config['logging']:
        print(f"{datetime.datetime.now()}:{ticker}: {timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d %H:%M:%S')}: Close: {ticker_history[-1].close} RSI: {indicator_data[ticker_history[-1].timestamp]} ")
    if position_type == PositionType.LONG:

        # return indicator_data[ticker_history[-1].timestamp] < module_config['indicator_configs'][Indicator.RSI]['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold']-20
        return indicator_data[ticker_history[-1].timestamp] < module_config['indicator_configs'][Indicator.RSI]['rsi_overbought_threshold']-20
    else:
        return indicator_data[ticker_history[-1].timestamp] > module_config['indicator_configs'][Indicator.RSI]['rsi_oversold_threshold']+20
def validate_adx(position_type, ticker, ticker_history, indicator_data, module_config):
    if module_config['logging']:
        print(f"{datetime.datetime.now()}:{ticker}: {timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d %H:%M:%S')}: Close: {ticker_history[-1].close} ADX[current]: {indicator_data['adx'][ticker_history[-1].timestamp]} ADX[previous]-: {indicator_data['adx'][ticker_history[-2].timestamp]} ")
    return validate_dmi(position_type, ticker, ticker_history, load_dmi_adx(ticker, ticker_history, module_config),module_config) and indicator_data['adx'][ticker_history[-1].timestamp] > module_config['adx_threshold'] and indicator_data['adx'][ticker_history[-1].timestamp] > indicator_data['adx'][ticker_history[-2].timestamp]

def validate_dmi(position_type, ticker, ticker_history, indicator_data, module_config):
    if module_config['logging']:
        print(f"{datetime.datetime.now()}:{ticker}: {timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d %H:%M:%S')}: Close: {ticker_history[-1].close} DMI+: {indicator_data['dmi+'][ticker_history[-1].timestamp]} DMI-: {indicator_data['dmi-'][ticker_history[-1].timestamp]} ADX: {indicator_data['adx'][ticker_history[-1].timestamp]}")
    if position_type == PositionType.LONG:
        return indicator_data['dmi+'][ticker_history[-1].timestamp] > indicator_data['dmi-'][ticker_history[-1].timestamp] and indicator_data['dmi+'][ticker_history[-1].timestamp] > module_config['adx_threshold'] and indicator_data['adx'][ticker_history[-1].timestamp] > module_config['adx_threshold'] and indicator_data['adx'][ticker_history[-1].timestamp] > indicator_data['adx'][ticker_history[-2].timestamp]
    else:
        return indicator_data['dmi+'][ticker_history[-1].timestamp] < indicator_data['dmi-'][ticker_history[-1].timestamp] and indicator_data['dmi-'][ticker_history[-1].timestamp] > module_config['adx_threshold'] and indicator_data['adx'][ticker_history[-1].timestamp] > module_config['adx_threshold'] and indicator_data['adx'][ticker_history[-1].timestamp] > indicator_data['adx'][ticker_history[-2].timestamp]
def validate_macd(position_type, ticker, ticker_history, indicator_data, module_config):
    if module_config['logging']:
        print(f"{datetime.datetime.now()}:{ticker}: {timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d %H:%M:%S')}: Close: {ticker_history[-1].close} MACD: {indicator_data['macd'][ticker_history[-1].timestamp]} Signal: {indicator_data['signal'][ticker_history[-1].timestamp]}")
    if position_type == PositionType.LONG:
        return indicator_data['macd'][ticker_history[-1].timestamp] > indicator_data['signal'][ticker_history[-1].timestamp]
    else:
        return indicator_data['macd'][ticker_history[-1].timestamp] < indicator_data['signal'][ticker_history[-1].timestamp]
def validate_sma(position_type, ticker, ticker_history, indicator_data, module_config):
    ticker_history = ticker_history[:-1]
    if module_config['logging']:
        print(f"{datetime.datetime.now()}:{ticker}: {timestamp_to_datetime(ticker_history[-1].timestamp).strftime('%Y-%m-%d %H:%M:%S')}: Close: {ticker_history[-1].close} SMA: {indicator_data[ticker_history[-1].timestamp]}")
    if position_type == PositionType.LONG:
        return (ticker_history[-1].close > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].low > indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close > ticker_history[-1].open)

    else:
        return (ticker_history[-1].close < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].high < indicator_data[ticker_history[-1].timestamp] and ticker_history[-1].close < ticker_history[-1].open)

def validate_ticker_history_integrity(ticker, ticker_history):
    invalid  = 0
    print(f"Validating {len(ticker_history)} entries")
    for i in range(0, len(ticker_history)-1):
        #case for next day
        current_bar_ts = timestamp_to_datetime(ticker_history[i].timestamp)
        next_bar_ts = timestamp_to_datetime(ticker_history[i+1].timestamp)
        if current_bar_ts.hour == 16 or current_bar_ts.hour == 15 and next_bar_ts.hour != 16:
            #case for close
            if next_bar_ts.hour != 9:
                print(f"${ticker} has gap in timeseries data: Current: {current_bar_ts.strftime('%Y-%m-%d %H:%S:%M')} Next: {next_bar_ts.strftime('%Y-%m-%d %H:%S:%M')} ")
                invalid = invalid +1
        else:
            if next_bar_ts.hour != current_bar_ts.hour+1:
                print(f"${ticker} has gap in timeseries data: Current: {current_bar_ts.strftime('%Y-%m-%d %H:%S:%M')} Next: {next_bar_ts.strftime('%Y-%m-%d %H:%S:%M')} ")
                invalid = invalid + 1


    if invalid > 0:
        print(f"${ticker} has corrupt history data")
    else:
        print(f"${ticker}'s history data is valid")
    #     pass

# def validate_line_similarity(ticker)