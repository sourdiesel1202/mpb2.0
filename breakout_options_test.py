import datetime
import time
import traceback

import pandas as pd

from history import TickerHistory, load_ticker_history_db
from history import write_ticker_history_db_entries,load_ticker_histories_data_frame
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, \
    calculate_x_is_what_percentage_of_y, calculate_what_is_x_percentage_of_y
from indicators import process_ticker_history
from strategy import IronCondor
from backtest import perform_backtest, print_backtest_results

# from common import *
strategy_name = 'Breakout'
days_to_backtest = 1

def breakout(ticker,df):
    # assign variables
    high = df.loc[:, df.columns.get_level_values(1).isin(['High'])].droplevel(1, axis='columns')
    low = df.loc[:, df.columns.get_level_values(1).isin(['Low'])].droplevel(1, axis='columns')
    close = df.loc[:, df.columns.get_level_values(1).isin(['Close'])].droplevel(1, axis='columns')
    #
    upper = high * (1 + 4 * (high - low) / (high + low))
    lower = low * (1 - 4 * (high - low) / (high + low))
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
    df = pd.concat([xo,xu,calls_hit,puts_hit], axis=1, keys=['xo','xu','calls_hit','puts_hit']).swaplevel(axis=1)
    symbols = df.columns.get_level_values(0).unique().sort_values(ascending=True)
    now = datetime.datetime.now()
    today = now.strftime('%Y-%m-%d')
    picks = []
    if ticker in symbols:
        return True
    if df[ticker].tail(1)['xo'].bool():
        contract_type = 'call'
        cross = 'xo'
    elif df[ticker].tail(1)['xu'].bool():
        pass
        # contract_type = 'put'
        # cross = 'xu'

if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("breakout_options_test")
    connection = obtain_db_connection(module_config)

    try:
        for ticker in module_config['tickers']:
            print(f"Test of {ticker}")
        # print(int(calculate_what_is_x_percentage_of_y(1,542)))
            ticker_history = load_ticker_history_db(ticker, module_config,connection)
            df =load_ticker_histories_data_frame([ticker],connection, module_config)
            breakout_results = breakout(ticker,df)
            process_ticker_history(connection, ticker, ticker_history, module_config,  process_alerts=True)

            write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, cache=False)
        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Market Scan in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")