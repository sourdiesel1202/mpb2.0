import datetime
import time
import traceback
from history import TickerHistory, load_ticker_history_db
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, \
    calculate_x_is_what_percentage_of_y, calculate_what_is_x_percentage_of_y, execute_update
from indicators import process_ticker_history
from strategy import IronCondor
from backtest import perform_backtest, print_backtest_results

# from common import *

if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("yahoo_data_loader")
    connection = obtain_db_connection(module_config)

    try:

        for i in range(0, len(module_config['tickers'])):
            ticker = module_config['tickers'][i]
            print(f"Cleaning up data for {i+1}/{len(module_config['tickers'])}: {ticker}")
            sql = f"delete  from history_tickerhistory where ticker_id= (select id from tickers_ticker where symbol ='{ticker}')  and date_format(from_unixtime(timestamp/1000), '%Y-%m-%d %H:%i:%S')  not like '%15:30:00' order by timestamp desc"
            execute_update(connection, sql, auto_commit=True,cache=False)
            # backtest_runner.py
        #     print(f"Running Backtest of {ticker}")
        # # print(int(calculate_what_is_x_percentage_of_y(1,542)))
        #     ticker_history = load_ticker_history_db(ticker, module_config,connection)
        #     print_backtest_results(ticker,perform_backtest(ticker, ticker_history, "IRON_CONDOR", module_config,connection), module_config)

        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Backtest in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")