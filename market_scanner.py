import datetime
import time
import traceback
from history import TickerHistory, load_ticker_history_db
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, \
    calculate_x_is_what_percentage_of_y, calculate_what_is_x_percentage_of_y
from indicators import process_ticker_history
from strategy import IronCondor
from backtest import perform_backtest, print_backtest_results

# from common import *

if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("market_scanner")
    connection = obtain_db_connection(module_config)

    try:
        for ticker in module_config['tickers']:
            print(f"Running scan of {ticker}")
        # print(int(calculate_what_is_x_percentage_of_y(1,542)))
            ticker_history = load_ticker_history_db(ticker, module_config,connection)
            process_ticker_history(connection, ticker, ticker_history, module_config,  process_alerts=True)
            write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, cache=False)
        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Market Scan in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")