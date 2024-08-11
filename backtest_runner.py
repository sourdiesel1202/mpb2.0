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
    module_config = load_module_config("backtest_runner")
    connection = obtain_db_connection(module_config)

    try:

        for ticker in module_config['tickers']:
            print(f"Running Backtest of {ticker}")
        # print(int(calculate_what_is_x_percentage_of_y(1,542)))
            ticker_history = load_ticker_history_db(ticker, module_config,connection)
            print_backtest_results(ticker,perform_backtest(ticker, ticker_history, "IRON_CONDOR", module_config,connection), module_config)
            # IronCondor.generate_strikes(ticker_history[-1].close,module_config )
        # tickers = module_config['tickers']
        # print(f"Loading Ticker Data for {len(tickers)} tickers")
        # for ticker in tickers:
        #     print(f"SPYLoading data for {ticker}")
        #     raw_data = read_csv(f"{module_config['data_dir']}/{ticker}.csv")
        #     ticker_history = load_ticker_history_db()
        #     write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, cache=False)
        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Backtest in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")