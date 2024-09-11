
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
    module_config = load_module_config("market_scanner")
    connection = obtain_db_connection(module_config)

    try:
        tickers = execute_query(connection, 'select distinct symbol from tickers_ticker')
        tickers = [tickers[i][0] for i in range(1, len(tickers))]
        for ticker in tickers:
            # print(f"Running Backtest of {ticker}")
            sql = f"delete from backtests_backtest where ticker_id=(select id from tickers_ticker where symbol='{ticker}')"
            execute_update(connection, sql, auto_commit=True, verbose=True, cache=False)
        # print(int(calculate_what_is_x_percentage_of_y(1,542)))
        #     ticker_history = load_ticker_history_db(ticker, module_config,connection)
        #     backtest_results  = perform_backtest(ticker, ticker_history, "IRON_CONDOR", module_config,connection)
        #     json_positions = [x.serialize() for x in backtest_results['positions']]
        #     positions = [IronCondor.deserialize(x) for x in json_positions]
        #     print_backtest_results(ticker,backtest_results, module_config)

        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Backtest in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")
