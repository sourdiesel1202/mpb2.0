import datetime
import json
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
def _write_backtest(ticker, ticker_history,  module_config, connection):
    sql = f"select distinct i.name indicator,ta.alert_type, t.symbol, from_unixtime(max(th.timestamp)/1000) last_alerted from alerts_indicatoralert ta,indicators_indicator i, history_tickerhistory th, tickers_ticker t where i.id=ta.indicator_id and t.symbol='{ticker}' and  t.id=th.ticker_id and th.timespan='{module_config['timespan']}' and th.timespan_multiplier='{module_config['timespan_multiplier']}' and th.id =ta.ticker_history_id group by i.name, ta.alert_type, t.symbol"
    alerts = execute_query(connection, sql)
    for strategy in ['LONG_CALL', 'LONG_PUT']:
        for i in range(1, len(alerts)):
            module_config['indicators'] =[alerts[i][0]]
            module_config['strategy'] =strategy
            module_config['use_indicators'] =True

            module_config['indicator_configs'][alerts[i][0]]['require_alert_type'] = True
            module_config['indicator_configs'][alerts[i][0]]['alert_type'] = alerts[i][1]

            for length in [5,10,15,20]:
                module_config['strategy_configs'][strategy]['position_length'] = length
                if len(execute_query(connection,f"select id from backtests_backtest where ticker_id = (select id from tickers_ticker where symbol='{ticker}') and indicator_id = (select id from indicators_indicator where name='{alerts[i][0]}') and alert_type= '{alerts[i][1]}' and strategy_id =(select id from strategies_strategy where name='{strategy}') and position_length={length}")) == 1:
                    print(f"Backtesting {length} day {strategy}s on {ticker} for {alerts[i][0]}:{alerts[i][1]}")

                    backtest_results = perform_backtest(ticker, ticker_history, strategy, module_config, connection)
                    if len(backtest_results['positions']) == 0:
                        print(f"No backtest positions for {ticker}:{alerts[i][0]}:{alerts[i][1]}")
                        continue
                    else:
                        print(f"Got backtest results for {ticker} for {alerts[i][0]}:{alerts[i][1]}")
                        attributes = {'positions': [x.serialize() for x in backtest_results['positions']]}
                        update_sql = f"insert ignore into backtests_backtest (ticker_id, indicator_id, alert_type,strategy_id,position_length,inverse, require_alert_type,winners,losers,total_positions,success_rate,attributes) values ((select id from tickers_ticker where symbol='{ticker}'), (select id from indicators_indicator where name='{alerts[i][0]}'),'{alerts[i][1]}',(select id from strategies_strategy where name='{strategy}'),{length},{module_config['indicator_configs'][alerts[i][0]]['inverse']},{module_config['indicator_configs'][alerts[i][0]]['require_alert_type']},{backtest_results['winners']},{backtest_results['losers']},{len(backtest_results['positions'])},{calculate_x_is_what_percentage_of_y(backtest_results['winners'], len(backtest_results['positions']))},'{json.dumps(attributes)}' )"
                        execute_update(connection, update_sql, auto_commit=True, cache=False)
                else:
                    print(f"Already backtested {length} day {strategy}s on {ticker} for {alerts[i][0]}:{alerts[i][1]} ")
            #now we get it sexxy (get it sexxy, get it sexxy)
            # print(update_sql)
if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("market_scanner")
    connection = obtain_db_connection(module_config)

    try:
        for ticker in module_config['tickers']:
            print(f"Running scan of {ticker}")
        # print(int(calculate_what_is_x_percentage_of_y(1,542)))
            ticker_history = load_ticker_history_db(ticker, module_config,connection)
            if len(ticker_history) < module_config['indicator_initial_range']:
                print(f"{ticker} has ticker history periods of: {len(ticker_history)}, minimum is {module_config['indicator_initial_range']}. Skipping")
                continue
            process_ticker_history(connection, ticker, ticker_history, module_config,  process_alerts=True)
            _write_backtest(ticker, ticker_history, module_config, connection)
            # print_backtest_results(ticker,perform_backtest(ticker, ticker_history, "IRON_CONDOR", module_config, connection),module_config)
            # write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, cache=False)
        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Market Scan in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")