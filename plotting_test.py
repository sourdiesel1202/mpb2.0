import time
from zoneinfo import ZoneInfo

# from enums import AlertType, PositionType
# from backtest import backtest_ticker, load_backtest_ticker_data, backtest_ticker_concurrently, load_backtest_results, analyze_backtest_results
import polygon, datetime

from backtest import perform_backtest, print_backtest_results
from history import load_ticker_history_raw,load_ticker_history_db
# from validation import validate_ticker
from functions import load_module_config, get_today, obtain_db_connection

# module_config = load_module_config(__file__.split("/")[-1].split(".py")[0])
module_config = load_module_config('plotting_test')
# from shape import compare_tickers
from plotting import plot_ticker_with_indicators, plot_indicator_data, plot_indicator_data_dual_y_axis, plot_sr_lines, \
    build_indicator_dict, build_strategy_dict, plot_ticker_with_indicators_and_positions

if __name__ == '__main__':
    start_time = time.time()
    # client = polygon.RESTClient(api_key=module_config['api_key'])

    # module_config['logging']=True
    for ticker_a in module_config['tickers']:
        # for ticker_b in module_config['compare_tickers']:
        print(f"Beginning plotting test of ${ticker_a}")
        connection = obtain_db_connection(module_config)
        ticker_history_a = load_ticker_history_db(ticker_a, module_config, connection=connection)
        backtest_results = perform_backtest(ticker_a, ticker_history_a, module_config['strategy'], module_config,connection=connection)
        print_backtest_results(ticker_a,backtest_results, module_config)
        plot_ticker_with_indicators_and_positions(ticker_a,ticker_history_a,build_indicator_dict(ticker_a, ticker_history_a, module_config,connection),build_strategy_dict(ticker_a, ticker_history_a,backtest_results['positions'], module_config), module_config)

    print(f"\nCompleted plotting test of ({','.join([f'${x}' for x in module_config['tickers']])}) in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")