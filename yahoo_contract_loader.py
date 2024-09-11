import datetime
import json
import time
import traceback

from contracts import find_nearest_expiration, load_ticker_expiration_dates, find_nearest_strike, load_ticker_strikes, \
    calculate_price_change_impact_contract, \
    calculate_theta_change_impact_contract
from enums import PriceGoalChangeType, PositionType
from history import TickerHistory, load_ticker_history_db
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, write_csv
from indicators import process_ticker_history
import os
import yfinance as yf

from strategy import LongCall, IronButterfly, IronCondor, LongPut


# from common import *

class OptionChainType:
    CALLS = "CALLS"
    PUTS = "PUTS"

class ContractType:
    CALL = 'CALL'
    PUT = 'PUT'




    # history = t.history(start=(datetime.datetime.now()-datetime.timedelta(days=365*30)).strftime("%Y-%m-%d"), end=datetime.datetime.now().strftime('%Y-%m-%d'), interval="1d")
    #ok so processing this is going to be a pain in the ass
    # rows= [['Date', 'Open','Close','High','Low','Volume']]
    # for i in range(0,len(history.index.values)):
    #     ok so write the csv here
        # row = [str(history.index.values[i]).split('T')[0],history['Open'][i],history['Close'][i],history['High'][i], history['Low'][i], history['Volume'][i]]
        # rows.append(row)
        # str(history['Open'].index.values[-1]).split('T')[0]
        # pass
    # write_csv(f"data/yahoo/{ticker}.csv", rows)
    # pass

    # for ticker in module_config['tickers']:
    # if os.path.exists(f"data/yahoo/{ticker}.csv"):
    #     returnhistory
    # print(f"Downloading data for {ticker}")
    # # timestamp = int(int(time.mktime(datetime.datetime.now().timetuple())) * 1e3)
    # timestamp = str(time.time()).split('.')[0]
    # url = f"curl -o data/yahoo/{ticker}.csv https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period2={timestamp}&period1=774921600&interval=1d&events=history&includeAdjustedClose=true"
    # print(url)
    # os.system(url)
    # time.sleep(2) #wait for download
# def process_raw_yahoo_data(raw_data, module_config):
#     #basically here convert realtime to usable data
#     ticker_history=[]
#     if len(raw_data) < 2:
#         return []
#     raw_data[0] = [x.lower() for x in raw_data[0]]
#     print(f"Oldest history record: {raw_data[1][0]} Latest history record: {raw_data[-1][0]} Total Records: {len(raw_data)-1}")
#     for i in range(1, len(raw_data)):
#         #xconvert to timestamp
#         timestamp = int(int(time.mktime(datetime.datetime.strptime(f"{raw_data[i][raw_data[0].index('date')]} 15:30:00", "%Y-%m-%d %H:%M:%S").timetuple()))*1e3)
#
#         round(12.40, ndigits=2)
#         # def __init__(self, open, close, high, low, volume, timestamp):
#         ticker_history.append(TickerHistory(raw_data[i][raw_data[0].index('open')],raw_data[i][raw_data[0].index('close')], raw_data[i][raw_data[0].index('high')], raw_data[i][raw_data[0].index('low')], raw_data[i][raw_data[0].index('volume')], timestamp))
#     return  ticker_history
#     pass
if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("yahoo_contract_loader")
    connection = obtain_db_connection(module_config)

    try:
        print(f"Loading Ticker Data for {len(module_config['tickers'])} tickers")
        # for ticker in tickers:
        for i in range(0, len(module_config['tickers'])):
            ticker=module_config['tickers'][i]
            print(f"Loading data for {ticker}")
            ticker_history = load_ticker_history_db(ticker, module_config, connection)
            for strategy_type in [ IronCondor, LongCall,LongPut, IronButterfly ]:
                # def generate_position(ticker, ticker_history, legs, position_length, module_config):
                # print(str(strategy_type.generate_position(ticker, ticker_history, strategy_type.generate_strikes(ticker_history[-1].close, ticker_history,module_config),10, module_config)))

                # calculate_price_change_for_exit(price_goal_change_type, position_type, asset_price, strike_price,price_goal, ask, dte):
                # def calculate_price_change_for_exit_neutral(contracts, position_cost, asset_price, strike_price,price_goal, dte):
                # for percent in [x for x in range(10,100,10)]:
                for price_goal in [3]:
                    # for position_type  in [PositionType.LONG, PositionType.SHORT]:
                    price_goal_change_type =PriceGoalChangeType.OPTION_THETA_DECAY_DAYS_UNTIL
                    print(f"seeking {price_goal_change_type}: {price_goal} change in {strategy_type}")
                    position = strategy_type.generate_position(ticker, ticker_history, strategy_type.generate_strikes(ticker_history[-1].close, ticker_history,module_config),10, module_config)
                    # position.contracts[0].type = position_type
                    # change_data = calculate_price_change_for_exit_neutral(position.contracts, position.position_cost, ticker_history[-1].close, price_goal, position.contracts[0].dte)
                        # calculate_price_change_impact_leg(price_goal_change_type, contract_type, position_type, asset_price,strike_price, price_goal, contract_last_price, dte)
                        # def calculate_price_change_impact_contract(price_goal_change_type, asset_price, price_goal,contract):
                    # calculate_theta_change_impact_contract(price_goal_change_type, asset_price, price_goal, position_cost, contracts):
                    results = calculate_theta_change_impact_contract(price_goal_change_type, ticker_history[-1].close, price_goal,position.position_cost, position.contracts)
                    # results = calculate_price_change_impact_leg(PriceGoalChangeType.ASSET_PERCENTAGE, position.contracts[0].contract_type, position.contracts[0].type, ticker_history[-1].close,position.contracts[0].strike,price_goal, position.contracts[0].last_price, position.contracts[0].dte)
                    # print(f"{price_goal}% change on {ticker} {position.expiration}  {strategy_type}: New Contract Price: {results['new_contract_price']}: Old Contract Price{results['original_position_price']}: Original Underlying Price: {ticker_history[-1].close}" )
                    print(f"{price_goal_change_type}:{price_goal} change on {ticker} {position.expiration}  {strategy_type}: New Contract Price: {results['new_contract_price']}: Old Contract Price{results['original_position_price']}: Original Underlying Price: {ticker_history[-1].close}: Days to realization: {results['days_to_target']}: Realzed on Contract DTE:{results['dte']}" )
                # print(f"{ticker} {position.expiration} {position.strategy} days to decay {price_goal}% of original position cost {position.position_cost}: {change_data['days_to_target']} days, position value {change_data['position_cost']}:Original Position Theta: {position.theta/100} Theta after {change_data['days_to_target']} days:{change_data['greeks']['theta']/100}")
                # print(calculate_price_change_for_exit(PriceGoalChangeType.OPTION_PERCENTAGE, PositionType.LONG,ticker_history[-1].close, position.contracts[0].strike, 20, position.contracts[0].last_price, position.contracts[0].dte))
                # with open(f"html/{ticker}_{position.strategy}.html", "w+") as f:
                #     f.write(position.build_html_representation())
            # download_raw_yahoo_contract_data(ticker,module_config)
            #     expiration = find_nearest_expiration(10, load_ticker_expiration_dates(ticker, module_config), module_config)
            # for contract_type in [ContractType.CALL, ContractType.PUT]:
            #     closest_strike = find_nearest_strike(ticker_history[-1].close, contract_type, load_ticker_strikes(ticker, find_nearest_expiration(10, load_ticker_expiration_dates(ticker, module_config), module_config), module_config), module_config)
            #     t = yf.Ticker(ticker)
            #     t.option_chain(expiration)
            #     print(f"Nearest 10 Day {contract_type} strike to {ticker_history[-1].close} is {closest_strike}")
            #     pass
            # raw_data = read_csv(f"{module_config['data_dir']}/{ticker}.csv")
            # ticker_history = process_raw_yahoo_data(raw_data, module_config)
            # write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, auto_commit=True, cache=False)
            # os.system(f"rm data/yahoo/{ticker}.csv")
        connection.close()

    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Initial Database Load in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")