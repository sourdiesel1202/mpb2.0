import datetime
import json
import os
import time
import traceback
from collections import ChainMap
from copy import copy

from history import TickerHistory, load_ticker_history_db
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, \
    calculate_x_is_what_percentage_of_y, calculate_what_is_x_percentage_of_y, all_possible_combinations, \
    get_permutations, write_csv, human_readable_datetime, process_list_concurrently, combine_csvs
from indicators import process_ticker_history
from strategy import IronCondor
from backtest import perform_backtest, print_backtest_results


# from common import *
class AutomatedBacktest:
    def __init__(self, ticker, ticker_history, strategy, indicators, module_config):
        self.ticker = ticker
        self.ticker_history = ticker_history
        self.strategy = strategy
        self.indicators = indicators
        self.module_config = module_config

    def perform_backtest(self,connection):
        backtest_results = perform_backtest(self.ticker, self.ticker_history, self.strategy, self.module_config,connection)
        return [self.ticker, self.strategy, '|'.join(self.indicators), len(backtest_results['positions']),backtest_results['winners'], backtest_results['losers'],calculate_x_is_what_percentage_of_y(backtest_results['winners'],len(backtest_results['positions'])),json.dumps(self.module_config['indicator_configs']),json.dumps(self.module_config['strategy_configs'])]


def set_base_indicator_values( module_config):
    for indicator in module_config['indicators']:
        for field, min_max in module_config['indicator_manipulations'][indicator]['integer_values'].items():
            module_config['indicator_configs'][indicator][field]=min_max[0]
def generate_strategy_config_combinations(strategy, module_config):
    # first generate the list of possible values
    if 'integer_values' in module_config['strategy_manipulations'][strategy]:
        integer_field_values = []
        for integer_field, value_range in module_config['strategy_manipulations'][strategy]['integer_values'].items():
            integer_field_values.append([])
            # if integer_field not in integer_field_values:
            #     integer_field_values[-1]=

            for i in range(module_config['strategy_manipulations'][strategy]['integer_values'][integer_field][0],
                           module_config['strategy_manipulations'][strategy]['integer_values'][integer_field][1]):
                integer_field_values[-1].append({integer_field: i})
                # in
        integer_permutations = get_permutations(integer_field_values)

    else:
        integer_permutations = [{}]  # if there are none add one as the basis for  the generation
    config_combinations = []
    for i in range(0, len(integer_permutations)):
        # new_permutation = copy(integer_permutations[i])
        new_permutation = {}
        for perm in integer_permutations[i]:
            for k, v in perm.items():
                new_permutation[k] = copy(v)

        # for inverse_flag in [True, False]:
        #     for require_alert_flag in [True, False]:
        #         new_permutation = {}
        #         for perm in integer_permutation:
        #             for k, v in perm.items():
        #                 new_permutation[k] = copy(v)
        #         new_permutation = copy(new_permutation)
        #         new_permutation['inverse'] = inverse_flag
        #         new_permutation['require_alert_type'] = require_alert_flag
        #         if not require_alert_flag:
        #             # continue
        #             config_combinations.append(copy(new_permutation))
        #             continue

        for strategy_generation_type in ["PERCENTAGE"]:
            # tmp = alert_type
            new_permutation['strategy_generation_type'] = copy(strategy_generation_type)
            config_combinations.append(copy(new_permutation))

    return config_combinations
def generate_indicator_config_combinations(indicator, module_config):
    #first generate the list of possible values
    if 'integer_values' in module_config['indicator_manipulations'][indicator]:
        integer_field_values = []
        for integer_field, value_range in module_config['indicator_manipulations'][indicator]['integer_values'].items():
            integer_field_values.append([])
            # if integer_field not in integer_field_values:
            #     integer_field_values[-1]=

            for i in range(module_config['indicator_manipulations'][indicator]['integer_values'][integer_field][0],module_config['indicator_manipulations'][indicator]['integer_values'][integer_field][1]):
                integer_field_values[-1].append({integer_field:i})
                # in
        integer_permutations = get_permutations(integer_field_values)

    else:
        integer_permutations = [{}]# if there are none add one as the basis for  the generation
    config_combinations = []
    for i in range(0, len(integer_permutations)):
        integer_permutation= copy(integer_permutations[i])

        for inverse_flag in [True, False]:
            for require_alert_flag in [True, False]:
                new_permutation = {}
                for perm in integer_permutation:
                    for k, v in perm.items():
                        new_permutation[k] = copy(v)
                new_permutation = copy(new_permutation)
                new_permutation['inverse'] = inverse_flag
                new_permutation['require_alert_type'] = require_alert_flag
                if not require_alert_flag:
                    # continue
                    config_combinations.append(copy(new_permutation))
                    continue

                for alert_type in module_config['indicator_manipulations'][indicator]['required_alerts']:
                    tmp = alert_type
                    new_permutation['alert_type'] =copy(tmp)
                    config_combinations.append(copy(new_permutation))

    return config_combinations
    #then generate the list with inverse flag

    #last generate the list with the different required alerts
def generate_strategy_configs( module_config):
    strategy_combinations = []
    for strategy in module_config['strategies']:
        strategy_combinations.append(generate_strategy_config_combinations(strategy,module_config))

    strategy_permutations = get_permutations(strategy_combinations)
    strategy_configs = []
    # strategy_configs = []
    for permutation in strategy_permutations:
        strategy_config ={}
        for i in range(0, len(module_config['strategies'])):
            strategy_config[module_config['strategies'][i]] = permutation[i]
        strategy_configs.append(strategy_config)
    return  strategy_configs
def generate_indicator_configs( module_config):
    #so the idea here is that we'll generate a list of all possible indicator_configs values based upon
    #the base values set
    indicator_combinations = []

    for indicator in module_config['automate_indicators']:
        indicator_combinations.append(generate_indicator_config_combinations(indicator, module_config))
    indicator_permutations = get_permutations(indicator_combinations)
    indicator_configs = []
    for permutation in indicator_permutations:
        new_config = {}
        for i in range(0, len(module_config['automate_indicators'])):
            new_config[module_config['automate_indicators'][i]]=permutation[i]
        indicator_configs.append(new_config)
    return indicator_configs


def _perform_automated_subprocess_backtest(backtest_objects):
    _connection = obtain_db_connection(backtest_objects[0].module_config)
    try:
        backtest_report = [["ticker", "strategy", "indicators", "total_positions", "winners", "losers", "percentage", "indicator_configs","strategy_configs"]]
        for backtest_object in backtest_objects:
            backtest_report.append(backtest_object.perform_backtest(_connection))
        write_csv(f"data/backtests/{os.getpid()}backtest.csv", backtest_report)
        _connection.close()
    except Exception as e:
        traceback.print_exc()
        _connection.close()
        raise e

def _perform_automated_backtest( module_config, connection):
    # pass
    #ok so here is where we will do the automated backtest
    #not sure of the best way to do this
    #generate test indicator configs
    indicator_configs = generate_indicator_configs(module_config)
    #now generate test strategy configs
    strategy_configs = generate_strategy_configs(module_config)
    #now the fun part
    #iterate over the strategy configs

    backtests = []
    for ticker in module_config['tickers']:
        ticker_history = load_ticker_history_db(ticker, module_config, connection)
        for strategy_config in strategy_configs:

            for strategy in module_config['strategies']:
                #then run each unique strategy config per strategy
                for indicator_config in indicator_configs:
                    #iterate over the indicator configs
                    for indicator_list in [x for x in all_possible_combinations(module_config['automate_indicators']) if len(x) > 0]:
                        #then over the potential indicator configs
                        #so now we can stub in
                        tmp_module_config = copy(module_config)
                        tmp_module_config['strategy_configs']=strategy_config
                        tmp_module_config['strategy'] = strategy
                        tmp_module_config['indicator_configs']=indicator_config
                        tmp_module_config['indicators']=indicator_list
                        tmp_module_config['use_indicators']=True

                        backtests.append(AutomatedBacktest(ticker, ticker_history, strategy,indicator_list,  tmp_module_config))
                        # backtest_results = perform_backtest(ticker, ticker_history, strategy, tmp_module_config,connection)
                        # backtest_report.append([ticker,strategy,'|'.join(indicator_list),len(backtest_results['positions']), backtest_results['winners'], backtest_results['losers'],calculate_x_is_what_percentage_of_y(backtest_results['winners'], len(backtest_results['positions'])), json.dumps(indicator_config), json.dumps(strategy_config)])
    # return backtest_report
    # for field, min_max in module_config['indicator_manipulations'][indicator]['integer_values'].items()
    pids = process_list_concurrently(backtests, _perform_automated_subprocess_backtest,int(len(backtests)/module_config['num_processes']))
    files = [f"data/backtests/{pid}backtest.csv" for pid in pids]
    write_csv(f"data/backtests/backtest.csv",combine_csvs(files))
    # pass
if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("automated_backtest_runner")
    connection = obtain_db_connection(module_config)

    try:
        module_config['automate_indicators']=[x for x in module_config['indicator_manipulations'].keys()
                                              ]
        _perform_automated_backtest( module_config, connection)

        connection.close()


    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Backtest in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")