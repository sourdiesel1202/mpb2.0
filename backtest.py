from enums import StrategyType, Indicator, InventoryFunctionTypes
from functions import calculate_x_is_what_percentage_of_y
from history import load_ticker_history_pd_frame
from strategy import IronCondor, Strategy, LongCall, LongPut
from stockstats import wrap
from indicators import load_rsi, get_indicator_inventory, load_macd, load_sma, load_dmi_adx


def perform_backtest(ticker, ticker_history, strategy_type, module_config):
    if strategy_type==StrategyType.IRON_CONDOR:
        if not module_config['use_indicators']:
            return backtest_strategy(ticker, ticker_history, module_config)
        else:
            return backtest_strategy_with_indicators(ticker, ticker_history, module_config)


def backtest_strategy(ticker, ticker_history, module_config):
    results = {"winners": 0, "losers": 0, "total_positions": 0}
    last_position_index = 0
    for i in range(0, len(ticker_history)):
        if i < module_config['position_length']:
            continue
        else:

            strategy = Strategy()
            if module_config['strategy'] == StrategyType.IRON_CONDOR:
                strategy = IronCondor(*IronCondor.generate_strikes(ticker_history[i - module_config['position_length']],module_config))
            elif module_config['strategy'] == StrategyType.LONG_CALL:
                strategy = LongCall(ticker_history[i - module_config['position_length']])
            elif module_config['strategy'] == StrategyType.LONG_PUT:
                strategy = LongPut(ticker_history[i - module_config['position_length']])

            # strategy = IronCondor(*IronCondor.generate_strikes(ticker_history[i-module_config['strategy_configs']['iron_condor']['position_length']], module_config))
            if strategy.is_profitable(ticker_history[i].close, module_config):
                # if modul/e_config['logging']:
                print(f'Opened profitable {type(strategy)} on {ticker_history[i - module_config["position_length"]].dt} ${ticker}:{ticker_history[i - module_config["position_length"]].close} exited position on {ticker_history[i].dt} ${ticker}:{ticker_history[i].close}')
                results['winners'] += 1
            else:
                results['losers'] += 1
            results['total_positions'] += 1
        # if last_position_index
        pass
    if module_config['logging']:
        print_backtest_results(results,  module_config)
    return results

def backtest_strategy_with_indicators(ticker, ticker_history, module_config):
    results = {"winners":0, "losers":0, "total_positions":0}
    last_position_index =0
    # for i in range(0, len(ticker_history),module_config['strategy_configs']['iron_condor']['position_length']):
    indicator_data = load_indicator_data(ticker, ticker_history, module_config)

    for i in range(module_config['indicator_initial_range'], len(ticker_history),module_config['position_length']):
        if i < module_config['position_length']+module_config['indicator_initial_range']:
            continue
        else:
            if not did_ticker_trigger_alerts_at_index(ticker, ticker_history, indicator_data, i, module_config):
                continue
            strategy = Strategy()
            #determine strategy type
            if module_config['strategy'] == StrategyType.IRON_CONDOR:
                strategy = IronCondor(*IronCondor.generate_strikes(ticker_history[i-module_config['position_length']], module_config))
            elif module_config['strategy'] == StrategyType.LONG_CALL:
                strategy = LongCall(ticker_history[i-module_config['position_length']])
            elif module_config['strategy'] == StrategyType.LONG_PUT:
                strategy = LongPut(ticker_history[i-module_config['position_length']])
                # strategy = LongCall(ticker_history[i-module_config['position_length']])
            if strategy.is_profitable(ticker_history[i].close, module_config):
                results['winners'] += 1
            else:
                results['losers'] += 1
            results['total_positions'] +=1
        # if last_position_index
        pass
    if module_config['logging']:
        print_backtest_results(results, module_config)
    return  results

# def backtest_iron_condor_with_indicators(ticker, ticker_history, module_config):
#     results = {"winners":0, "losers":0, "total_positions":0}
#     rsi= wrap(load_ticker_history_pd_frame(ticker, ticker_history))['rsi']


def did_ticker_trigger_alerts_at_index(ticker, ticker_history,indicator_data, index, module_config):
    tmp_ticker_history = ticker_history[:-(len(ticker_history)-1-index)]
    indicator_inventory = get_indicator_inventory()
    for indicator in module_config['indicators']:
        # def did_rsi_alert(indicator_data, ticker, ticker_history, module_config, connection=None):
        if not indicator_inventory[indicator][InventoryFunctionTypes.DID_ALERT](indicator_data[indicator], ticker, tmp_ticker_history, module_config):
            #so basically this allows us to say it alerted when the indicator value is outside of the range, an event DIDN"T happen, etc
            #use case for this is RSI, if inverse then flags as alerting when the rsi is between the oversold and overbought value
            if not module_config['indicator_configs'][indicator]['inverse']:
                return False
            else:
                continue
        else:
            if module_config['indicator_configs'][indicator]['inverse']:
                return False
            if module_config['indicator_configs'][indicator]['require_alert_type']:
                alert_type = indicator_inventory[indicator][InventoryFunctionTypes.DETERMINE_ALERT_TYPE](indicator_data[indicator], ticker, tmp_ticker_history, module_config)
                if module_config['indicator_configs'][indicator]['alert_type'] != alert_type:
                # print(f"{ticker} alerted {indicator}:{alert_type} value {indicator_data[indicator][ticker_history[index].timestamp]} on {ticker_history[index].dt}")
                    return  False
                # print(f"{ticker} alerted {indicator}:{alert_type} value {indicator_data[indicator][ticker_history[index].timestamp]} on {ticker_history[index].dt}")
                # print(f"{ticker} alerted {indicator}:{alert_type} value {indicator_data[indicator]['macd'][ticker_history[index].timestamp]} on {ticker_history[index].dt}")
        if module_config['logging']:
            print(f"{ticker} alerted {indicator} on {ticker_history[index].dt}")
    # for indicator in module_config['indicators']:



    return True

def load_indicator_data(ticker, ticker_history, module_config):
    '''
    Retuns a dict of pd frames with the indicator as the key
    :param ticker:
    :param ticker_history:
    :param module_config:
    :return:
    '''
    indicator_dict= {}
    for indicator in module_config['indicators']:
        if indicator==Indicator.RSI:
            indicator_dict[indicator]=load_rsi(ticker, ticker_history, module_config)
        if indicator == Indicator.MACD:
            indicator_dict[indicator]=load_macd(ticker, ticker_history, module_config)
        if indicator == Indicator.SMA:
            indicator_dict[indicator]=load_sma(ticker, ticker_history, module_config)
        if indicator in  [Indicator.ADX, Indicator.DMI, Indicator.ADX_REVERSAL]:
            indicator_dict[indicator]=load_dmi_adx(ticker, ticker_history, module_config)



    #so basically here we want to iterate through the history and find the indexes that meet the criteria
    return  indicator_dict

def print_backtest_results(results,  module_config):
    if module_config['strategy'] == StrategyType.IRON_CONDOR:
        print(f"Sold {results['total_positions']} iron condors held for {module_config['position_length']} days")
        print(f"Strategy generation type: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_type']}| Value: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_value']}")
        print(f"{results['winners']} winners, {results['losers']} losers. The strategy is successful {calculate_x_is_what_percentage_of_y(results['winners'], results['total_positions'])}% of the time ")
    if module_config['strategy'] == StrategyType.LONG_CALL or module_config['strategy'] == StrategyType.LONG_PUT:
        print(f"Bought {results['total_positions']} {module_config['strategy']}s held for {module_config['position_length']} days")
        # print(f"Strategy generation type: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_type']}| Value: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_value']}")
        print(f"{results['winners']} winners, {results['losers']} losers. The strategy is successful {calculate_x_is_what_percentage_of_y(results['winners'], results['total_positions'])}% of the time ")
