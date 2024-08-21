from enums import StrategyType, Indicator, InventoryFunctionTypes
from functions import calculate_x_is_what_percentage_of_y
from history import load_ticker_history_pd_frame
from strategy import IronCondor, Strategy, LongCall, LongPut
from stockstats import wrap
from indicators import load_rsi, get_indicator_inventory, load_macd, load_sma, load_dmi_adx, load_breakout, \
    load_death_cross, load_golden_cross, load_support_resistance, load_vix_rsi, load_stochastic_rsi, \
    load_breakout_predict, load_breakout_longterm, load_adx_crossover, load_current_breakout, load_extreme_rsi, \
    load_extreme_rsi_alternative, load_momentum_quick, load_rsi_reversal, load_adx_reversal, \
    load_adx_reversal_alternative


def perform_backtest(ticker, ticker_history, strategy_type, module_config, connection=None):
    if not module_config['use_indicators']:
        return backtest_strategy(ticker, ticker_history, module_config)
    else:
        return backtest_strategy_with_indicators(ticker, ticker_history, module_config, connection=connection)


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

def backtest_strategy_with_indicators(ticker, ticker_history, module_config, connection=None):
    results = {"winners":0, "losers":0,  "positions":[]}
    last_position_index =0
    # for i in range(0, len(ticker_history),module_config['strategy_configs']['iron_condor']['position_length']):
    indicator_data = load_indicator_data(ticker, ticker_history, module_config, connection)

    for i in range(module_config['indicator_initial_range'], len(ticker_history)-module_config['strategy_configs'][module_config['strategy']]['position_length']):
        if i < module_config['strategy_configs'][module_config['strategy']]['position_length']+module_config['indicator_initial_range']:
            continue
        else:
            # print(f"Backtesting {ticker} at {ticker_history[i].dt}")
            if module_config['logging']:
                print(f"Backtesting {ticker} at {ticker_history[i].dt}")
            if not did_ticker_trigger_alerts_at_index(ticker, ticker_history,indicator_data,  i, module_config):
                continue
            strategy = Strategy(ticker_history[i-module_config['strategy_configs'][module_config['strategy']]['position_length']].dt, module_config['strategy_configs'][module_config['strategy']]['position_length'])
            #determine strategy type
            if module_config['strategy'] == StrategyType.IRON_CONDOR:
                strategy = IronCondor(ticker_history[i].close,*IronCondor.generate_strikes(ticker_history[i].close, ticker_history[:-(len(ticker_history)-1-i)], module_config),ticker_history[i].dt, module_config['strategy_configs'][module_config['strategy']]['position_length'])
            elif module_config['strategy'] == StrategyType.LONG_CALL:
                strategy = LongCall(ticker_history[i],ticker_history[i].dt, module_config['strategy_configs'][module_config['strategy']]['position_length'])
            elif module_config['strategy'] == StrategyType.LONG_PUT:
                strategy = LongPut(ticker_history[i],ticker_history[i].dt, module_config['strategy_configs'][module_config['strategy']]['position_length'])
                # strategy = LongCall(ticker_history[i-module_config['strategy_configs'][module_config['strategy']]['position_length']])
            results['positions'].append(strategy)
            if strategy.is_profitable(ticker_history[i+module_config['strategy_configs'][module_config['strategy']]['position_length']], module_config):
                results['winners'] += 1
            else:
                results['losers'] += 1
            # results['total_positions'] +=1
        # if last_position_index
        pass
    if module_config['logging']:
        print_backtest_results(ticker,results, module_config)
    return  results

# def backtest_iron_condor_with_indicators(ticker, ticker_history, module_config):
#     results = {"winners":0, "losers":0, "total_positions":0}
#     rsi= wrap(load_ticker_history_pd_frame(ticker, ticker_history))['rsi']


def did_ticker_trigger_alerts_at_index(ticker, ticker_history, indicator_data,index, module_config):
    tmp_ticker_history = ticker_history[:-(len(ticker_history)-1-index)]
    indicator_inventory = get_indicator_inventory()
    if module_config['logging']:
        print(f"Getting {ticker} indicator value at {tmp_ticker_history[-1].dt}")
    for indicator in module_config['indicators']:
        # def did_rsi_alert(indicator_data, ticker, ticker_history, module_config, connection=None):
        # def load_breakout(ticker, ticker_history, module_config, connection=None):
        # indicator_data = indicator_inventory[indicator][InventoryFunctionTypes.LOAD](ticker,tmp_ticker_history,module_config,connection)
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

def load_indicator_data(ticker, ticker_history, module_config, connection):
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
        if indicator == Indicator.BREAKOUT:
            indicator_dict[indicator]=load_breakout(ticker, ticker_history, module_config)
        if indicator == Indicator.DEATH_CROSS:
            indicator_dict[indicator]=load_death_cross(ticker, ticker_history, module_config)
        if indicator == Indicator.GOLDEN_CROSS:
            indicator_dict[indicator]=load_golden_cross(ticker, ticker_history, module_config)
        if indicator == Indicator.SUPPORT_RESISTANCE:
            indicator_dict[indicator]=load_support_resistance(ticker, ticker_history, module_config)
        if indicator == Indicator.VIX_RSI:
            indicator_dict[indicator]=load_vix_rsi(ticker, ticker_history, module_config, connection=connection)
        if indicator == Indicator.STOCHASTIC_RSI:
            indicator_dict[indicator]=load_stochastic_rsi(ticker, ticker_history, module_config, connection=connection)
        if indicator in  [Indicator.ADX, Indicator.DMI]:
            indicator_dict[indicator]=load_dmi_adx(ticker, ticker_history, module_config)
        if indicator == Indicator.ADX_REVERSAL:
            indicator_dict[indicator]=load_adx_reversal(ticker, ticker_history, module_config)
        if indicator == Indicator.BREAKOUT_PREDICT:
            indicator_dict[indicator]=load_breakout_predict(ticker, ticker_history, module_config)
        if indicator == Indicator.BREAKOUT_LONGTERM:
            indicator_dict[indicator]=load_breakout_longterm(ticker, ticker_history, module_config)
        if indicator == Indicator.ADX_CROSSOVER:
            indicator_dict[indicator]=load_adx_crossover(ticker, ticker_history, module_config)
        if indicator == Indicator.CURRENT_BREAKOUT:
            indicator_dict[indicator]=load_current_breakout(ticker, ticker_history, module_config)
        if indicator == Indicator.EXTREME_RSI:
            indicator_dict[indicator]=load_extreme_rsi(ticker, ticker_history, module_config)
        if indicator == Indicator.EXTREME_RSI_ALTERNATIVE:
            indicator_dict[indicator]=load_extreme_rsi_alternative(ticker, ticker_history, module_config)
        if indicator == Indicator.MOMENTUM_QUICK:
            indicator_dict[indicator]=load_momentum_quick(ticker, ticker_history, module_config)
        if indicator == Indicator.RSI_REVERSAL:
            indicator_dict[indicator]=load_rsi_reversal(ticker, ticker_history, module_config)
        if indicator == Indicator.ADX_REVERSAL_ALTERNATIVE:
            indicator_dict[indicator]=load_adx_reversal_alternative(ticker, ticker_history, module_config)



    #so basically here we want to iterate through the history and find the indexes that meet the criteria
    return  indicator_dict

def print_backtest_results(ticker,results,  module_config):
    print(f"Backtest Results for ${ticker}")
    print(f"Indicators used: {','.join(module_config['indicators'])}")
    for k, v in module_config['indicator_configs'].items():
        if k in module_config['indicators']:
            print(f"Indicator Parameters for {k}")
            for kk, vv in module_config['indicator_configs'][k].items():
                print(f"\t{kk}={vv}")
    if module_config['strategy'] == StrategyType.IRON_CONDOR:
        print(f"Sold {len(results['positions'])} iron condors held for {module_config['strategy_configs'][module_config['strategy']]['position_length']} days")
        print(f"Strategy generation type: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_type']}| Value: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_value']}")
        print(f"{results['winners']} winners, {results['losers']} losers. The strategy is successful {calculate_x_is_what_percentage_of_y(results['winners'], len(results['positions']))}% of the time ")
    if module_config['strategy'] == StrategyType.LONG_CALL or module_config['strategy'] == StrategyType.LONG_PUT:
        print(f"Bought {len(results['positions'])} {module_config['strategy']}s held for {module_config['strategy_configs'][module_config['strategy']]['position_length']} days")
        # print(f"Strategy generation type: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_type']}| Value: {module_config['strategy_configs'][module_config['strategy']]['strategy_generation_value']}")
        print(f"{results['winners']} winners, {results['losers']} losers. The strategy is successful {calculate_x_is_what_percentage_of_y(results['winners'], len(results['positions']))}% of the time ")
    print("\n")