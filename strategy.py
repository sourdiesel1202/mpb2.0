from abc import abstractmethod
from functions import calculate_what_is_x_percentage_of_y
from enums import PositionType, StrategyGenerationType, StrategyType


class Leg:
    def __init__(self, strike, type):
        self.strike = strike
        if type not in [PositionType.LONG, PositionType.SHORT]:
            raise Exception("Leg must be short or long")
        self.type=type
    def __str__(self):
        return f"{self.strike} {self.type}"
class Strategy:
    def __init__(self, open_date, length):
        self.open_date=open_date
        self.length=length
    def is_profitable(self, ticker_price,module_config):
        '''
        this method determines whether a strategy would be profitable i.e. would it make money or lose money

        :param ticker_price:
        :return: true if you make money, false if you lose money
        '''
        pass

    def legs(self):
        pass
    def __str__(self):
        pass
class LongCall(Strategy):
    def __init__(self, strike_price, open_date, length):
        super().__init__(open_date, length)
        self.strike = Leg(strike_price.close,PositionType.LONG )


    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        return self.strike.strike < ticker_history.close

    def legs(self):
        return [self.strike]
    def __str__(self):
        return f"{self.open_date} Long Call"
class LongPut(Strategy):
    def __init__(self, strike_price, open_date, length):
        super().__init__(open_date, length)
        self.strike = Leg(strike_price.close,PositionType.SHORT )

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        return self.strike.strike > ticker_history.close
    def legs(self):
        return [self.strike]
    def __str__(self):
        return f"{self.open_date} Long Put"
class IronCondor(Strategy):
    def __init__(self, long_put, short_put, long_call, short_call, open_date, length):
        super().__init__(open_date,length)
        self.long_put=long_put
        self.short_put=short_put
        self.long_call=long_call
        self.short_call=short_call

    def legs(self):
        return [self.long_put, self.short_put, self.short_call, self.long_call]
    def __str__(self):
        return f"{self.open_date} Iron Condor"
    def is_itm(self, ticker_price, module_config):
        '''
        is between the two short strikes
        :param ticker_price:
        :return:
        '''
        return self.short_call.strike > ticker_price > self.short_put.strike
        pass
    def is_otm(self, ticker_price, module_config):
        '''
        is outside the long strike
        :param ticker_price:
        :return:
        '''
        return ticker_price < self.long_put.strike_price or ticker_price > self.long_call.strike_price

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        return self.is_itm(ticker_history.close, module_config)
    def is_atm(self, ticker_price, module_config):
        # ticker_price=ticker_history.close
        return (self.long_put.strike_price <= ticker_price <= self.short_put.strike_price) or (self.short_call.strike_price <= ticker_price <= self.long_call.strike_price)

    @staticmethod
    def generate_strikes(ticker_history, module_config):
        '''
        If you use the * operator you can generate the condor with this
        IronCondor(*IronCondor.generate_strikes(ticker_price,module_config))
        :param ticker_price:
        :param module_config:
        :return:
        '''
        #so basically here we can do the mathematical crap to determine our strikes
        #initally let's try 4% moves +/-
        ticker_price = ticker_history.close
        if module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_type'] == StrategyGenerationType.PERCENTAGE:
            long_put = Leg(int(ticker_price - int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) -module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            short_put = Leg(int(ticker_price - int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) ), PositionType.SHORT)
            long_call = Leg(int(ticker_price + int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) +module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            short_call = Leg(int(ticker_price + int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price))),PositionType.SHORT)
            if module_config['logging']:
                print(f'Generated Iron Condor {ticker_history.dt}: Last Price: {ticker_history.close}: Put Wing: {long_put.strike}/{short_put.strike} Call Wing: {short_call.strike}/{long_call.strike}')
            return [long_put, short_put, long_call,short_call]


