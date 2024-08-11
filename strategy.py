import datetime
from abc import abstractmethod

from history import TickerHistory
from functions import calculate_what_is_x_percentage_of_y
from enums import PositionType, StrategyGenerationType, StrategyType


class Leg:
    def __init__(self, strike, type):
        self.strike = strike
        if type not in [PositionType.LONG, PositionType.SHORT]:
            raise Exception("Leg must be short or long")
        self.type=type
        # self.expiration
    def __str__(self):
        return f"{self.strike} {self.type}"
    def serialize(self):
        return {"strike":self.strike, "type":self.type}
    @staticmethod
    def deserialize(self, json_data):
        return Leg(json_data['strike'], json_data['type'])

class Strategy:
    def __init__(self, open_date, length):
        self.open_date=open_date
        self.length=length
        self.profitable = False
    def is_profitable(self, ticker_price,module_config):
        '''
        this method determines whether a strategy would be profitable i.e. would it make money or lose money

        :param ticker_price:
        :return: true if you make money, false if you lose money
        '''
        pass

    def legs(self):
        pass
    def serialize(self):
        pass

    @staticmethod
    def deserialize(self,json_date):
        pass
    def __str__(self):
        pass
class LongCall(Strategy):
    def __init__(self, strike_price, open_date, length):
        super().__init__(open_date, length)
        if type(strike_price) == TickerHistory:
            self.strike = Leg(strike_price.close,PositionType.LONG )
        else:
            self.strike = Leg(strike_price,PositionType.LONG )

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        self.profitable = self.strike.strike < ticker_history.close
        return  self.profitable


    def legs(self):
        return [self.strike]
    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"strike_price":self.strike.strike, "open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":[x.serialize() for x in self.legs()]}

    @staticmethod
    def deserialize(self,json_data):
        return LongCall(json_data['strike_price'],datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'])
    def __str__(self):
        return f"{self.open_date.strftime('%Y-%m-%d')} {self.strike.strike} Long Call"
class LongPut(Strategy):
    def __init__(self, strike_price, open_date, length):
        super().__init__(open_date, length)
        if type(strike_price) == TickerHistory:
            self.strike = Leg(strike_price.close, PositionType.LONG)
        else:
            self.strike = Leg(strike_price, PositionType.LONG)

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        self.profitable = self.strike.strike > ticker_history.close
        return self.profitable
    def legs(self):
        return [self.strike]
    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"strike_price":self.strike.strike, "open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":[x.serialize() for x in self.legs()]}

    @staticmethod
    def deserialize(self,json_data):
        return LongPut(json_data['strike_price'],datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'])
    def __str__(self):
        return f"{self.open_date.strftime('%Y-%m-%d')} {self.strike.strike} Long Put"
    # def __str__(self):
    #     return f"{self.open_date} {self.strike.strike} Long Put"
class IronCondor(Strategy):
    def __init__(self, long_put, short_put, long_call, short_call, open_date, length):
        super().__init__(open_date,length)
        self.long_put=Leg(long_put, PositionType.LONG)
        self.short_put=Leg(short_put, PositionType.SHORT)
        self.long_call=Leg(long_call,PositionType.LONG)
        self.short_call=Leg(short_call,PositionType.SHORT)

    def legs(self):
        return [self.long_put, self.short_put, self.short_call, self.long_call]
    def __str__(self):
        return f"{self.open_date} {self.long_put.strike}/{self.short_put.strike} {self.short_call.strike}/{self.long_call.strike} Iron Condor"
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
        return ticker_price < self.long_put.strike or ticker_price > self.long_call.strike

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        self.profitable = self.is_itm(ticker_history.close, module_config)
        return self.profitable
    def is_atm(self, ticker_price, module_config):
        # ticker_price=ticker_history.close
        return (self.long_put.strike <= ticker_price <= self.short_put.strike) or (self.short_call.strike <= ticker_price <= self.long_call.strike)

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

    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":{"long_put":self.long_put.serialize(), "short_put":self.short_put.serialize(), "short_call":self.short_call.serialize(), "long_call":self.long_call.serialize()}}

    @staticmethod
    def deserialize(self,json_data):
        return LongPut(json_data['strike_price'],datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'])
