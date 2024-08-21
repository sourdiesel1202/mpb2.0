import datetime
from abc import abstractmethod

from history import TickerHistory
from functions import calculate_what_is_x_percentage_of_y, calculate_x_is_what_percentage_of_y
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
    def deserialize(json_data):
        return Leg(json_data['strike'], json_data['type'])

class Strategy:
    def __init__(self, open_date, length,profitable=False, underlying_percentage_gain=None, underlying_handle_gain=None):
        self.open_date=open_date
        self.length=length
        self.profitable = profitable
        if underlying_percentage_gain is None:
            self.underlying_percentage_gain = 0.0
        else:
            self.underlying_percentage_gain=underlying_percentage_gain
        if underlying_handle_gain is None:
            self.underlying_handle_gain = 0.0
        else:
            self.underlying_handle_gain=underlying_handle_gain
        self.underlying_handle_gain = 0.0
    def is_profitable(self, ticker_history,module_config):
        '''
        this method determines whether a strategy would be profitable i.e. would it make money or lose money

        :param ticker_price:
        :return: true if you make money, false if you lose money
        '''
        # pass

    def legs(self):
        pass
    def serialize(self):
        pass

    @staticmethod
    def deserialize(json_date):
        pass
    def __str__(self):
        pass
class LongCall(Strategy):
    def __init__(self, strike_price, open_date, length,profitable=False,underlying_percentage_gain=None, underlying_handle_gain=None):
        super().__init__(open_date, length,profitable=profitable,underlying_percentage_gain=underlying_percentage_gain, underlying_handle_gain=underlying_handle_gain)
        if type(strike_price) == TickerHistory:
            self.strike = Leg(strike_price.close,PositionType.LONG )
        else:
            self.strike = Leg(strike_price,PositionType.LONG )
        # self.profitable=profitable

    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        self.profitable = self.strike.strike < ticker_history.close
        self.underlying_percentage_gain = calculate_x_is_what_percentage_of_y(ticker_history.close - self.strike.strike,
                                                                   self.strike.strike)
        self.underlying_handle_gain = ticker_history.close - self.strike.strike

        return  self.profitable


    def legs(self):
        return [self.strike]
    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"underlying_percentage_gain":self.underlying_percentage_gain,"underlying_handle_gain":self.underlying_handle_gain,"profitable":self.profitable, "strike_price":self.strike.strike, "open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":[x.serialize() for x in self.legs()]}

    @staticmethod
    def deserialize(json_data):
        return LongCall(json_data['strike_price'],datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'],profitable=json_data['profitable'], underlying_percentage_gain=json_data['underlying_percentage_gain'], underlying_handle_gain=json_data['underlying_handle_gain'])
    def __str__(self):
        return f"{self.open_date.strftime('%Y-%m-%d')} {self.strike.strike} Long Call"
class LongPut(Strategy):
    def __init__(self, strike_price, open_date, length,profitable=False,underlying_percentage_gain=None, underlying_handle_gain=None):
        super().__init__(open_date, length,profitable=profitable, underlying_percentage_gain=underlying_percentage_gain, underlying_handle_gain=underlying_handle_gain)
        if type(strike_price) == TickerHistory:
            self.strike = Leg(strike_price.close, PositionType.LONG)
        else:
            self.strike = Leg(strike_price, PositionType.LONG)


    def is_profitable(self, ticker_history, module_config):
        if module_config['logging']:
            print(f"Checking Profitabilty of {str(self)}, opened on {self.open_date} closed on {ticker_history.dt} held for {module_config['strategy_configs'][module_config['strategy']]['position_length']}")
        self.profitable = self.strike.strike > ticker_history.close
        # if self.profitable
        self.underlying_percentage_gain = calculate_x_is_what_percentage_of_y(ticker_history.close - self.strike.strike,
                                                                   self.strike.strike)
        self.underlying_handle_gain = ticker_history.close - self.strike.strike

        return self.profitable
    def legs(self):
        return [self.strike]
    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"underlying_percentage_gain":self.underlying_percentage_gain,"underlying_handle_gain":self.underlying_handle_gain, "profitable":self.profitable,"strike_price":self.strike.strike, "open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":[x.serialize() for x in self.legs()]}

    @staticmethod
    def deserialize(json_data):
        return LongPut(json_data['strike_price'],datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'],profitable=json_data['profitable'], underlying_percentage_gain=json_data['underlying_percentage_gain'], underlying_handle_gain=json_data['underlying_handle_gain'])
    def __str__(self):
        return f"{self.open_date.strftime('%Y-%m-%d')} {self.strike.strike} Long Put"
    # def __str__(self):ƒ
    #     return f"{self.open_date} {self.strike.strike} Long Put"
class IronCondor(Strategy):
    def __init__(self, ticker_price, long_put, short_put, long_call, short_call, open_date, length,profitable=False,underlying_percentage_gain=None, underlying_handle_gain=None):
        super().__init__(open_date,length,profitable=profitable,underlying_percentage_gain=underlying_percentage_gain, underlying_handle_gain=underlying_handle_gain)
        self.ticker_price = ticker_price
        if type(long_put) == Leg:
            self.long_put = Leg(long_put.strike, PositionType.LONG)
        else:
            self.long_put = Leg(long_put, PositionType.LONG)
        if type(short_put) == Leg:
            self.short_put = Leg(short_put.strike, PositionType.SHORT)
        else:
            self.short_put = Leg(short_put, PositionType.SHORT)

        if type(short_call) == Leg:
            self.short_call = Leg(short_call.strike, PositionType.SHORT)
        else:
            self.short_call = Leg(short_call, PositionType.SHORT)

        if type(long_call) == Leg:
            self.long_call = Leg(long_call.strike, PositionType.LONG)
        else:
            self.long_call = Leg(long_call, PositionType.LONG)
        # self.long_put = long_put  # Leg(long_put, PositionType.LONG)
        # self.short_put = short_put  # Leg(short_put, PositionType.SHORT)
        # self.long_call = long_call  # Leg(long_call,PositionType.LONG)
        # self.short_call = short_call  # Leg(short_call,PositionType.SHORT)

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
        # pass
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
        self.underlying_percentage_gain = calculate_x_is_what_percentage_of_y(ticker_history.close - self.ticker_price,
                                                                              self.ticker_price)
        self.underlying_handle_gain = ticker_history.close - self.ticker_price

        return self.profitable
    def is_atm(self, ticker_price, module_config):
        # ticker_price=ticker_history.close
        return (self.long_put.strike <= ticker_price <= self.short_put.strike) or (self.short_call.strike <= ticker_price <= self.long_call.strike)

    @staticmethod
    def generate_strikes(last_close,ticker_history, module_config):
        '''
        If you use the * operator you can generate the condor with this
        IronCondor(*IronCondor.generate_strikes(ticker_price,module_config))
        :param ticker_price:
        :param module_config:
        :return:
        '''
        #so basically here we can do the mathematical crap to determine our strikes
        #initally let's try 4% moves +/-
        #ok so new plan we're going to go off of the average of the daily moves
        # ups = ([ticker_history[i].close
        ups = []
        downs = []
        for i in range(len(ticker_history)-(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length']*3), len(ticker_history)-1):
            delta = calculate_x_is_what_percentage_of_y(ticker_history[i+1].close-ticker_history[i].close, ticker_history[i].close)
            if delta >0:
                ups.append(delta)
            else:
                downs.append(delta)
        avg_move_positive = float(sum(ups)/len(ups))
        avg_move_negative = float(sum(downs)/len(downs))
        # max_avg_move_positive = max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs))) *-1 if max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs))) < 0 else max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs)))
        # max_avg_move_negative = min(float(sum(downs)/len(downs)),float(sum(downs)/len(downs))) *-1 if max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs))) < 0 else max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs)))
         # = max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs))) *-1 if max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs))) < 0 else max(float(sum(ups)/len(ups)),float(sum(downs)/len(downs)))
        # short_call_strike = 0.0
        # short_put_strike = 0.0

        if last_close < 10:
            module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width'] = 0.5
        elif last_close < 50:
            module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width'] = 2
        else:
            module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width'] = 5
        ticker_price = ticker_history[-1].close
        if module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_type'] == StrategyGenerationType.PERCENTAGE:
            #do the shorts first
            # short_put = Leg(int(ticker_price - int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) ), PositionType.SHORT)
            # short_call = Leg(int(ticker_price + int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price))),PositionType.SHORT)
            # short_call = Leg(int(last_close+(avg_move_positive*module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])), PositionType.SHORT)
            # short_put = Leg(int(last_close-(avg_move_negative*module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])), PositionType.SHORT)
            # long_put = Leg(int(last_close-(avg_move_negative*module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])-module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            long_call = Leg(int(last_close+(calculate_what_is_x_percentage_of_y( avg_move_positive, last_close)* module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])+module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            long_put = Leg(int(last_close+(calculate_what_is_x_percentage_of_y( avg_move_negative, last_close)* module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])-module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            short_put = Leg(int(last_close+(calculate_what_is_x_percentage_of_y( avg_move_negative, last_close)* module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])), PositionType.SHORT)
            short_call = Leg(int(last_close+(calculate_what_is_x_percentage_of_y( avg_move_positive, last_close)* module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])), PositionType.SHORT)
            # long_put = Leg(int(last_close-(avg_move_negative*calculate_what_is_x_percentage_of_y( avg_move_negative, last_close)* module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])-module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            # long_call = Leg(int(last_close+(avg_move_positive*module_config['strategy_configs'][StrategyType.IRON_CONDOR]['position_length'])+module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            # long_call = Leg(int(ticker_price + int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) +module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            # long_put = Leg(int(ticker_price - int(calculate_what_is_x_percentage_of_y(module_config['strategy_configs'][StrategyType.IRON_CONDOR]['strategy_generation_value'], ticker_price)) -module_config['strategy_configs'][StrategyType.IRON_CONDOR]['width']), PositionType.LONG)
            if module_config['logging']:
                print(f'Generated Iron Condor {ticker_history.dt}: Last Price: {ticker_history.close}: Put Wing: {long_put.strike}/{short_put.strike} Call Wing: {short_call.strike}/{long_call.strike}')
            return [long_put, short_put, long_call,short_call]

    def serialize(self):
        # datetime.datetime.strptime(exp, '%Y-%m-%d')
        return {"ticker_price":self.ticker_price,"underlying_percentage_gain":self.underlying_percentage_gain,"underlying_handle_gain":self.underlying_handle_gain, "profitable":self.profitable,"open_date":self.open_date.strftime('%Y-%m-%d'), "position_length":self.length, "legs":{"long_put":self.long_put.serialize(), "short_put":self.short_put.serialize(), "short_call":self.short_call.serialize(), "long_call":self.long_call.serialize()}}

    @staticmethod
    def deserialize(json_data):
        # (self, long_put, short_put, long_call, short_call, open_date, length, profitable=False, underlying_percentage_gain=None, underlying_handle_gain=None)
        return IronCondor(json_data['ticker_price'],Leg.deserialize(json_data['legs']['long_put']),Leg.deserialize(json_data['legs']['short_put']),Leg.deserialize(json_data['legs']['long_call']),Leg.deserialize(json_data['legs']['short_call']),datetime.datetime.strptime(json_data['open_date'],'%Y-%m-%d'), json_data['position_length'],profitable=json_data['profitable'], underlying_percentage_gain=json_data['underlying_percentage_gain'], underlying_handle_gain=json_data['underlying_handle_gain'])
