import copy

import mibian
import yfinance as yf, datetime
from enums import ContractType, PositionType, PriceGoalChangeType
from functions import calculate_x_is_what_percentage_of_y
# from strategy import Contract

class Position:
    '''
    Container for an active position. Whereas one of the strategy classes simply indicate a theoretical position at a given time in the ticker's history
    '''
    def __init__(self, contracts, strategy):
        self.contracts = contracts
        self.expiration = min([datetime.datetime.strptime(x.expiration,'%Y-%m-%d') for x in self.contracts]).strftime('%Y-%m-%d')
        self.strategy = strategy
        self.position_cost = round(sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price)*-1.00 for x in self.contracts]),2)
        # self.position_cost = round(sum([x.last_price for x in self.contracts]),2)
        self.theta = round(sum([x.theta for x in self.contracts]),2)
        self.delta = round(sum([x.delta for x in self.contracts]),2)
        self.gamma = round(sum([x.gamma  for x in self.contracts]),2)
        self.rho = round(sum([x.rho for x in self.contracts]),2)
        self.debit_credit = DebitCredit.DEBIT if self.position_cost > 0 else DebitCredit.CREDIT


    def __str__(self):
        legs_str = '\n'.join([str(x) for x in self.contracts])
        return f"{self.expiration} {self.strategy}\nLegs\n{legs_str}\n"

    def serialize(self):
        # json_data = super().serialize()  # {"strike":self.strike, "type":self.type}
        json_data = {}
        json_data['strategy'] = self.strategy
        json_data['contracts']=[x.serialize() for x in self.contracts]
        return json_data
    @staticmethod
    def deserialize(json_data, underlying_close=None):
        # return Leg(json_data['strike'], json_data['type'], contract_type=contract_type)
        # def __init__(self, strike, underlying_close, last_price, contract_type, type, expiration, implied_volatility,contract_name=None, dte=None):
        # constructor will re-calculate greeks
        # print(json_data)
        contracts =[Contract(json_data['contracts'][i]['strike'], underlying_close, json_data['contracts'][i]['last_price'],json_data['contracts'][i]['contract_type'], json_data['contracts'][i]['type'], json_data['contracts'][i]['expiration'],json_data['contracts'][i]['implied_volatility'], contract_name=json_data['contracts'][i]['contract_name']) for i in range(0, len(json_data['contracts']))]

        return Position(contracts,json_data['strategy'])

    def calculate_theta_change_impact_position(self,price_goal_change_type, asset_price, price_goal):
        # ok so the idea here is we have a list of contracts and we need to iterate through the DTE
        # we know that the change type is % of option contract
        if price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_PERCENTAGE and price_goal >= 0:
            raise Exception(
                f"To calculate theta decay impact using percentage of the contract as a base, please use negative option % as theta is decaying, postive theta still indicates a decrease in position price; ")
        original_position_cost = self.position_cost
        # ok so now we can iterate through each DTE and recalculate theta for each DTE and determine if the total value of the
        # position

        new_contracts = [x for x in self.contracts]
        # new_contracts= []
        # original_position_cost
        position_theta = round(sum([x.theta for x in new_contracts]), 2)
        position_delta = round(sum([x.delta for x in new_contracts]), 2)
        position_gamma = round(sum([x.gamma for x in new_contracts]), 2)
        position_rho = round(sum([x.rho for x in new_contracts]), 2)
        _position_cost = round(
            sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price) * -1.00 for x in
                 new_contracts]), 2)
        for i in reversed(range(1, min([x.dte for x in new_contracts]))):
            print(f"Processing DTE: {i}")
            # iterate over DTE
            # for each DTE we need to figure out what the new position price is given the DTE
            delete = len(new_contracts)
            _tmp_new_contracts = []
            for ii in range(0, delete):
                # for contract in contracts:
                contract = new_contracts[ii]
                _tmp_ask = contract.last_price - (
                    contract.theta / 100.00 if contract.theta > 0 else contract.theta * -1 / 100.00)
                if contract.contract_type == ContractType.CALL:
                    _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i],
                                        callPrice=_tmp_ask).impliedVolatility
                else:
                    _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i],
                                        putPrice=_tmp_ask).impliedVolatility
                # recalculate greeks for the current DTE
                greeks = mibian.BS([asset_price, new_contracts[ii].strike, 0, i], volatility=_tmp_iv)
                # now create new contracts and replace the old
                # if contract.contract_type == ContractType.CALL:
                tmp_contract = Contract(new_contracts[ii].strike, asset_price, _tmp_ask, contract.contract_type,
                                        contract.type, contract.expiration, _tmp_iv, dte=i)
                if tmp_contract.contract_type == ContractType.CALL:
                    tmp_contract.delta = greeks.callDelta * 100
                    tmp_contract.theta = greeks.callTheta * 100
                    tmp_contract.gamma = greeks.gamma * 100
                    tmp_contract.rho = greeks.callRho * 100
                else:
                    tmp_contract.delta = greeks.putDelta * 100
                    tmp_contract.theta = greeks.putTheta * 100
                    tmp_contract.gamma = greeks.gamma * 100
                    tmp_contract.rho = greeks.callRho * 100
                if tmp_contract.type == PositionType.SHORT:
                    tmp_contract.delta = tmp_contract.delta * -1.00
                    tmp_contract.theta = tmp_contract.theta * -1.00
                    tmp_contract.gamma = tmp_contract.gamma * -1.00
                    tmp_contract.rho = tmp_contract.rho * -1.00

                _tmp_new_contracts.append(tmp_contract)

            # ok so one we're down here we have the contracts at n dte
            # clear the old contracts
            for j in range(0, len(new_contracts)):
                new_contracts[j] = _tmp_new_contracts[j]
                pass
            # for j in range(0, delete):
            #     new_contracts.pop()
            # pass
            position_theta = round(sum([x.theta for x in new_contracts]), 2)
            position_delta = round(sum([x.delta for x in new_contracts]), 2)
            position_gamma = round(sum([x.gamma for x in new_contracts]), 2)
            position_rho = round(sum([x.rho for x in new_contracts]), 2)
            _position_cost = round(
                sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price) * -1.00 for x in
                     new_contracts]), 2)
            # print(f"Starting DTE: {dte} Analyzing {i} DTE: New Theta {position_theta} New Position Cost: {_position_cost} Original: {original_position_cost}")
            # basically we either hit the target with theta decay percentage of contract
            valid = False
            if ((price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_PERCENTAGE) and (calculate_x_is_what_percentage_of_y((original_position_cost - _position_cost), original_position_cost) * -1 if price_goal < 0 else 1) <= price_goal):
                valid = True
            if (price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_DAYS_UNTIL and price_goal == self.contracts[0].dte - i):
                valid = True
            if (price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_DAYS_DTE and price_goal == i):
                valid = True
            if valid:
                return {
                    "dte": i,
                    "days_to_target": self.contracts[0].dte - i,
                    "new_contract_price": _position_cost,
                    "original_position_price": original_position_cost,
                    "greeks": {
                        "delta": position_delta,
                        "theta": position_theta,
                        "gamma": position_gamma,
                        "rho": position_rho,
                    }
                }
        return {
            "dte": 0,
            "days_to_target": max([x.dte for x in self.contracts]),
            "new_contract_price": _position_cost + (position_theta / 100),
            "original_position_price": original_position_cost,
            "greeks": {
                "delta": position_delta,
                "theta": position_theta,
                "gamma": position_gamma,
                "rho": position_rho,
            }
        }

    def calculate_delta_impact_contract(self,price_goal_change_type, asset_price, price_goal, contract):
        ##in THEORY this should work for futures too?
        # basically, how much does the price need to change before our bid is met
        # can't think of a better way to do this than by the penny
        # _tmp_iv =
        # make this work for calls/puts/shorts/longs and single legs
        if contract.contract_type == ContractType == ContractType.CALL:
            _tmp_iv = mibian.BS([asset_price, contract.strike_price, 0, contract.dte],
                                callPrice=contract.last_price).impliedVolatility
        else:
            _tmp_iv = mibian.BS([asset_price, contract.strike, 0, contract.dte],
                                putPrice=contract.last_price).impliedVolatility
        # _tmp_iv = mibian.BS([asset_price, strike, 0, dte], callPrice=ask).impliedVolatility
        greeks = mibian.BS([asset_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
        per_cent_delta = float(
            greeks.callDelta / 100 if contract.contract_type == ContractType == ContractType.CALL else greeks.putDelta / 100)
        # _ticker_price_change_value = (0.1/100) * asset_price
        if price_goal_change_type in [PriceGoalChangeType.ASSET_POINTS, PriceGoalChangeType.ASSET_PERCENTAGE,
                                      PriceGoalChangeType.ASSET_SET_PRICE]:
            # ok so basically we need to figure out what the bid is at our target asking price
            # if we are targeting specifc increase/decrease in asset price by points i.e. 5 points off SPY
            if price_goal_change_type == PriceGoalChangeType.ASSET_POINTS:
                _asset_price = asset_price - price_goal
            elif price_goal_change_type == PriceGoalChangeType.ASSET_PERCENTAGE:
                # otherwise we are targeting a specific percentage of the underlying
                p = float(float(price_goal / 100) * asset_price)
                print(f"{price_goal} percent of ${asset_price}: ${p}")
                _asset_price = asset_price + p  # * (-1 if p > 0 else 1)
            else:
                # finally, if niether, we assume we are inputting a set asset price (i.e. SPY @ 561)
                _asset_price = price_goal
            _tmp_price = asset_price
            _tmp_ask = contract.last_price

            # either way above we need to calculcate what the target bid would be, iterating over the price of the asset by 0.01 until we reach the target price of the underlying
            if contract.contract_type == ContractType.CALL:
                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte],
                                    callPrice=_tmp_ask).impliedVolatility
            else:
                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], putPrice=_tmp_ask).impliedVolatility
            # _tmp_iv = mibian.BS([_tmp_price, strike, 0, dte], callPrice=_tmp_ask).impliedVolatility
            greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
            per_cent_delta = float(
                greeks.callDelta if contract.contract_type == ContractType == ContractType.CALL else greeks.putDelta) / 100
            # ok so this is going to get tricky
            while round(_asset_price, 2) != round(_tmp_price, 2):
                if _asset_price > _tmp_price:
                    if contract.contract_type == ContractType.CALL:  # so for a long call price needs to go up
                        multiplier = 1  # we want long calls to go up

                    else:
                        multiplier = -1  # long puts we want to go down

                else:  # if the bid is less than the ask, we want short puts to be profitable, long puts not to be
                    if contract.contract_type == ContractType.CALL:
                        multiplier = -1  # we want long calls to go down
                    else:
                        multiplier = 1  # long puts we want to go up
                _tmp_price = _tmp_price - 0.01 * (-1 if _tmp_price < _asset_price else 1)  # one send decrements
                _tmp_ask = _tmp_ask - per_cent_delta * multiplier
                # print(f"Calculating implied volatility")
                if contract.contract_type == ContractType.CALL:

                    _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte],
                                        callPrice=_tmp_ask).impliedVolatility
                else:
                    _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte],
                                        putPrice=_tmp_ask).impliedVolatility
                # print(f"Recalculating greeks")
                greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
                # print(f"Recalculating per-cent delta")
                per_cent_delta = float(
                    greeks.callDelta if contract.contract_type == ContractType == ContractType.CALL else greeks.putDelta) / 100
            bid = _tmp_ask
            return {
                "asset_price": round(_tmp_price, 2),
                "asset_change": round(float(_tmp_price - asset_price), 2),
                "asset_percentage_change": round(
                    calculate_x_is_what_percentage_of_y(float(_tmp_price - asset_price), asset_price), 2),
                "last_contract_price": round(contract.last_price, 2),
                "new_contract_price": round(bid, 2),
                "greeks": {
                    "delta": greeks.callDelta if contract.contract_type == ContractType.CALL else greeks.putDelta,
                    "theta": greeks.callTheta if contract.contract_type == ContractType.CALL else greeks.putTheta,
                    "gamma": greeks.gamma,
                    "vega": greeks.vega,
                    "implied_violatility": _tmp_iv
                }
            }
            # _tmp_price = _asset_price
        else:

            # basically for all cases but asset changes, we can more ore less re-use the existing code, we just need to set our bid accordingly
            # if price_goal_change_type == PriceGoalChangeType.OPTION_SET_PRICE:
            if price_goal_change_type == PriceGoalChangeType.OPTION_TICK:
                bid = contract.last_price + (price_goal * -1 if contract.type == PositionType.SHORT else 1)
            elif price_goal_change_type == PriceGoalChangeType.OPTION_PERCENTAGE:
                p = float(float(
                    price_goal / 100) * contract.last_price)  # * (-1 if position_type == PositionType.LONG else 1 )
                print(
                    f"{price_goal} percent of ${contract.strike} {contract.type} Option priced at ${contract.last_price}: ${(p * -1 if contract.type == PositionType.SHORT else p)}")
                bid = contract.last_price + (p * -1 if contract.type == PositionType.SHORT else p)
                pass
                # _tmp_ask =
            else:
                bid = price_goal

        # don't care about vega rn
        _tmp_contract_last_price = contract.last_price
        _tmp_price = asset_price
        # while _tmp_ask < bid: #stop at a 0.05 ask, etrade order price min increment lol
        # while (round(_tmp_ask,2) < round(bid,2) if position_type == PositionType.LONG else round(bid,2) < round(_tmp_ask,2)):
        while round(_tmp_contract_last_price, 2) != round(bid,
                                                          2):  # and _tmp_ask > 0.05: #stop at a 0.05 ask, etrade order price min increment lol
            # short options we want the value to go down of the contract
            # so if the  bid is greater than the ask it's for a loser
            if bid > _tmp_contract_last_price:
                if contract.contract_type == ContractType.CALL:  # so for a long call price needs to go up
                    multiplier = -1  # we want long calls to go up

                else:
                    multiplier = 1  # long puts we want to go down
            else:  # if the bid is less than the ask, we want short puts to be profitable, long puts not to be
                if contract.contract_type == ContractType.CALL:
                    multiplier = 1  # we want long calls to go down
                else:
                    multiplier = -1  # long puts we want to go up

            _tmp_price = _tmp_price - 0.01 * multiplier  # (-1 if bid > _tmp_contract_last_price else 1)#increase if the bid is greater that the contract price
            _tmp_contract_last_price = _tmp_contract_last_price - per_cent_delta * multiplier  # (-1 if bid > _tmp_contract_last_price else 1)
            # _tmp_contract_last_price = _tmp_contract_last_price - per_cent_delta * delta_multiplier#(-1 if bid > _tmp_contract_last_price else 1)
            if contract.contract_type == ContractType.CALL:
                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte],
                                    callPrice=_tmp_contract_last_price).impliedVolatility
            else:
                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte],
                                    putPrice=_tmp_contract_last_price).impliedVolatility
            greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
            per_cent_delta = float(
                greeks.callDelta if contract.contract_type == ContractType.CALL else greeks.putDelta) / 100

        if round(bid, 2) == round(_tmp_contract_last_price, 2):
            # assuming we get here, we know that the order would have been filled
            return {
                "asset_price": round(_tmp_price, 2),
                "asset_change": round(float(_tmp_price - asset_price), 2),
                "asset_percentage_change": round(
                    calculate_x_is_what_percentage_of_y(float(_tmp_price - asset_price), asset_price), 2),
                "last_contract_price": round(contract.last_price, 2),
                "new_contract_price": round(bid, 2),
                "greeks": {
                    "delta": greeks.callDelta if contract.contract_type == ContractType.CALL else greeks.putDelta,
                    "theta": greeks.callTheta if contract.contract_type == ContractType.CALL else greeks.putTheta,
                    "gamma": greeks.gamma,
                    "vega": greeks.vega,
                    "implied_violatility": _tmp_iv
                }
            }

        else:
            raise Exception(f"Decreased call price from {asset_price} ==> {_tmp_price} did not reach bid price")


    def build_theta_table_html(self, asset_price):
        html = f"<h3>{self.expiration}<br>{self.strategy}</h3>"
        html += f"<h4>{self.position_cost} {self.debit_credit}</h3>"
        html += f"<h5>Theta Decay Table</h5>"
        html += f"<table style='border: 1px solid black;border-collapse: collapse;'><tr>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Position Change (%)</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Position Value</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Days Until Change</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Position DTE</th>"
        html += f"</tr>"
        for i in reversed(range(-90, 0, 10)):
            print(f"Calculating time decay for {i}% gain in {self.expiration} {self.strategy}")
            results = self.calculate_theta_change_impact_position(PriceGoalChangeType.OPTION_THETA_DECAY_PERCENTAGE, asset_price, i)
            print(results)
            html += f"<tr>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{i}%</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{results['new_contract_price']* -1 if results['new_contract_price'] < 0 else results['new_contract_price']}</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{results['days_to_target']}</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{results['dte']}</td>"
            html += f"</tr>"
        html += "</table>"
        return html
    def build_delta_table_html(self, asset_price):
        strike_str = '/'.join([str(x.strike) for x in self.contracts])
        html = f"<h3>{self.expiration}<br>{strike_str}<br>{self.strategy}</h3>"
        html += f"<h4>{self.position_cost} {self.debit_credit}</h3>"
        html += f"<h5>Gain/loss Table</h5>"
        html += f"<table style='border: 1px solid black;border-collapse: collapse;'><tr>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Position Gain/Loss</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Position Value</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Move in Underlying<br>(Price)</th>"
        html+=f"<th style='border: 1px solid black;border-collapse: collapse;'>Move in Underlying<br>(Percentage)</th>"
        html += f"</tr>"
        for i in range(-90, 100, 10):
            # new_position_cost = sum([self.calculate_delta_impact_contract(PriceGoalChangeType.OPTION_PERCENTAGE, asset_price, i,x)['new_contract_price'] for x in self.contracts])
            strike_str = '/'.join([str(x.strike) for x in self.contracts])
            # print(f"Calculating required move for {i}% change in {self.expiration} {strike_str} {self.strategy}")
            print(f"Calculating change in position value for {i}% move in underlying: {self.expiration} {strike_str} {self.strategy}")
            new_leg_prices = []
            asset_change=0.0
            asset_change_percent=0.0
            for contract in self.contracts:
                results =self.calculate_delta_impact_contract(PriceGoalChangeType.OPTION_PERCENTAGE, asset_price, i, contract)
                new_leg_prices.append(results['new_contract_price'])
                asset_change = results['asset_change']
                asset_change_percent = results['asset_percentage_change']
            # results = self.calculate_delta_impact_contract(PriceGoalChangeType.OPTION_PERCENTAGE, asset_price, i,contract)
            # print(results)
            html += f"<tr>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{i}%</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{round(sum(new_leg_prices),2)}</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{round(asset_change,2)}</td>"
            html += f"<td style='border: 1px solid black;border-collapse: collapse;'>{round(asset_change_percent,2)}%</td>"
            html += f"</tr>"
        html += "</table>"
        return html

    def build_html_representation(self):
        pass
        html_str = f"<h3>{self.strategy} <h3>"
        html_str += f"<h3>{self.expiration} <h3><br>"

        html_str += f"<h4>{self.position_cost} {self.debit_credit}</h3>"
        html_str += f"<p>Position Greeks</p>"
        greeks_table = f"<table class='w3-center' style='width: 90%;margin:0 5%; border: 1px solid black;border-collapse: collapse;'><tr>"
        greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Theta</th>"
        greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Delta</th>"
        greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Gamma</th>"
        greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Rho</th>"
        greeks_table += f"</tr>"
        greeks_table += f"<tr>"
        greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{self.theta}</td>"
        greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{self.delta}</td>"
        greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{self.gamma}</td>"
        greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{self.rho}</td>"
        greeks_table += f"</tr><table><br>"

        html_str += greeks_table
        html_str += f"<h4>Legs<h4>"
        # ?f"<table><tr>"
        leg_greeks_table = f"<table class='w3-center' style='border: 1px solid black;border-collapse: collapse;'><tr>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Contract</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Last Price</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Theta</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Delta</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Gamma</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Rho</th>"
        leg_greeks_table += f"<th style='border: 1px solid black;border-collapse: collapse;'>Implied Volatility</th>"
        leg_greeks_table += f"</tr>"
        for position_leg in self.contracts:
            leg_greeks_table += f"<tr>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.strike} {position_leg.type} {position_leg.contract_type}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.last_price}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.theta}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.delta}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.gamma}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.rho}</td>"
            leg_greeks_table += f"<td style='border: 1px solid black;border-collapse: collapse;'>{position_leg.implied_volatility}</td>"
            leg_greeks_table += f"</tr>"
        leg_greeks_table += "</table>"
        html_str += leg_greeks_table
        return html_str
        # html_str +=

            #so the idea here is to create an html bit to throw in a div
class Contract:
    '''
    This is the realtime representation of a strategy leg; a leg is a theoretical leg of a position at some point in a ticker's history
    '''
    implied_volatility = 0.0
    delta = 0.0
    theta = 0.0
    gamma = 0.0
    rho = 0.0
    underlying_close = 0.0
    contract_name = 'Not Found'

    def __init__(self,strike, underlying_close,last_price, contract_type,type, expiration,implied_volatility,contract_name=None, dte=None):
        self.dte = None
        self.strike = strike
        self.underlying_close = underlying_close
        self.last_price = last_price
        self.contract_type = contract_type
        self.expiration = expiration
        self.type = type #short or long
        self.implied_volatility = implied_volatility
        if contract_name is not None:
            self.contract_name = contract_name
        self.calculate_greeks()
        #ok so once we've called the constructor, we can go ahead and set our  greeks

    def calculate_greeks(self):
        # if len(self.contract) == 0:
        #     raise  Exception("Cannot calculate greeks for a ContractHistory with no contract attribute specified")
        # contract_details = polygon.parse_option_symbol(self.contract)
        # option_symbol = OptionSymbol(self.contract)
        # dte =
        # position_type = PositionType.LONG if option_symbol.call_or_put.upper() == PositionType.LONG_OPTION[0].upper() else PositionType.SHORT
        if self.dte is None:
            self.dte = (datetime.datetime.strptime(self.expiration,'%Y-%m-%d') - datetime.datetime.now()).days
        # if self.contract_type == ContractType.CALL:
        #     _tmp_iv = mibian.BS([self.underlying_close, float(self.strike), 0, self.dte],
        #                         callPrice=self.last_price).impliedVolatility
        #
        # else:
        #     _tmp_iv = mibian.BS([self.underlying_close, float(self.strike), 0, self.dte],
        #                         putPrice=self.last_price).impliedVolatility
        # self.implied_volatility = _tmp_iv
        greeks = mibian.BS([self.underlying_close, float(self.strike), 0, self.dte],
                           volatility=self.implied_volatility)
        if self.contract_type == ContractType.CALL:
            self.theta = greeks.callTheta
            self.delta = greeks.callDelta
            self.gamma = greeks.gamma
            self.rho = greeks.callRho
        else:
            self.theta = greeks.putTheta
            self.delta = greeks.putDelta
            self.gamma = greeks.gamma
            self.rho = greeks.putRho
        #not sure if this is right, case for short options
        if self.type == PositionType.SHORT:
            self.theta = self.theta*-1
            self.delta = self.delta*-1
            self.gamma = self.gamma*-1
            self.rho = self.rho*-1

        self.theta = round(float(self.theta*100.00),2)
        self.delta = round(float(self.delta*100.00),2)
        self.gamma = round(float(self.gamma*100.00),2)
        self.rho = round(float(self.rho*100.00),2)
    def __str__(self):
        return f"\nExpiration:{self.expiration} Contract Name: {self.contract_name} Type: {self.type} Contract Type:{self.contract_type} Strike:{self.strike}\n\tDTE: {self.dte}\n\tImplied Volatility: {self.implied_volatility}\n\tTheta: {self.theta}\n\tDelta: {self.delta}\n\tGamma: {self.gamma}\n\tRho: {self.rho}"
# class ContractLoader:
#     def __init__(self,ticker):
#         self.ticker = ticker
def closest_in_list_to_input(lst, value):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - value))]
def load_ticker_expiration_dates(ticker, module_config):
    t = yf.Ticker(ticker)
    return [datetime.datetime.strptime(x, "%Y-%m-%d") for x in t.options]
def load_ticker_strikes(ticker,expiration_date, module_config):
    t = yf.Ticker(ticker)
    # return [datetime.datetime.strptime(x, "%Y-%m-%d") for x in t.options]
    return {ContractType.CALL:[x for x in t.option_chain(expiration_date).calls.strike], ContractType.PUT:[x for x in t.option_chain(expiration_date).puts.strike] }

def load_contract_last_price(ticker, expiration, strike, contract_type):
    t = yf.Ticker(ticker)
    if contract_type == ContractType.CALL:
        return [x for x in t.option_chain(expiration).calls.lastPrice][[x for x in t.option_chain(expiration).calls.strike].index(strike)]
    else: #putties
        # pass
        return [x for x in t.option_chain(expiration).puts.lastPrice][[x for x in t.option_chain(expiration).puts.strike].index(strike)]
    # pass
def load_contract_implied_volatility(ticker, expiration, strike, contract_type):
    t = yf.Ticker(ticker)
    if contract_type == ContractType.CALL:
        return round([x for x in t.option_chain(expiration).calls.impliedVolatility][[x for x in t.option_chain(expiration).calls.strike].index(strike)]*100.00,2)
    else: #putties
        # pass
        return round([x for x in t.option_chain(expiration).puts.impliedVolatility][[x for x in t.option_chain(expiration).puts.strike].index(strike)]*100.00,2)
    # pass
def find_nearest_expiration(position_length, expiration_dates, module_config):
    now = datetime.datetime.now()
    dtes = {(x-now).days:x.strftime('%Y-%m-%d') for x in expiration_dates}
    return dtes[closest_in_list_to_input([x for x in dtes.keys()], position_length)]
def find_nearest_strike(target_strike_price, contract_type, strike_dict, module_config):
    # now = datetime.datetime.now()
    deltas = {x-target_strike_price:x for x in strike_dict[contract_type]}
    # dtes = {(x-now).days:x.strftime('%Y-%m-%d') for x in expiration_dates}
    return closest_in_list_to_input(strike_dict[contract_type], target_strike_price)
    # return dtes[closest_in_list_to_input([x for x in deltas.keysΩ()], position_length)]
    # pass
def download_raw_yahoo_contract_data(ticker,module_config):
    t = yf.Ticker(ticker)
    options = t.options
    [datetime.datetime.strptime(x, "%y-%m-%d") for x in t.option_chain('2024-08-30').calls.strike]
    pass



def calculate_theta_change_impact_contract(price_goal_change_type,asset_price, price_goal, position_cost, contracts):
        # ok so the idea here is we have a list of contracts and we need to iterate through the DTE
        # we know that the change type is % of option contract
        if price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_PERCENTAGE  and price_goal>=0:
            raise Exception(f"To calculate theta decay impact using percentage of the contract as a base, please use negative option % as theta is decaying, postive theta still indicates a decrease in position price; ")
        original_position_cost = position_cost
        # ok so now we can iterate through each DTE and recalculate theta for each DTE and determine if the total value of the
        # position

        new_contracts = [x for x in contracts]
        # new_contracts= []
        # original_position_cost
        position_theta = round(sum([x.theta for x in new_contracts]), 2)
        position_delta = round(sum([x.delta for x in new_contracts]), 2)
        position_gamma = round(sum([x.gamma for x in new_contracts]), 2)
        position_rho = round(sum([x.rho for x in new_contracts]), 2)
        _position_cost = round(sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price) * -1.00 for x in new_contracts]), 2)
        for i in reversed(range(1, min([x.dte for x in new_contracts]))):
            print(f"Processing DTE: {i}")
            # iterate over DTE
            # for each DTE we need to figure out what the new position price is given the DTE
            delete = len(new_contracts)
            _tmp_new_contracts = []
            for ii in range(0, delete):
                # for contract in contracts:
                contract = new_contracts[ii]
                _tmp_ask = contract.last_price - (
                    contract.theta / 100.00 if contract.theta > 0 else contract.theta * -1 / 100.00)
                if contract.contract_type == ContractType.CALL:
                    _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i],
                                        callPrice=_tmp_ask).impliedVolatility
                else:
                    _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i],
                                        putPrice=_tmp_ask).impliedVolatility
                # recalculate greeks for the current DTE
                greeks = mibian.BS([asset_price, new_contracts[ii].strike, 0, i], volatility=_tmp_iv)
                # now create new contracts and replace the old
                # if contract.contract_type == ContractType.CALL:
                tmp_contract = Contract(new_contracts[ii].strike, asset_price, _tmp_ask, contract.contract_type,
                                        contract.type, contract.expiration, _tmp_iv, dte=i)
                if tmp_contract.contract_type == ContractType.CALL:
                    tmp_contract.delta = greeks.callDelta * 100
                    tmp_contract.theta = greeks.callTheta * 100
                    tmp_contract.gamma = greeks.gamma * 100
                    tmp_contract.rho = greeks.callRho * 100
                else:
                    tmp_contract.delta = greeks.putDelta * 100
                    tmp_contract.theta = greeks.putTheta * 100
                    tmp_contract.gamma = greeks.gamma * 100
                    tmp_contract.rho = greeks.callRho * 100
                if tmp_contract.type == PositionType.SHORT:
                    tmp_contract.delta = tmp_contract.delta * -1.00
                    tmp_contract.theta = tmp_contract.theta * -1.00
                    tmp_contract.gamma = tmp_contract.gamma * -1.00
                    tmp_contract.rho = tmp_contract.rho * -1.00

                _tmp_new_contracts.append(tmp_contract)

            # ok so one we're down here we have the contracts at n dte
            # clear the old contracts
            for j in range(0, len(new_contracts)):
                new_contracts[j] = _tmp_new_contracts[j]
                pass
            # for j in range(0, delete):
            #     new_contracts.pop()
            # pass
            position_theta = round(sum([x.theta for x in new_contracts]), 2)
            position_delta = round(sum([x.delta for x in new_contracts]), 2)
            position_gamma = round(sum([x.gamma for x in new_contracts]), 2)
            position_rho = round(sum([x.rho for x in new_contracts]), 2)
            _position_cost = round(
                sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price) * -1.00 for x in
                     new_contracts]), 2)
            # print(f"Starting DTE: {dte} Analyzing {i} DTE: New Theta {position_theta} New Position Cost: {_position_cost} Original: {original_position_cost}")
            #basically we either hit the target with theta decay percentage of contract
            valid = False
            if ((price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_PERCENTAGE) and (calculate_x_is_what_percentage_of_y((original_position_cost - _position_cost),original_position_cost )*-1 if price_goal < 0 else 1) <= price_goal):
                valid = True
            if (price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_DAYS_UNTIL and price_goal ==contracts[0].dte - i ):
                valid=True
            if (price_goal_change_type == PriceGoalChangeType.OPTION_THETA_DECAY_DAYS_DTE and price_goal ==i ):
                valid =True
            if valid:
                return {
                    "dte": i,
                    "days_to_target": contracts[0].dte - i,
                    "new_contract_price": _position_cost,
                    "original_position_price": original_position_cost,
                    "greeks": {
                        "delta": position_delta,
                        "theta": position_theta,
                        "gamma": position_gamma,
                        "rho": position_rho,
                    }
                }
        return {
            "dte": 0,
            "days_to_target": max([x.dte for x in contracts]),
            "position_cost": _position_cost + (position_theta / 100),
            "original_position_price": original_position_cost,
            "greeks": {
                "delta": position_delta,
                "theta": position_theta,
                "gamma": position_gamma,
                "rho": position_rho,
            }
}

def calculate_price_change_impact_contract(contracts,position_cost,asset_price, price_goal,  dte):
    # ok so the idea here is we have a list of contracts and we need to iterate through the DTE
    # we know that the change type is % of option contract

    original_position_cost= position_cost
    #ok so now we can iterate through each DTE and recalculate theta for each DTE and determine if the total value of the
    #position

    new_contracts = [x for x in contracts]
        # new_contracts= []
    # original_position_cost
    position_theta = round(sum([x.theta for x in new_contracts]), 2)
    position_delta = round(sum([x.delta for x in new_contracts]), 2)
    position_gamma = round(sum([x.gamma for x in new_contracts]), 2)
    position_rho = round(sum([x.rho for x in new_contracts]), 2)
    _position_cost = round(sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price) * -1.00 for x in new_contracts]), 2)
    for i in reversed(range(1, dte)):
        print(f"Processing DTE: {i}")
        #iterate over DTE
        #for each DTE we need to figure out what the new position price is given the DTE
        delete = len(new_contracts)
        _tmp_new_contracts= []
        for ii in range(0, delete):
        # for contract in contracts:
            contract = new_contracts[ii]
            _tmp_ask =  contract.last_price - (contract.theta/100.00 if contract.theta > 0 else contract.theta *-1/100.00)
            if contract.contract_type == ContractType.CALL:
                _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i], callPrice=_tmp_ask ).impliedVolatility
            else:
                _tmp_iv = mibian.BS([asset_price, new_contracts[ii].strike, 0, i],putPrice=_tmp_ask).impliedVolatility
            #recalculate greeks for the current DTE
            greeks = mibian.BS([asset_price, new_contracts[ii].strike, 0, i], volatility=_tmp_iv)
            #now create new contracts and replace the old
            # if contract.contract_type == ContractType.CALL:
            tmp_contract = Contract(new_contracts[ii].strike,asset_price,_tmp_ask , contract.contract_type, contract.type,contract.expiration,_tmp_iv, dte=i)
            if tmp_contract.contract_type == ContractType.CALL:
                tmp_contract.delta = greeks.callDelta*100
                tmp_contract.theta = greeks.callTheta*100
                tmp_contract.gamma = greeks.gamma*100
                tmp_contract.rho = greeks.callRho*100
            else:
                tmp_contract.delta = greeks.putDelta*100
                tmp_contract.theta = greeks.putTheta*100
                tmp_contract.gamma = greeks.gamma*100
                tmp_contract.rho = greeks.callRho*100
            if tmp_contract.type == PositionType.SHORT:
                tmp_contract.delta = tmp_contract.delta*-1.00
                tmp_contract.theta = tmp_contract.theta*-1.00
                tmp_contract.gamma = tmp_contract.gamma*-1.00
                tmp_contract.rho = tmp_contract.rho*-1.00


            _tmp_new_contracts.append(tmp_contract)

        #ok so one we're down here we have the contracts at n dte
        #clear the old contracts
        for j in range(0, len(new_contracts)):
            new_contracts[j]=_tmp_new_contracts[j]
            pass
        # for j in range(0, delete):
        #     new_contracts.pop()
                # pass
        position_theta = round(sum([x.theta for x in new_contracts]),2)
        position_delta = round(sum([x.delta for x in new_contracts]),2)
        position_gamma = round(sum([x.gamma for x in new_contracts]),2)
        position_rho = round(sum([x.rho for x in new_contracts]),2)
        _position_cost = round(sum([float(x.last_price) if x.type == PositionType.LONG else float(x.last_price)*-1.00 for x in new_contracts]),2)
        # print(f"Starting DTE: {dte} Analyzing {i} DTE: New Theta {position_theta} New Position Cost: {_position_cost} Original: {original_position_cost}")
        if calculate_x_is_what_percentage_of_y(((original_position_cost * -1.00)-(_position_cost*-1.00)), original_position_cost*-1.00) >= price_goal:
            return {
            "dte":i,
            "days_to_target":dte-i,
            "position_cost":_position_cost,
            "original_position_price":original_position_cost,
            "greeks": {
                "delta": position_delta,
                "theta": position_theta,
                "gamma": position_gamma,
                "rho": position_rho,
            }
        }
    return {
        "dte": 0,
        "days_to_target": dte ,
        "position_cost": _position_cost+(position_theta/100),
        "original_position_price": original_position_cost,
        "greeks": {
            "delta": position_delta,
            "theta": position_theta,
            "gamma": position_gamma,
            "rho": position_rho,
        }
    }



def calculate_price_change_impact_contract(price_goal_change_type, asset_price, price_goal,contract):
    ##in THEORY this should work for futures too?
    #basically, how much does the price need to change before our bid is met
    #can't think of a better way to do this than by the penny
    # _tmp_iv =
    #make this work for calls/puts/shorts/longs and single legs
    if contract.contract_type == ContractType==ContractType.CALL:
        _tmp_iv = mibian.BS([asset_price, contract.strike_price, 0, contract.dte], callPrice=contract.last_price).impliedVolatility
    else:
        _tmp_iv = mibian.BS([asset_price, contract.strike, 0, contract.dte], putPrice=contract.last_price).impliedVolatility
    # _tmp_iv = mibian.BS([asset_price, strike, 0, dte], callPrice=ask).impliedVolatility
    greeks = mibian.BS([asset_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
    per_cent_delta = float(greeks.callDelta / 100 if contract.contract_type == ContractType==ContractType.CALL else greeks.putDelta / 100)
    # _ticker_price_change_value = (0.1/100) * asset_price
    if price_goal_change_type   in [PriceGoalChangeType.ASSET_POINTS, PriceGoalChangeType.ASSET_PERCENTAGE, PriceGoalChangeType.ASSET_SET_PRICE]:
        #ok so basically we need to figure out what the bid is at our target asking price
        #if we are targeting specifc increase/decrease in asset price by points i.e. 5 points off SPY
        if price_goal_change_type == PriceGoalChangeType.ASSET_POINTS:
            _asset_price = asset_price - price_goal
        elif price_goal_change_type == PriceGoalChangeType.ASSET_PERCENTAGE:
        #otherwise we are targeting a specific percentage of the underlying
            p = float(float(price_goal / 100) * asset_price)
            print(f"{price_goal} percent of ${asset_price}: ${p}")
            _asset_price = asset_price + p #* (-1 if p > 0 else 1)
        else:
        #finally, if niether, we assume we are inputting a set asset price (i.e. SPY @ 561)
            _asset_price = price_goal
        _tmp_price = asset_price
        _tmp_ask = contract.last_price

        #either way above we need to calculcate what the target bid would be, iterating over the price of the asset by 0.01 until we reach the target price of the underlying
        if contract.contract_type ==ContractType.CALL:
            _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], callPrice=_tmp_ask).impliedVolatility
        else:
            _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], putPrice=_tmp_ask).impliedVolatility
        # _tmp_iv = mibian.BS([_tmp_price, strike, 0, dte], callPrice=_tmp_ask).impliedVolatility
        greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
        per_cent_delta = float(greeks.callDelta if contract.contract_type == ContractType==ContractType.CALL else greeks.putDelta )/ 100
        #ok so this is going to get tricky
        while round(_asset_price,2) != round(_tmp_price,2):
            if _asset_price > _tmp_price:
                if contract.contract_type==ContractType.CALL: #so for a long call price needs to go up
                    multiplier = 1 #we want long calls to go up

                else:
                    multiplier = -1 #long puts we want to go down

            else: #if the bid is less than the ask, we want short puts to be profitable, long puts not to be
                if contract.contract_type == ContractType.CALL:
                    multiplier = -1  # we want long calls to go down
                else:
                    multiplier = 1  # long puts we want to go up
            _tmp_price = _tmp_price - 0.01 * (-1 if _tmp_price < _asset_price else 1)  # one send decrements
            _tmp_ask = _tmp_ask - per_cent_delta * multiplier
            # print(f"Calculating implied volatility")
            if contract.contract_type == ContractType.CALL:

                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], callPrice=_tmp_ask).impliedVolatility
            else:
                _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], putPrice=_tmp_ask).impliedVolatility
            # print(f"Recalculating greeks")
            greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
            # print(f"Recalculating per-cent delta")
            per_cent_delta = float(greeks.callDelta if contract.contract_type == ContractType==ContractType.CALL else greeks.putDelta )/ 100
        bid = _tmp_ask
        return {
            "asset_price": round(_tmp_price, 2),
            "asset_change": round(float(_tmp_price - asset_price), 2),
            "asset_percentage_change": round(
                calculate_x_is_what_percentage_of_y(float(_tmp_price - asset_price), asset_price), 2),
            "last_contract_price": round(contract.last_price, 2),
            "new_contract_price": round(bid, 2),
            "greeks": {
                "delta": greeks.callDelta if contract.contract_type == ContractType.CALL else greeks.putDelta,
                "theta": greeks.callTheta if contract.contract_type == ContractType.CALL else greeks.putTheta,
                "gamma": greeks.gamma,
                "vega": greeks.vega,
                "implied_violatility": _tmp_iv
            }
        }
        # _tmp_price = _asset_price
    else:

        #basically for all cases but asset changes, we can more ore less re-use the existing code, we just need to set our bid accordingly
        # if price_goal_change_type == PriceGoalChangeType.OPTION_SET_PRICE:
        if price_goal_change_type == PriceGoalChangeType.OPTION_TICK:
            bid = contract.last_price + (price_goal * -1 if contract.type == PositionType.SHORT else 1)
        elif price_goal_change_type == PriceGoalChangeType.OPTION_PERCENTAGE:
            p = float(float(price_goal/100)*contract.last_price)# * (-1 if position_type == PositionType.LONG else 1 )
            print(f"{price_goal} percent of ${contract.strike} {contract.type} Option priced at ${contract.last_price}: ${(p * -1 if contract.type == PositionType.SHORT else p)}")
            bid  = contract.last_price +(p * -1 if contract.type == PositionType.SHORT else p)
            pass
            # _tmp_ask =
        else:
            bid = price_goal

    #don't care about vega rn
    _tmp_contract_last_price = contract.last_price
    _tmp_price = asset_price
    # while _tmp_ask < bid: #stop at a 0.05 ask, etrade order price min increment lol
    # while (round(_tmp_ask,2) < round(bid,2) if position_type == PositionType.LONG else round(bid,2) < round(_tmp_ask,2)):
    while round(_tmp_contract_last_price,2) != round(bid,2):# and _tmp_ask > 0.05: #stop at a 0.05 ask, etrade order price min increment lol
        #short options we want the value to go down of the contract
        #so if the  bid is greater than the ask it's for a loser
        if bid > _tmp_contract_last_price:
            if contract.contract_type==ContractType.CALL: #so for a long call price needs to go up
                multiplier = -1 #we want long calls to go up

            else:
                multiplier = 1 #long puts we want to go down
        else: #if the bid is less than the ask, we want short puts to be profitable, long puts not to be
            if contract.contract_type == ContractType.CALL:
                multiplier = 1  # we want long calls to go down
            else:
                multiplier = -1  # long puts we want to go up

        _tmp_price = _tmp_price - 0.01 * multiplier#(-1 if bid > _tmp_contract_last_price else 1)#increase if the bid is greater that the contract price
        _tmp_contract_last_price = _tmp_contract_last_price - per_cent_delta * multiplier#(-1 if bid > _tmp_contract_last_price else 1)
        # _tmp_contract_last_price = _tmp_contract_last_price - per_cent_delta * delta_multiplier#(-1 if bid > _tmp_contract_last_price else 1)
        if contract.contract_type == ContractType.CALL:
            _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], callPrice=_tmp_contract_last_price).impliedVolatility
        else:
            _tmp_iv = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], putPrice=_tmp_contract_last_price).impliedVolatility
        greeks = mibian.BS([_tmp_price, contract.strike, 0, contract.dte], volatility=_tmp_iv)
        per_cent_delta = float(greeks.callDelta if contract.contract_type == ContractType.CALL else greeks.putDelta ) /100

    if round(bid,2) == round(_tmp_contract_last_price,2):
        #assuming we get here, we know that the order would have been filled
        return {
            "asset_price": round(_tmp_price,2),
            "asset_change": round(float(_tmp_price - asset_price),2),
            "asset_percentage_change": round(calculate_x_is_what_percentage_of_y(float(_tmp_price - asset_price), asset_price),2),
            "last_contract_price": round(contract.last_price,2),
            "new_contract_price": round(bid,2),
            "greeks": {
                "delta": greeks.callDelta if contract.contract_type==ContractType.CALL else greeks.putDelta,
                "theta": greeks.callTheta if contract.contract_type==ContractType.CALL else greeks.putTheta,
                "gamma": greeks.gamma,
                "vega": greeks.vega,
                "implied_violatility": _tmp_iv
            }
        }

    else:
        raise Exception(f"Decreased call price from {asset_price} ==> {_tmp_price} did not reach bid price")
# def calculate_price_change_for_exit(price_goal_change_type, position_type,asset_price,strike_price, price_goal, ask,  dte):
#     ##in THEORY this should work for futures too?
#     #basically, how much does the price need to change before our bid is met
#     #can't think of a better way to do this than by the penny
#     # _tmp_iv =
#     if position_type == PositionType.LONG:
#         _tmp_iv = mibian.BS([asset_price, strike_price, 0, dte], callPrice=ask).impliedVolatility
#     else:
#         _tmp_iv = mibian.BS([asset_price, strike_price, 0, dte], putPrice=ask).impliedVolatility
#     # _tmp_iv = mibian.BS([asset_price, strike_price, 0, dte], callPrice=ask).impliedVolatility
#     greeks = mibian.BS([asset_price, strike_price, 0, dte], volatility=_tmp_iv)
#     per_cent_delta = float(greeks.callDelta / 100 if position_type == PositionType.LONG else greeks.putDelta / 100)
#     # _ticker_price_change_value = (0.1/100) * asset_price
#     if price_goal_change_type   in [PriceGoalChangeType.ASSET_POINTS, PriceGoalChangeType.ASSET_PERCENTAGE, PriceGoalChangeType.ASSET_SET_PRICE]:
#         #ok so basically we need to figure out what the bid is at our target asking price
#         if price_goal_change_type == PriceGoalChangeType.ASSET_POINTS:
#             _asset_price = asset_price - price_goal
#         elif price_goal_change_type == PriceGoalChangeType.ASSET_PERCENTAGE:
#             p = float(float(price_goal / 100) * asset_price)
#             print(f"{price_goal} percent of ${asset_price}: ${p}")
#             _asset_price = asset_price - p *(-1 if position_type == PositionType.LONG else 1)
#         else:
#             _asset_price = price_goal
#         _tmp_price = asset_price
#         _tmp_ask = ask
#         if position_type == PositionType.LONG:
#             _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], callPrice=_tmp_ask).impliedVolatility
#         else:
#             _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], putPrice=_tmp_ask).impliedVolatility
#         # _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], callPrice=_tmp_ask).impliedVolatility
#         greeks = mibian.BS([_tmp_price, strike_price, 0, dte], volatility=_tmp_iv)
#         per_cent_delta = float(greeks.callDelta if position_type == PositionType.LONG else greeks.putDelta )/ 100
#         while round(_asset_price,2) != round(_tmp_price,2):
#         # while (round(_tmp_price,2) <= round(_asset_price,2) if position_type == PositionType.LONG else _asset_price <= round(_tmp_price,2)) :# and _tmp_ask > 0.05: #stop at a 0.05 ask, etrade order price min increment lol
#             # if round(_tmp_price, 2) == round(_asset_price, 2):
#             #     break
#             _tmp_price = _tmp_price - 0.01 * (-1 if position_type == PositionType.LONG else 1)  # one send decrements
#             _tmp_ask = _tmp_ask - per_cent_delta * (-1 if position_type == PositionType.LONG else 1)
#             if position_type == PositionType.LONG:
#                 _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], callPrice=_tmp_ask).impliedVolatility
#             else:
#                 _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], putPrice=_tmp_ask).impliedVolatility
#             greeks = mibian.BS([_tmp_price, strike_price, 0, dte], volatility=_tmp_iv)
#             per_cent_delta = float(greeks.callDelta if position_type == PositionType.LONG else greeks.putDelta )/ 100
#         bid = _tmp_ask
#         # _tmp_price = _asset_price
#     else:
#
#         #basically for all cases but asset changes, we can more ore less re-use the existing code, we just need to set our bid accordingly
#         # if price_goal_change_type == PriceGoalChangeType.OPTION_SET_PRICE:
#         if price_goal_change_type == PriceGoalChangeType.OPTION_TICK:
#             bid = ask + price_goal
#         elif price_goal_change_type == PriceGoalChangeType.OPTION_PERCENTAGE:
#             p = float(float(price_goal/100)*ask)# * (-1 if position_type == PositionType.LONG else 1 )
#             print(f"{price_goal} percent of ${strike_price} {position_type} Option priced at ${ask}: ${p}")
#             bid  = ask +p
#             pass
#             # _tmp_ask =
#         else:
#             bid = price_goal
#
#     # bid = bid
#     # per_cent_gamma = float(gamma / 100)
#     #don't care about vega rn
#     _tmp_ask = ask
#     _tmp_price = asset_price
#     # if bid > ask:
#     #     raise Exception(f"Just slap the ask {ask}, it's less than your bid {bid}")
#
#     # while _tmp_ask < bid: #stop at a 0.05 ask, etrade order price min increment lol
#     # while (round(_tmp_ask,2) < round(bid,2) if position_type == PositionType.LONG else round(bid,2) < round(_tmp_ask,2)):
#     while _tmp_ask <= bid:# and _tmp_ask > 0.05: #stop at a 0.05 ask, etrade order price min increment lol
#
#         # print(_tmp_price)
#         if _tmp_ask >= bid:
#             break
#         _tmp_price = _tmp_price - 0.01 * (-1 if position_type == PositionType.LONG else 1)
#         _tmp_ask = _tmp_ask - per_cent_delta * (-1 if position_type == PositionType.LONG else 1)
#         if position_type == PositionType.LONG:
#             _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], callPrice=_tmp_ask).impliedVolatility
#         else:
#             _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], putPrice=_tmp_ask).impliedVolatility
#         greeks = mibian.BS([_tmp_price, strike_price, 0, dte], volatility=_tmp_iv)
#         per_cent_delta = float(greeks.callDelta if position_type == PositionType.LONG else greeks.putDelta ) /100
#
#         # if round(_tmp_ask, 2) == round(bid, 2):
#         #     break
#
#
#     # else:
#     #     while _tmp_ask > bid and _tmp_ask > 0.05:  # stop at a 0.05 ask, etrade order price min increment lol
#     #         _tmp_price = _tmp_price + 0.01  # one send decrements
#     #         _tmp_ask = _tmp_ask + per_cent_delta
#     #         # ok so calculate new ask and price first, we will need them to calculate the rest
#     #         #
#     #         _tmp_iv = mibian.BS([_tmp_price, strike_price, 0, dte], callPrice=_tmp_ask).impliedVolatility
#     #         greeks = mibian.BS([_tmp_price, strike_price, 0, dte], volatility=_tmp_iv)
#     #         per_cent_delta = float(greeks.putDelta / 100)
#
#     if round(bid,2) <= round(_tmp_ask,2):
#         #assuming we get here, we know that the order would have been filled
#         return {
#             "asset_price": round(_tmp_price,2),
#             "asset_change": round(float(_tmp_price - asset_price),2),
#             "asset_percentage_change": round(calculate_x_is_what_percentage_of_y(float(_tmp_price - asset_price), asset_price),2),
#             "bid": round(ask,2),
#             "ask": round(bid,2),
#             "greeks": {
#                 "delta": greeks.callDelta if position_type == PositionType.LONG else greeks.putDelta,
#                 "theta": greeks.callTheta if position_type == PositionType.LONG else greeks.putTheta,
#                 "gamma": greeks.gamma,
#                 "vega": greeks.vega,
#                 "implied_violatility": _tmp_iv
#             }
#         }
#
#     else:
#         raise Exception(f"Decreased call price from {asset_price} ==> {_tmp_price} did not reach bid price")
