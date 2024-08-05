from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

from enums import PositionType
# import s3
from history import load_ticker_history_pd_frame
from indicators import load_macd, load_sma, load_dmi_adx, load_rsi, load_support_resistance, load_breakout, \
    load_vix_rsi, load_stochastic_rsi
from indicators import load_dmi_adx
from indicators import  load_death_cross, load_golden_cross, determine_death_cross_alert_type,determine_golden_cross_alert_type, did_golden_cross_alert, did_death_cross_alert

def plot_ticker(ticker, ticker_history, module_config):
    # df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')
    df = load_ticker_history_pd_frame(ticker, ticker_history, convert_to_datetime=True, human_readable=True)
    fig = go.Figure(data=[go.Candlestick(x=df['date'],
                    open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'])
                         ])
    # fig.append_trace()
    fig.update_layout(xaxis_rangeslider_visible=False,xaxis=dict(type = "date"),height=40000)
    fig.show()


def plot_ticker_with_indicators(ticker, ticker_history, indicator_data, module_config):

    df = load_ticker_history_pd_frame(ticker, ticker_history[-module_config['plot_bars']:], convert_to_datetime=True, human_readable=True)
    subplot_titles = [x  for x in indicator_data.keys() if not indicator_data[x]['overlay']]
    # candle_fig  = make_subplots(rows=len([not x['overlay'] for x in indicator_data.values()]), cols=2, subplot_titles=subplot_titles)
    candle_fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                         open=df['open'], high=df['high'],
                                         low=df['low'], close=df['close'],name=ticker)] )
    r = 1
    # c =2
    indicator_figure = make_subplots(rows=len([not x['overlay'] for x in indicator_data.values()]), cols=1, subplot_titles=subplot_titles)
    for key,x in indicator_data.items():
        if x['overlay']:
            if type(x['plot']) not in [list, tuple]:
                candle_fig.add_trace(x['plot'])
            else:
                for i in range(0, len(x['plot'])):
                    if type(x['plot'][i]) == go.Scatter:

                        candle_fig.add_trace(x['plot'][i])
                    else:
                        candle_fig.add_shape(x['plot'][i])
        else:
            if type(x['plot']) not in [list,tuple]:
                indicator_figure.add_trace(x['plot'], row=r, col=1)
            else:
                for i in range(0, len(x['plot'])):
                    indicator_figure.add_trace(x['plot'][i], row=r, col=1)
            r = r + 1

    candle_fig.update_layout(xaxis_rangeslider_visible=False, xaxis=dict(type="date"))
    min_percent=(50/100)*min([x.low for x in ticker_history])
    max_percent=(50/100)*max([x.low for x in ticker_history])
    # fig.update_yaxes( type='log')

    candle_fig.update_xaxes(rangebreaks=[
            dict(bounds=["sat", "mon"]),
            dict(bounds=[16, 9.5], pattern="hour"),

        ])
    # candle_fig.show()
    # indicator_figure.show()
    figures_to_html(ticker, [candle_fig, indicator_figure], f"html/{module_config['timespan_multiplier']}{module_config['timespan']}{ticker}.html")

    # s3.upload_file(f"html/{module_config['timespan_multiplier']}{module_config['timespan']}{ticker}.html","www.mpb-traders-data.com")
    pass
def plot_ticker_with_indicators_and_positions(ticker, ticker_history, indicator_data,strategy_data, module_config):

    df = load_ticker_history_pd_frame(ticker, ticker_history[-module_config['plot_bars']:], convert_to_datetime=True, human_readable=True)
    subplot_titles = [x  for x in indicator_data.keys() if not indicator_data[x]['overlay']]
    # candle_fig  = make_subplots(rows=len([not x['overlay'] for x in indicator_data.values()]), cols=2, subplot_titles=subplot_titles)
    candle_fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                         open=df['open'], high=df['high'],
                                         low=df['low'], close=df['close'],name=ticker)] )
    r = 1
    # c =2
    indicator_figure = make_subplots(rows=len([not x['overlay'] for x in indicator_data.values()]), cols=1, subplot_titles=subplot_titles)
    for key,x in indicator_data.items():
        if x['overlay']:
            if type(x['plot']) not in [list, tuple]:
                candle_fig.add_trace(x['plot'])
            else:
                for i in range(0, len(x['plot'])):
                    if type(x['plot'][i]) == go.Scatter:

                        candle_fig.add_trace(x['plot'][i])
                    else:
                        candle_fig.add_shape(x['plot'][i])
        else:
            if type(x['plot']) not in [list,tuple]:
                indicator_figure.add_trace(x['plot'], row=r, col=1)
            else:
                for i in range(0, len(x['plot'])):
                    indicator_figure.add_trace(x['plot'][i], row=r, col=1)
            r = r + 1
    #now we do the strategy figure
    r = 1
    # c =2
    strategy_figure = make_subplots(rows=len([not x['overlay'] for x in strategy_data.values()]), cols=1,
                                     subplot_titles=subplot_titles)
    for key, x in strategy_data.items():
        if x['overlay']:
            if type(x['plot']) not in [list, tuple]:
                candle_fig.add_trace(x['plot'])
            else:
                for i in range(0, len(x['plot'])):
                    if type(x['plot'][i]) == go.Scatter:

                        candle_fig.add_trace(x['plot'][i])
                    else:
                        candle_fig.add_shape(x['plot'][i])
        else:
            if type(x['plot']) not in [list, tuple]:
                strategy_figure.add_trace(x['plot'], row=r, col=1)
            else:
                for i in range(0, len(x['plot'])):
                    strategy_figure.add_trace(x['plot'][i], row=r, col=1)
            r = r + 1
    candle_fig.update_layout(xaxis_rangeslider_visible=False, xaxis=dict(type="date"))
    min_percent=(50/100)*min([x.low for x in ticker_history])
    max_percent=(50/100)*max([x.low for x in ticker_history])
    # fig.update_yaxes( type='log')

    candle_fig.update_xaxes(rangebreaks=[
            dict(bounds=["sat", "mon"]),
            dict(bounds=[16, 9.5], pattern="hour"),

        ])
    # candle_fig.show()
    # indicator_figure.show()
    figures_to_html(ticker, [candle_fig, indicator_figure, strategy_figure], f"html/{module_config['timespan_multiplier']}{module_config['timespan']}{ticker}.html")

    # s3.upload_file(f"html/{module_config['timespan_multiplier']}{module_config['timespan']}{ticker}.html","www.mpb-traders-data.com")
    pass

# def plot_sma(ticker, ticker_history,indicator_data, module_config):
#     df = load_ticker_history_pd_frame(ticker, ticker_history, convert_to_datetime=True, human_readable=True)
#     return go.Scatter(
#         x=df['date'],
#         y=[indicator_data[x.timestamp] for x in ticker_history],
#         name=f"sma{module_config['sma_window']}", mode='lines', line={'color':'blue'}
#     )

def plot_indicator_data(ticker, ticker_history,indicator_data, module_config, name='', color='blue', max=100):
    df = load_ticker_history_pd_frame(ticker, ticker_history, convert_to_datetime=True, human_readable=True)
    return go.Figure(data=go.Scatter(
        x=df['date'],
        y=[indicator_data[x.timestamp] for x in ticker_history if x.timestamp in indicator_data],
        name=name, mode='lines', line={'color':color, 'width':1},
    ), layout_yaxis_range=[0, max]).data[0]


def plot_indicator_data_dual_y_axis(ticker, ticker_history,indicator_data, module_config,keys=[],colors=['blue']):

    fig  = make_subplots(rows=1, cols=1)
    # fig.add_trace()
    counter = 2

    for i in range(0, len(keys)):

        fig.add_trace(plot_indicator_data(ticker, ticker_history,indicator_data[keys[i]],module_config, color=colors[i], name=keys[i]), row=1, col=1)


    return fig.data


def plot_strategy_lines(ticker, ticker_history,strategy_data, module_config):
    lines = []
    for position in strategy_data:
        text = ['' for x in strategy_data]

        # text.insert(-1, sr_level)
        for leg in position.legs():
            starting_timestamp_index = [x.dt for x in ticker_history].index(position.open_date)
            x = [ticker_history[i].dt for i in range(starting_timestamp_index, starting_timestamp_index+position.length)]
            y = [leg.strike for i in range(starting_timestamp_index, starting_timestamp_index+position.length)]
            lines.append(go.Scatter(
                x=x,
                y=y,
                mode="lines+text",
                line={'width': 2, 'color': 'red' if not position.profitable else 'green', 'dash': 'dash' if leg.type == PositionType.SHORT else 'solid'},
                name=f"{str(position)} {str(leg)}",
                text=text,
                textposition="bottom left"
            )

        )
    return lines
def plot_sr_lines(ticker, ticker_history,indicator_data, module_config):

    lines = []
    for sr_level in indicator_data:
        text = ['' for x in ticker_history[:1]]
        # text.insert(-1, sr_level)
        lines.append(go.Scatter(
                    x=[x.dt for x in ticker_history],
                    y=[sr_level for x in ticker_history],
                    mode="lines+text",
                    line={'width':0.5, 'color':'blue', 'dash':'dashdot'},
                    name=f"S/R {sr_level}",
                    text=text,
                    textposition="bottom right"
)

        )
        # lines.append(go.Line(
        #
        #     x0=ticker_history[0].dt,
        #     y0=sr_level,
        #     x1=ticker_history[-1].dt,
        #     y1=sr_level, line={'color': 'blue', 'width': 0.25, 'dash':'dashdot'},
        # ))
    return lines


def figures_to_html(ticker,figs, filename):
    with open(filename, 'w') as dashboard:
        dashboard.write(f"<html><head></head><body><h2>${ticker}</h2>" + "\n")
        for fig in figs:
            inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
            dashboard.write(inner_html)
        dashboard.write("</body></html>" + "\n")

def build_strategy_dict(ticker, ticker_history,strategy_data, module_config):
    strategy_dict = {
        module_config['strategy']:
            {
                "plot" : plot_strategy_lines(ticker, ticker_history,strategy_data,module_config),
                "overlay":True

            }
    }
    return strategy_dict
def build_indicator_dict(ticker, ticker_history, module_config, connection):
    indicator_dict = {
        "breakout": {
            "plot": plot_indicator_data_dual_y_axis(ticker, ticker_history[-module_config['plot_bars']:],
                                                    load_breakout(ticker, ticker_history, module_config),
                                                    module_config, keys=['xo', 'xu'],
                                                    colors=['green', 'red']),
            "overlay": False
        },
        "sma": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_sma(ticker, ticker_history, module_config), module_config,
                                        name='sma10'),
            "overlay": True
        },
        "ema50": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_golden_cross(ticker, ticker_history, module_config)['sma_short'],
                                        module_config, name='ema50', color='yellow'),
            "overlay": True
        },
        "ema200": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_golden_cross(ticker, ticker_history, module_config)['sma_long'],
                                        module_config, name='ema200', color='purple'),
            "overlay": True
        },

        "rsi": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_rsi(ticker, ticker_history, module_config),
                                        module_config, name='rsi', color='Blue'),
            "overlay": False
        },
        "stochastic_rsi": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_stochastic_rsi(ticker, ticker_history, module_config),
                                        module_config, name='rsi', color='Blue'),
            "overlay": False
        },
        "vix_rsi": {
            "plot": plot_indicator_data(ticker, ticker_history[-module_config['plot_bars']:],
                                        load_vix_rsi(ticker, ticker_history, module_config, connection),
                                        module_config, name='rsi', color='Blue'),
            "overlay": False
        },
        "macd": {
            "plot": plot_indicator_data_dual_y_axis(ticker, ticker_history[-module_config['plot_bars']:],
                                                    load_macd(ticker, ticker_history, module_config),
                                                    module_config, keys=['macd', 'signal'], colors=['green', 'red']),
            "overlay": False
        },
        "dmi": {
            "plot": plot_indicator_data_dual_y_axis(ticker, ticker_history[-module_config['plot_bars']:],
                                                    load_dmi_adx(ticker, ticker_history, module_config),
                                                    module_config, keys=['dmi+', 'dmi-', 'adx'],
                                                    colors=['green', 'red', 'blue']),
            "overlay": False
        },

        # "s/r levels": {
        #     "plot": plot_sr_lines(ticker, ticker_history[-module_config['plot_bars']:],
        #                           load_support_resistance(ticker, ticker_history, module_config, flatten=True),
        #                           module_config),
        #     "overlay": True
        # }

    }

    return indicator_dict