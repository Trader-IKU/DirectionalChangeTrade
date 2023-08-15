# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 07:21:48 2023

@author: Trader-IKU
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../Libraries/trade"))

import polars as pl
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta 

from const import Const
from converter import Converter
from candle_chart import CandleChart, BandPlot, makeFig, gridFig, Colors
from dc_detector import DCDetector, Direction, indicators, TimeUnit, coastline, EventStatus

class TradeRuleParams:
    def __init__(self):
        self.th_percent: float = 0
        self.horizon = 0
        self.pullback_percent: float = 0
        self.close_timelimit: float = 2.0
        self.losscut: float = 0.0        
        
class Position:
    def __init__(self):
        self.entry_time: datetime = None
        self.entry_price: float = 0
        self.losscut_price: float = 0
        self.close_limit: datetime = None
        self.profit: float = 0
        self.time: timedelta = None
        self.close_time: datetime = None
        self.close_price: float = 0
        
class DataBuffer:
    def __init__(self, time, prices):
        self.time = time
        self.prices = prices

    def update(self, time, prices):
        self.time += time
        self.prices += prices        
        
class AlternateTrade:
    def __init__(self, param_up: TradeRuleParams, param_down: TradeRuleParams):
        self.param_up = param_up
        self.param_down = param_down
        
    def back_test(self, data: DataBuffer):
        detector = DCDetector(data.time, data.prices) 
        time = data.time
        prices = data.prices
        
        n = len(prices)   
        i = 100
        t = time[:i]
        p = prices[:i]
        
        detector.run(t, p, self.param_up.th_percent, self.param_down.th_percent)
        i += 100
        while i < n:
            t = time[: i]
            p = prices[: i]
            count = detector.update(t, p)
            if count == 0:
                pair = detector.pair
                if pair is None:
                    print('Found: ', count, ' pair:', pair)
                    
                else:
                    print('Found: ', count, ' pair:', pair[0].valid(), pair[1].valid())
            i += 100
        return detector.events
# -----

def plot_events(events, time, price, date_format=CandleChart.DATE_FORMAT_DAY_HOUR):
    fig, ax = makeFig(1, 1, (30,10))
    chart = CandleChart(fig, ax, title='', date_format=date_format)
    #chart.drawCandle(time, op, hi, lo, cl)
    chart.drawLine(time, price, color='blue')
    for i, evs in enumerate(events):

        dc_event, os_event = evs
        if dc_event is None:
            print('#' +str(i + 1) + '... No DC event and OS event')
        if dc_event.direction == Direction.Up:
            c = 'green'
        else:
            c = 'red'
        x = dc_event.term[1]
        y = dc_event.price[1]
        chart.drawMarker(x, y, 'o', c, markersize=10)
        chart.drawLine(dc_event.term, dc_event.price, should_set_xlim=False, linewidth=3.0, color=c)
        if os_event is None:
            print('#' +str(i + 1) + '... No OS event')
            break
        chart.drawLine(os_event.term, os_event.price, should_set_xlim=False, linewidth=3.0, color=c, linestyle='dotted')
        #print('<' + str(i + 1) + '>')
        #dc_event.desc()
        #os_event.desc()
        #print('--')
        (TMV, T, R) = indicators(dc_event, os_event, TimeUnit.DAY)
        label1 = "#{}  TMV: {:.5f}  ".format(i + 1, TMV)
        label2 = " T: {}  R: {:.5f}".format(T, R)
        chart.drawText(x, y + (chart.getYlimit()[1] - chart.getYlimit()[0]) * 0.05, label1 + ' \n' + label2)
        print(label1 + label2)
        
        
def test():
    with open('./data/TICK/GBPJPY_2023.pkl', 'rb') as f:
        ticks = pickle.load(f)
    print('Load size:', len(ticks[Const.TIME]))

    time = ticks[Const.TIME]
    prices = ticks[Const.PRICE]
    n = 30000
    time = time[-n:]
    prices = prices[-n:]    
    
    m = n #2000
    buffer = DataBuffer(time[:m], prices[:m])
    param_up = TradeRuleParams()
    param_up.th_percent = 0.04
    param_down = TradeRuleParams()
    param_down.th_percent = 0.04
    trade = AlternateTrade(param_up, param_down)
    events = trade.back_test(buffer)
    plot_events(events, time, prices)


if __name__ == '__main__':
    test()