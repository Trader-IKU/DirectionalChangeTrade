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
        
def param_long():
    param = TradeRuleParams()
    param.th_percent = 0.05
    param.horizon = 0
    param.pullback_percent = 0.04
    param.close_timelimit = 10
    param.losscut = 1
    return param
    
def param_short():
    param = TradeRuleParams()
    param.th_percent = 0.05
    param.horizon = 0
    param.pullback_percent = 0.04
    param.close_timelimit = 10    
    param.losscut = 1 
    return param 
    
# --

class Kind:
    Short = 'short'
    Long = 'long'
    
class Cause:
    losscut = 'losscut'
    timelimit = 'timelimit'
    eventover = 'eventover'
    
class Position:
    def __init__(self, kind: Kind, param: TradeRuleParams, event_no: int):
        self.kind: Kind = kind
        self.rule = param
        self.event_no = event_no
        self.entry_time: datetime = None
        self.entry_price: float = 0
        self.losscut_price: float = 0
        self.close_limit: datetime = None
        self.profit: float = 0
        self.close_time: datetime = None
        self.close_price: float = 0
        self.closed = False
        self.cause = None
        
    def entry(self, time, price):
        self.etnry_time = time
        self.entry_price = price
        
    def close(self, time, price, cause: Cause):
        self.close_time = time
        self.close_price = price
        self.cause = cause
        self.profit = 100 * ((price / self.entry_price) - 1.0)
        self.closed = True
        
    def is_closed(self):
        return self.closed
    
    def desc(self):
        print('event_no:', self.event_no, 'kind:', self.kind, 'cause:', self.cause, 'profit:', self.profit)
# --
    
class DataBuffer:
    def __init__(self, time, prices):
        self.time = time
        self.prices = prices

    def update(self, time, prices):
        self.time += time
        self.prices += prices        
# --
        
class AlternateTrade:
    def __init__(self, param_up: TradeRuleParams, param_down: TradeRuleParams):
        self.param_up = param_up
        self.param_down = param_down
        self.positions = []
        
    def close_all(self, time, prices, dc_event):
        for position in self.positions:
            if not position.is_closed():
                position.close(time[-1], prices[-1], Cause.eventover)       
        
    def check_close(self, time, prices, dc_event):
        for position in self.positions:
            if not position.is_closed():
                # profitをアップデート
                profit = 100 * ((prices[-1] / position.entry_price) - 1.0)
                # losscut チェック
                if position.kind == Kind.Long:
                    if profit <= - 1.0 * position.rule.losscut:
                        position.close(time[-1], prices[-1], Cause.losscut)
                else:
                    if profit >= position.rule.losscut:
                        position.close(time[-1], prices[-1], Cause.losscut)
                # 時間チェック
                dt = time[-1] - dc_event.term[1]
                k = dt / (dc_event.term[1] - dc_event.term[0])
                if position.kind == Kind.Long:
                    if k > position.rule.close_timelimit:
                        position.close(time[-1], prices[-1], Cause.timelimit)
                else:
                    if k > position.rule.close_timelimit:
                        position.close(time[-1], prices[-1], Cause.timelimit)
                
    def entry(self, time, prices, i_last, dc_event, event_no):
        i = len(prices) - 1
        dt = time[i_last] - time[i]
        di = i - i_last + 1
        if dc_event.direction == Direction.Up:
            #Long
            if(type(self.param_up.horizon)) == int:
                if di <= self.param_up.horizon:
                    return
            else:
                if dt <= self.param_up.horizon:
                    return
            d =  100 * (prices[-1] / dc_event.price[1] -1)
            if d < -1 * self.param_up.pullback_percent:
                return
        else:
            #Short
            if(type(self.param_down.horizon)) == int:
                if di <= self.param_down.horizon:
                    return
            else:
                if dt <= self.param_down.horizon:
                    return
            d =  100 * (prices[-1] / dc_event.price[1] -1)
            if d >   self.param_down.pullback_percent:
                return        
        # Entry
        if dc_event.direction == Direction.Up:       
            position = Position(Kind.Long, self.param_up, event_no)
        else:
            position = Position(Kind.Short, self.param_down, event_no)
        position.entry(time[-1], prices[-1])
        self.positions.append(position)
        return
# --
        
class Handling:    
    def __init__(self, trade: AlternateTrade):
        self.trade = trade
        pass
        
    def back_test(self, data: DataBuffer):
        detector = DCDetector(data.time, data.prices) 
        time = data.time
        prices = data.prices        
        n = len(prices)   
        i = 200
        t = time[:i]
        p = prices[:i]
        detector.run(t, p, self.trade.param_up.th_percent, self.trade.param_down.th_percent)
        i_last = i
        i += 5
        while i < n:            
            t = time[: i]
            p = prices[: i]
            dc_event_num = detector.update(t, p)
            if dc_event_num == 0:
                if len(detector.events) > 0:
                    dc_event = detector.events[-1][0]
                    self.trade.check_close(time, prices, dc_event)   
            else:
                dc_event = detector.pair[0]
                self.trade.close_all(time, prices, dc_event)
                self.trade.entry(t, p, i_last, dc_event, len(detector.events))
            i += 5
        return detector.events, self.trade.positions
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
        (TMV, R, T, k, Tdc, Tos) = indicators(dc_event, os_event, TimeUnit.DAY)
        s1 = "#{}  k: {:.3f}  ".format(i + 1, k)
        chart.drawText(x, y + (chart.getYlimit()[1] - chart.getYlimit()[0]) * 0.05, s1)
        s2 = '#{} TMV: {:.5f}  T:{}  k:{} index:{}-{} dc_end_time: {}'.format(i + 1, TMV, T, k, dc_event.index, os_event.index, dc_event.term[1])
        print(s2)
        
def disp(positions):
    for position in positions:
       position.desc()
       
def validation(time, prices, th_up, th_down):
    dc_end_up = 1 
    dc_end_down = -1
    up = 1 
    down = -1
    n = len(prices)
    ref = prices[0]
    refs = np.full(n, np.nan)
    ror = np.full(n, np.nan)
    status = np.full(n, np.nan)
    
    i = 1
    for i in range(1, n):
        refs[i] = ref
        r = (prices[i] / ref - 1) * 100 
        ror[i] = r        
        if r >= th_up:
            status[i] = dc_end_up
            i_ref = i
            begin = i + 1 
            ref = prices[i]
            direction = down
            break
        elif r <= -1 * th_down :
            status[i] = dc_end_down
            i_ref = i
            begin = i + 1
            direction = up
            ref = prices[i]
            break

    for i in range(begin, n):
        refs[i] = ref
        r = (prices[i] / ref - 1) * 100 
        ror[i] = r
        if direction == up:
            if r >= th_up:
                status[i] = dc_end_up
                ref = prices[i]
                i_ref = i
                continue 
            if prices[i] > ref:
                ref = prices[i]
                i_ref = i 
        else:
            if r <= -1 * th_down:
                status[i] = dc_end_down 
                ref = prices[i]
                i_ref = i
                continue
            if prices[i] < ref:
                ref = prices[i]
                i_ref = i
                
    df = pd.DataFrame({'Time': time, 'Price': prices, 'Status': status, 'ror': ror})
    return df
    
    
    
    
    
    
    

       
def save(path, time, prices):
    tlist = []
    for t in time:
        tlist.append(t.strftime('%Y/%m/%d %H:%M:%S.%f'))
    df = pd.DataFrame({'Time': tlist, 'Price': prices})
    df.to_excel(path, index=True)
        
def test():
    with open('./data/TICK/GBPJPY_2023.pkl', 'rb') as f:
        ticks = pickle.load(f)
    print('Load size:', len(ticks[Const.TIME]))

    time = ticks[Const.TIME]
    prices = ticks[Const.PRICE]
    n = 30000
    time = time[-n:]
    prices = prices[-n:]
    df = validation(time, prices, 0.05, 0.05)
    df.to_excel('./gbpjpy.xlsx', index=False)
    
    m = n #2000
    buffer = DataBuffer(time[:m], prices[:m])
    trade_rule = AlternateTrade(param_long(), param_short())
    loop = Handling(trade_rule)
    events, positions = loop.back_test(buffer)
    disp(positions)
    plot_events(events, time, prices)

if __name__ == '__main__':
    test()