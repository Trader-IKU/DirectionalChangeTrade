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
from time import sleep
from matplotlib import patches
import logging

from const import Const
from converter import Converter
from candle_chart import CandleChart, BandPlot, makeFig, gridFig, Colors
from dc_detector import DCDetector, Direction, indicators, TimeUnit, coastline, EventStatus

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s:%(name)s - %(message)s",
                    filename='./profit.log')
    

class TradeRuleParams:
    def __init__(self):
        self.th_percent: float = 0
        self.horizon = 0
        self.pullback_percent: float = 0
        self.close_timelimit: float = 2.0
        self.losscut: float = 0.0
        
def param_long(th_percent):
    param = TradeRuleParams()
    param.th_percent = th_percent
    param.horizon = 0
    param.pullback_percent = 0.04
    param.close_timelimit = 10
    param.losscut = 1
    return param
    
def param_short(th_percent):
    param = TradeRuleParams()
    param.th_percent = th_percent
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
    
    def detect_test(self, data: DataBuffer):
        detector = DCDetector(data.time, data.prices) 
        time = data.time
        prices = data.prices        
        n = len(prices)   
        i = 200
        t = time[:i]
        p = prices[:i]
        detector.run(t, p, self.trade.param_up.th_percent, self.trade.param_down.th_percent)
        i += 1
        while i < n:            
            t = time[: i]
            p = prices[: i]
            detector.update(t, p)
            i += 1
        return detector.events    

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
        i += 1
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
            i += 1
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
            s = '#' +str(i + 1) + '... No DC event and OS event'
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
        (TMV, R, T, kT, kPrice, Tdc, Tos) = indicators(dc_event, os_event, TimeUnit.DAY)
        s1 = "#{} kT:{:.3f} kPrice:{:.3f} ".format(i + 1, kT, kPrice)
        chart.drawText(x, y + (chart.getYlimit()[1] - chart.getYlimit()[0]) * 0.05, s1)
        s2 = '#{} TMV: {:.5f}  T:{}  kT:{} kPrice:{} index:{}-{} dc_end_time: {}'.format(i + 1, TMV, T, kT, kPrice, dc_event.index, os_event.index, dc_event.term[1])
        logging.info(s2)
        
def calc_event_indicator(events):
    out = []
    for i, evs in enumerate(events):
        dc_event, os_event = evs
        if dc_event is None:
            print('#' +str(i + 1) + '... No DC event and OS event')
            continue            
        if os_event is None:
            print('#' +str(i + 1) + '... No OS event')
            continue

        (TMV, R, T, kT, kPrice, Tdc, Tos) = indicators(dc_event, os_event, TimeUnit.DAY)    
        direction =  dc_event.direction
        out.append([i, dc_event.term[0], direction, TMV, R, T, kT, kPrice, Tdc, Tos])
    return out
        
def disp(positions):
    for position in positions:
       position.desc()
       
def validation(time, prices, th_up, th_down):
    dc_end_up = 1 
    os_end_up = 2
    dc_end_down = -1
    os_end_down = -2
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
            direction = up
            break
        elif r <= -1 * th_down :
            status[i] = dc_end_down
            i_ref = i
            begin = i + 1
            direction = down
            ref = prices[i]
            break

    for i in range(begin, n):
        refs[i] = ref
        r = (prices[i] / ref - 1) * 100 
        ror[i] = r
        if direction == up:
            if r <= -1 * th_down:
                status[i] = dc_end_down
                status[i_ref] = os_end_up
                ref = prices[i]
                i_ref = i
                direction = down
                continue 
            if prices[i] > ref:
                ref = prices[i]
                i_ref = i 
        else:
            if r >= th_up:
                status[i] = dc_end_up
                status[i_ref] = os_end_down
                ref = prices[i]
                i_ref = i
                direction = up
                continue
            if prices[i] < ref:
                ref = prices[i]
                i_ref = i
                
    df = pd.DataFrame({'Time': time, 'Price': prices, 'Refference': refs, 'Status': status, 'ror': ror})
    return df
       

def trend_follow_simulation(events, time, prices, is_long): 
    n = len(events)
    profits = 0 
    profit_rates = 0
    draw_down = 0
    for i in range(0, n - 1):
        _, os_event = events[i]
        next_dc_event, _ = events[i + 1]
        p0 = prices[os_event.index[0] + 1]
        j = next_dc_event.index[1] + 1
        if j > len(prices) - 1:
            j = len(prices) - 1
        p1 = prices[j]
        delta = p1 - p0
        if os_event.direction == Direction.Down:
            delta *= -1
        if os_event.direction == Direction.Up and is_long:
            profits += delta
        if os_event.direction == Direction.Down and is_long == False:
            profits += delta
        if profits < draw_down:
            draw_down = profits
        profit_rates += delta / p0
        logging.info('i:' + str(i) + ' profit: ' + str(delta))
    return profits, profit_rates, draw_down

def save(path, time, prices):
    tlist = []
    for t in time:
        tlist.append(t.strftime('%Y/%m/%d %H:%M:%S.%f'))
    df = pd.DataFrame({'Time': tlist, 'Price': prices})
    df.to_excel(path, index=True)
        

def detect1(data: DataBuffer, trade_rule: AlternateTrade):
    loop = Handling(trade_rule)
    events = loop.detect_test(data)
    result = calc_event_indicator(events)
    columns=['i', 'time', 'direction', 'TMV', 'R', 'T', 'kT', 'kPrice', 'Tdc', 'Tos']
    df = pd.DataFrame(data=result, columns=columns)
    return events, df

def detect2(data: DataBuffer, trade_rule: AlternateTrade):
    loop = Handling(trade_rule)
    events, positions = loop.back_test(data)
    result = calc_event_indicator(events)
    columns=['i', 'time', 'direction', 'TMV', 'R', 'T', 'kT', 'kPrice', 'Tdc', 'Tos']
    df = pd.DataFrame(data=result, columns=columns)
    return df
    
def statics(df, items, th_long, th_short):
    n = len(df)
    data = [th_long, th_short, n]
    columns = ['th_long', 'th_short', 'n']
    for item in items:
        d = df[item]
        data.append(d.mean())
        columns.append(item + '_mean')
        data.append(d.std())
        columns.append(item + '_std')    
        data.append(d.min())
        columns.append(item + '_min')
        data.append(d.max())
        columns.append(item + '_max')        
    
    out = pd.DataFrame(data=[data], columns=columns)
    return out


def draw_circle(ax, x, y, r1, r2, color='r'):
    circle = patches.Circle(xy=(x, y), radius=r1, ec='w', fc=color)
    ax.add_patch(circle)
    circle = patches.Circle(xy=(x, y), radius=r2, ec='#444444', fill=False)
    ax.add_patch(circle)
    ax.grid()
    ax.set_xlabel("Threshold_long")
    ax.set_ylabel("Threshold_short")
    
def visualize(excel_path, symbol):
    df = pd.read_excel(excel_path)
   
    limit = 0.07
    #limit = 1.2
    n = len(df)
    k0_list = [0.0001, 0.00001]
    #k0_list = [0.1, 0.01, 0.001]
    for k0 in k0_list:
        fig, ax = makeFig(1, 1, (10,10))
        ax.set_xlim(0, limit)
        ax.set_ylim(0, limit)
        ax.set_title(symbol + ': kT distribution')
        for row in range(n):
            d = df.iloc[row]
            mean = d['kT_mean']
            if mean > limit / 20 / k0:
                mean = limit / 20 / k0
            std = d['kT_std']
            draw_circle(ax, d['th_long'], d['th_short'], k0 * mean, k0 * std)
    
    fig, ax = makeFig(1, 1, (10,10))
    ax.set_xlim(0, limit)
    ax.set_ylim(0, limit)
    ax.set_title(symbol + ': kPrice distribution')
    k1 = 0.002
    #k1 = 0.02
    for row in range(n):
        d = df.iloc[row]
        mean = d['kPrice_mean']
        std = d['kPrice_std']
        draw_circle(ax, d['th_long'], d['th_short'], mean * k1, std * k1, color='g')
        
    fig, ax = makeFig(1, 1, (10,10))
    ax.set_xlim(0, limit)
    ax.set_ylim(0, limit)
    ax.set_title(symbol + ': kPrice sum')
    k2 = 0.000005
    #k2 = 0.01
    for row in range(n):
        d = df.iloc[row]
        sum = d['kPrice_mean'] * d['n']
        draw_circle(ax, d['th_long'], d['th_short'], sum * k2, 0, color='b')
    pass

def visualize_profit(excel_path, symbol):
    df = pd.read_excel(excel_path)
   
    limit = 0.12
    #limit = 1.2
    n = len(df)
    k0 = 0.001
    fig, ax = makeFig(1, 1, (10,10))
    ax.set_xlim(0, limit)
    ax.set_ylim(0, limit)
    ax.set_title(symbol + ': Profits')
    for row in range(n):
        d = df.iloc[row]
        value = d['profits'] * k0
        if value > 0:
            c = 'g'
        else:
            c = 'r'
            value *= -1
        draw_circle(ax, d['th_long'], d['th_short'], value, 0, c)


def optimize(ticks):
    time = ticks[Const.TIME]
    prices = ticks[Const.PRICE]
    n = int(len(prices) /40)
    time = time[-n:]
    prices = prices[-n:]
    buffer = DataBuffer(time, prices)
    
    #df = validation(time, prices, 0.05, 0.05)
    #df.to_excel('./gbpjpy.xlsx')
    df = None
    count = 0
    t0 = datetime.now()
    for th_long in np.arange(0.06, 0.01, -0.01):
        for th_short in np.arange(0.06, 0.01, -0.01):
            trade_rule = AlternateTrade(param_long(th_long), param_short(th_short))
            events, indicators = detect1(buffer, trade_rule)
            stat = statics(indicators, ['kT', 'kPrice'], th_long, th_short)
            if df is None:
                df = stat
            else:
                df = pd.concat([df, stat])
            count += 1
            now = datetime.now()
            print(count, 'Elapsed time:', now - t0)
            t0 = now
            sleep(10)
    df.to_excel('./indicators.xlsx', index=False)
   
    #df.to_excel('indicators.xlsx', index=False)
    
    
    #disp(positions)
    #plot_events(events, time, prices)
    
def detect_and_plot(time, prices, th_long, th_short):
    buffer = DataBuffer(time, prices)
    trade_rule = AlternateTrade(param_long(th_long), param_short(th_short))
    events, indicators = detect1(buffer, trade_rule)    
    profits, profit_rates, draw_down = trend_follow_simulation(events, time, prices, th_long > th_short)
    plot_events(events, time, prices)
    print('n: ', len(events) - 1, 'Profit: ', profits, 'Profit rates:', profit_rates)


def profit_simulation(time, prices):
    buffer = DataBuffer(time, prices)
    
    large = np.arange(0.1, 2.0, 0.1)
    small = np.arange(0.01, 0.1, 0.01)

    count = 0
    t0 = datetime.now()
    data = []

    combination = []    
    for i in range(2):
        if i == 0:
            for th_long in large:
                for th_short in small:
                    combination.append([th_long, th_short])
        else:
            for th_long in small:
                for th_short in small:
                    combination.append([th_long, th_short])
                    
    for th_long, th_short in combination:
        trade_rule = AlternateTrade(param_long(th_long), param_short(th_short))
        events, indicators = detect1(buffer, trade_rule)    
        profits, profit_rates, draw_down = trend_follow_simulation(events, time, prices, th_long > th_short)
        data.append([th_long, th_short, profits, profit_rates, draw_down])
        count += 1
        now = datetime.now()
        s = str(count) + ' Elapsed time: ' + str(now - t0) + ' th_long: ' +  str(th_long) + ' th_short: ' + str(th_short) +  ' profit: ' + str(profits)
        logging.info(s)
        print(s)
        t0 = now
        sleep(10)
        
    df = pd.DataFrame(data=data, columns=['th_long', 'th_short', 'profits', 'profit_rates', 'draw_down'])
    df.to_excel('gbpjpy_profits.xlsx', index=False)



def main():
    with open('./data/TICK/GBPJPY_2023.pkl', 'rb') as f:
        ticks = pickle.load(f)
    print('Load size:', len(ticks[Const.TIME]))
    #main()
    #detect_and_plot(ticks, 0.05, 0.05)
    time = ticks[Const.TIME]
    prices = ticks[Const.PRICE]
    n = int(len(prices) /100)
    time = time[-n:]
    prices = prices[-n:]
    profit_simulation(time, prices)
    
    


def graph():
    #visualize('./gbpjpy2023/indicators3.xlsx', 'GBPJPY')
    visualize_profit('./gbpjpy_profits.xlsx', 'GBPJPY')

def chart():
    with open('./data/TICK/GBPJPY_2023.pkl', 'rb') as f:
        ticks = pickle.load(f)
    print('Load size:', len(ticks[Const.TIME]))
    #main()
    #detect_and_plot(ticks, 0.05, 0.05)
    time = ticks[Const.TIME]
    prices = ticks[Const.PRICE]
    n = int(len(prices) /500)
    time = time[-n:]
    prices = prices[-n:]    
    detect_and_plot(time, prices, 0.04, 0.05)

if __name__ == '__main__':
    #main()
    chart()