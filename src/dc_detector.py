# -*- coding: utf-8 -*-
"""
Created on Sat Jul 29 08:31:27 2023

@author: Trader-IKU
"""
import polars as pl
import numpy as np
from numpy import array
import copy


class Direction: 
    Up = 'up'
    Down = 'down'

class TimeUnit:
    DAY = 'day'
    HOUR = 'hour'
    MINUT = 'minute'
    SECOND = 'second' 
    
class EventStatus:
    Nothing = 0
    DC_first = 1
    DC = 2
    OS = 3

def indicators(dc_event, os_event, time_unit: TimeUnit):
    def interval(t0, t1):
        t = t1 - t0
        if time_unit == TimeUnit.DAY:        
             T = t.total_seconds() / 60 / 60 / 24
        elif time_unit == TimeUnit.HOUR:
            T = t.total_seconds() / 60 / 60
        elif time_unit == TimeUnit.MINUTE:
            T = t.total_seconds() / 60
        elif time_unit == TimeUnit.SECOND:
            T = t.total_seconds()
        return T
    
    T = interval(dc_event.term[0], os_event.term[1])
    Tdc = interval(dc_event.term[0], dc_event.term[1])
    Tos = interval(os_event.term[0], os_event.term[1])
    try:
        TMV = abs(os_event.price[1] - dc_event.price[0]) / dc_event.price[0] / dc_event.threshold_percent * 100.0
        kT = Tos / Tdc
        kPrice = (os_event.price[1] - os_event.price[0]) / (dc_event.price[1] - dc_event.price[0])
    except:
        return (None, None, None, None, None, None, None)
    
    #R = abs(os_event.price[1] - dc_event.price[0]) / dc_event.price[0] / T
    R = TMV / T
    return (TMV, R, T, kT, kPrice, Tdc, Tos)

def coastline(events, time_unit: TimeUnit):
    s = 0.0
    for dc_event, os_event in events:
        (TMV, R, T, kT, kPrice, Tdc, Tos) = indicators(dc_event, os_event, time_unit)
        if TMV is None:
            break
        s += TMV
    return s
# ----

class Event:
    def __init__(self, i_begin, time_begin, price_begin):
        self.index = [i_begin]
        self.term = [time_begin]
        self.price = [price_begin]
        self.delta = None
        self.direction = None
        self.threshold_percent = None
        self.i_refference = None
        self.refference_price = None
        self.is_valid = False
        
    def set_refferene(self, i_refference: int, refference_price: float):
        self.i_refference = i_refference
        self.refference_price = refference_price
        
    def set_end(self, i_end, time_end, price_end, threshold_percent):
        self.index.append(i_end)
        self.term.append(time_end)
        self.price.append(price_end)
        self.delta = (self.price[1] / self.price[0] - 1.0) * 100.0
        if self.delta >= 0:
            self.direction = Direction.Up
        else:
            self.direction = Direction.Down
        self.threshold_percent = threshold_percent
        self.is_valid = True

    def valid(self):
        return self.is_valid
        
    def desc(self):
        print('index: ', self.index)
        print('term: ', self.term)
        print('price: ', self.price)
        print('delta: ', self.delta)
        print('direction: ', self.direction)
        print('threshold: ', self.threshold_percent)
        print('refference: ', self.i_refference, self.refference_price)
# -----

class DCDetector:
    def __init__(self, time: array, prices: array):
        self.time = time
        self.prices = prices
    
    def detect_first_dc(self, time: array, prices: array, scan_begin: int, th_up_percent: float, th_down_percent: float):
        n = len(prices)
        event = Event(0, time[0], prices[0])
        event.set_refferene(0, prices[0])
        for i in range(scan_begin, n):
            delta = (prices[i] / event.refference_price - 1.0) * 100.0
            if delta >= th_up_percent:
                event.set_end(i, time[i], prices[i], th_up_percent)
                return (True, event, i)
            if delta < -1 * th_down_percent:
                event.set_end(i, time[i], prices[i], th_down_percent)
                return (True, event, i)
        return (False, event, (n - 1))
    
    def detect_next_dc(self, event_pair, time: array, prices: array, scan_begin: int, th_up_percent: float, th_down_percent: float):
        n = len(prices)
        dc_event = event_pair[0]
        os_event = event_pair[1]
        if os_event is None:
            #dc_event.desc()
            idx = dc_event.index[1]
            if idx >= n:
                print('Error')
                exit()
            os_event = Event(idx, time[idx], prices[idx])
            os_event.set_refferene(idx, prices[idx])
        pairs = []
        for i in range(scan_begin, n):
            delta = (prices[i] / os_event.refference_price - 1.0) * 100.0
            if dc_event.direction == Direction.Up:
                if delta <= -1 * th_down_percent:
                    idx = os_event.i_refference
                    os_event.set_end(idx, time[idx], prices[idx], th_up_percent)
                    pair = [dc_event, os_event]
                    dc_event = Event(idx, time[idx], prices[idx])
                    dc_event.set_refferene(i, prices[i])
                    dc_event.set_end(i, time[i], prices[i], th_down_percent)
                    pairs.append(pair)
                    pairs.append([dc_event, None])
                    return (True, pairs, i + 1)
                if prices[i] > os_event.refference_price:
                    os_event.refference_price = prices[i]
                    os_event.i_refference = i
            else:
                if delta >= th_up_percent:
                    idx = os_event.i_refference
                    os_event.set_end(idx, time[idx], prices[idx], th_down_percent)
                    pair = [dc_event, os_event]
                    dc_event = Event(idx, time[idx], prices[idx])
                    dc_event.set_refferene(i, prices[i])
                    dc_event.set_end(i, time[i], prices[i], th_up_percent)
                    pairs.append(pair)
                    pairs.append([dc_event, None])
                    return (True, pairs, i + 1)
                if prices[i] < os_event.refference_price:
                    os_event.refference_price = prices[i]
                    os_event.i_refference = i
        pairs = [[dc_event, os_event]]
        return (False, pairs, n)
    
    def search_max_point(self, data: array, begin: int, min_limit: float):
        max_value = -1
        max_i = -1
        for i in range(begin, len(data)):
            if data[i] > max_value:
                max_value = data[i]
                max_i = i
            if data[i] <= min_limit:
                break
        return (max_i, max_value)
        
    def search_min_point(self, data: array, begin: int, max_limit: float):
        min_value = max_limit
        min_i = -1
        for i in range(begin, len(data)):
            if data[i] < min_value:
                min_value = data[i]
                min_i = i
            if data[i] >= max_limit:
                break
        return (min_i, min_value)
    
    def make_status(self, length: int, events):
        s = np.full(length, np.nan)
        for dc_event, os_event in events:
            if dc_event.direction == Direction.Up:
                s[dc_event.index[0]] = EventStatus.DC_up_begin 
                for i in range(dc_event.index[0] + 1, dc_event.index[1]):
                    s[i] = EventStatus.DC_up
                s[dc_event.index[1]] = EventStatus.DC_up_end
            else:
                s[os_event.index[0]] = EventStatus.OS_down_begin 
                for i in range(os_event.index[0] + 1, os_event.index[1]):
                    s[i] = EventStatus.DC_down
                s[os_event.index[1]] = EventStatus.OS_up_end
        return s
                
    def detect_events(self, begin, events, pair, time, prices, th_up_percent: float, th_down_percent: float):
        if len(events) == 0 and pair is None:
            found, dc_event, index = self.detect_first_dc(time, prices, begin, th_up_percent, th_down_percent)
            if found:
                pair = [dc_event, None]
                begin = index
            else:
                return (0, None, index)
        if events is None:
            events = []
        count = 0
        while True:
            if pair is None:
                last_pair = events[-1]
            else:
                last_pair = pair                
            (found, pairs, index) = self.detect_next_dc(last_pair, time, prices, begin, th_up_percent, th_down_percent)            
            if found:
                events.append(pairs[0])
                pair = pairs[1]
                begin = index
                count += 1
            else:
                pair = pairs[0]   
                return (count, pair, index)
        
    def run(self, time, prices, th_up_percent, th_down_percent):
        self.begin = 0
        self.events = []
        self.pair = None
        self.th_up_percent = th_up_percent
        self.th_down_percent = th_down_percent
        (count, pair, index) = self.detect_events(self.begin, self.events, self.pair, time, prices, self.th_up_percent, self.th_down_percent)
        self.pair = pair
        self.begin = index
        return count
    
    def update(self, time, prices):
        (count, pair, index) = self.detect_events(self.begin, self.events, self.pair, time, prices, self.th_up_percent, self.th_down_percent)
        self.pair = pair
        self.begin = index
        return count   
    
    