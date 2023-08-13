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
    try:
        TMV = abs(os_event.price[1] - dc_event.price[0]) / dc_event.price[0] / dc_event.threshold_percent * 100.0
    except:
        return (None, None, None)
    t = os_event.term[1] - dc_event.term[0]
    if time_unit == TimeUnit.DAY:        
         T = t.total_seconds() / 60 / 60 / 24
    elif time_unit == TimeUnit.HOUR:
        T = t.total_seconds() / 60 / 60
    elif time_unit == TimeUnit.MINUTE:
        T = t.total_seconds() / 60
    elif time_unit == TimeUnit.SECOND:
        T = t.total_seconds()
    #R = abs(os_event.price[1] - dc_event.price[0]) / dc_event.price[0] / T
    R = TMV / T
    return (TMV, T, R)

def coastline(events, time_unit: TimeUnit):
    s = 0.0
    for dc_event, os_event in events:
        tmv, t, r = indicators(dc_event, os_event, time_unit)
        if tmv is None:
            break
        s += tmv
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
        
    def description(self):
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
    
    def detect_first_dc(self, event: Event, time: array, prices: array, scan_begin: int, th_up_percent: float, th_down_percent: float):
        n = len(prices)
        if event is None:
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
        last_dc_event = event_pair[0]
        last_os_event = event_pair[1]
        if last_os_event is None:
            last_dc_event.description()
            idx = last_dc_event.index[1] + 1
            last_os_event = Event(idx, time[idx], prices[idx])
            last_os_event.set_refferene(idx, prices[idx])
        for i in range(scan_begin, n):
            delta = (prices[i] / last_os_event.refference_price - 1.0) * 100.0
            if last_dc_event.direction == Direction.Up:
                if delta <= -1 * th_down_percent:
                    idx = last_os_event.i_refference
                    last_os_event.set_end(idx, time[idx], prices[idx], th_up_percent)
                    pairs = [last_dc_event, last_os_event]
                    dc_event = Event(idx, time[idx], prices[idx])
                    dc_event.set_refferene(i + 1, prices[i + 1])
                    dc_event.set_end(i + 1, time[i + 1], prices[i + 1], th_down_percent)
                    return (pairs, dc_event, i + 1)
                if prices[i] > last_os_event.refference_price:
                    last_os_event.refference_price = prices[i]
                    last_os_event.i_refference = i
            else:
                if delta >= th_up_percent:
                    idx = last_os_event.i_refference
                    last_os_event.set_end(idx, time[idx], prices[idx], th_down_percent)
                    pairs = [last_dc_event, last_os_event]
                    dc_event = Event(idx, time[idx], prices[idx])
                    dc_event.set_refferene(i + 1, prices[i + 1])
                    dc_event.set_end(i + 1, time[i + 1], prices[i + 1], th_up_percent)
                    return (pairs, dc_event, i + 1)
                if prices[i] < last_os_event.refference_price:
                    last_os_event.refference_price = prices[i]
                    last_os_event.i_refference = i
        pairs = [last_dc_event, last_os_event]
        return (pairs, None, n)
    
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
                
    def detect_events(self, begin, last_dc_event: Event, time, prices, th_up_percent: float, th_down_percent: float):
        events = []
        found, dc_event, index = self.detect_first_dc(last_dc_event, time, prices, begin, th_up_percent, th_down_percent)
        if not found:
            return (events, index)
        begin = index
        last_dc_event = dc_event
        while True:
            (pairs, dc_event, index) = self.detect_next_dc([last_dc_event, None], time, prices, begin, th_up_percent, th_down_percent)
            if pairs[1] is None:
                events.append(pairs)
                return (events, index)
            events.append(pairs)
            if dc_event is None:
                return (events, index)
            last_dc_event = dc_event
            #dc_event.description()
            begin = index
        return (events, index)
        