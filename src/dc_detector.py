# -*- coding: utf-8 -*-
"""
Created on Sat Jul 29 08:31:27 2023

@author: docs9
"""
import polars as pl
import numpy as np
from numpy import array


class TimeUnit:
    DAY = 'day'
    HOUR = 'hour'
    MINUT = 'minute'
    SECOND = 'second' 

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

class Event:
    def __init__(self, i_begin, i_end, time_begin, time_end, price_begin, price_end, threshold_percent):
        self.index = [i_begin, i_end]
        self.term = [time_begin, time_end]
        self.price = [price_begin, price_end]
        self.delta = (price_end / price_begin - 1.0) * 100.0
        self.upward = (self.delta >= 0)
        self.downward = (self.delta < 0)
        self.threshold_percent = threshold_percent


class DCDetector:
    def __init__(self, time: array, prices: array):
        self.time = time
        self.prices = prices
        
    def detect_os(self, time: array, data: array, begin: int, th_up_percent: float, th_down_percent: float, is_upward):
        ref = data[begin]
        i_peak = begin
        n = len(data)
        for i in range(begin + 1, n):
            delta = (data[i] / ref - 1.0) * 100.0
            event = Event(begin, i_peak, time[begin], time[i_peak], data[begin], data[i_peak], 0.0)     
            if is_upward:
                if delta <= -1.0 *  th_down_percent:
                    event.threshold_percent = th_down_percent
                    return event
                if data[i] > ref:
                    ref = data[i]
                    i_peak = i
            else:
                if delta >= th_up_percent:
                    event.threshold_percent = th_up_percent
                    return event
                if data[i] < ref:
                    ref = data[i]
                    i_peak = i
        return None
    
    def detect_dc(self, time: array, data: array, begin: int, th_up_percent: float, th_down_percent: float, upward=None):
        ref = data[begin]
        n = len(data)
        for i in range(begin + 1, n):
            delta = (data[i] / ref - 1.0) * 100.0
            event = Event(begin, i, time[begin], time[i], ref, data[i], 0)     
            if upward is None:
                # detect DC event
                if delta >= th_up_percent:
                    event.threshold_percent = th_up_percent
                    return event
                if delta < -1 * th_down_percent:
                    event.threshold_percent = th_down_percent
                    return event
            else:
                if upward:
                    if delta >= th_up_percent:
                        event.threshold_percent = th_up_percent
                        return event
                else:
                    if delta <= -1 * th_down_percent:
                        event.threshold_percent = th_down_percent
                        return event
        return None
        
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
    
    def detect_events(self, th_up_percent, th_down_percent):
        time = self.time.copy()
        prices = self.prices.copy()
        events = []
        begin = 0        
        while True:
            dc_event = self.detect_dc(time, prices, begin, th_up_percent, th_down_percent)
            if dc_event is None:
                return events
            begin = dc_event.index[1]
            direction = dc_event.upward
            os_event = self.detect_os(time, prices, begin, th_up_percent, th_down_percent, direction)
            events.append([dc_event, os_event])
            if os_event is None:
                return events
            begin = os_event.index[1]
        return events        
        