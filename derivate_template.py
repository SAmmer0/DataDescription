#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-25 15:23
"""
import pandas as pd
import numpy as np

from pitdata import query_group, pitcache_getter
from tdtools import get_calendar, trans_date
from data_checkers import drop_delist_data, check_completeness

def divide_data_factory(denominator_func, numerator_func, denominator_kwargs=None,
                        numerator_kwargs=None):
    """
    母函数，用于生成计算相除的数据的函数

    Parameter
    ---------
    denominator_func: function
        用于获取分母部分的数据，格式为function(start_time, end_time, **denominator_kwargs)
    numerator_func: function
        用于获取分子部分的数据，格式为function(start_time, end_time, **numerator_kwargs)
    denominator_kwargs: dict, default None
        denominator_func的字典格式的参数，None表示对应函数不需要参数
    numerator_kwargs: dict, default None
        numerator_func的字典格式参数，None表示对应函数不需要参数

    Return
    ------
    func: function(start_time, end_time)
    """
    if denominator_kwargs is None:
        denominator_kwargs = {}
    if numerator_kwargs is None:
        numerator_kwargs = {}

    @drop_delist_data
    def inner(start_time, end_time):
        denominator_data = denominator_func(start_time, end_time, **denominator_kwargs)
        numerator_data = numerator_func(start_time, end_time, **numerator_kwargs)
        data = denominator_data / numerator_data
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner

def growth_rate_factory(data_name_format, period):
    """
    母函数，生成使用线性回归的方法计算增长率的函数
    计算方法为：将数值对时间做回归(时间由远及近为1到period)，将回归系数除以这段时间内的平均值的绝对值

    Parameter
    ---------
    data_name_format: string
        需要计算的数据的格式，将采用data_name_format.format(i=i)进行转换
        i为1到period的正整数
        例如，EPS{i:d}Y表示EPS年度数据
    period: int
        正整数

    Return
    ------
    function(start_time, end_time)
    """
    def calc_growth(df):
        # 假设数据按照升序排列，即由上到下依次为1到period，数字越大表示离当前时间越远
        # df格式为index为排列后的数据名称，columns为股票名称
        t = np.arange(period, 0, -1)
        df_mean = df.mean()
        df_demean = df - df_mean
        res = np.dot(t, df_demean.values) / np.dot(t, t-np.mean(t))
        res = pd.Series(res, index=df.columns)
        res = res / np.abs(df_mean)
        return res
    
    @drop_delist_data
    def inner(start_time, end_time):
        data_group = [data_name_format.format(i=i) for i in range(1, period + 1)]
        raw_data = query_group(data_group, start_time, end_time)
        by_date = raw_data.groupby(level=0)
        data = by_date.apply(calc_growth)
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner

def vol_factory(lag, ret_source='CLOSE_DRET'):
    """
    母函数，用于生成简易波动率，对于停牌的股票，如果期间没有任何交易，则为NA，
    有交易的话则首先计算有交易的时间段的波动，然后scale到给定的交易日长度

    Parameter
    ---------
    lag: int
        波动率的计算周期
    ret_source: string
        计算波动率使用的收益率

    Return
    ------
    func: function(start_time, end_time)
    """
    @drop_delist_data
    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shifted = get_calendar('stock.sse').shift_tradingdays(start_time, -(lag+1))
        ret_data = pitcache_getter(ret_source, lag+1).get_tsdata(start_time_shifted, end_time)
        data = ret_data.rolling(lag, min_periods=lag).std()
        latest_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(latest_td).index)
        data = data.reindex(columns=universe)
        data = data.loc[(data.index>=start_time) & (data.index<=end_time)]
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner