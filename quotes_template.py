#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-02 14:52:54
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

import pdb

from numpy import isclose as np_isclose
from numpy import nan as np_nan
from numpy import log as np_log
from numpy import power as np_power

from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import jydb
from pitdata import pitcache_getter
from pitdata.const import DATA_START_DATE
from tdtools import trans_date, get_calendar
from datautils.toolkits import add_stock_suffix
from data_checkers import check_completeness, drop_delist_data


def quotes_factory(data_name):
    '''
    母函数，用于生成获取特定行情数据的函数

    Return
    ------
    func: function(start_time, end_time)
    '''

    SQL_TEMPLATE = '''
            SELECT S.TradingDay, data_type, M.Secucode
            FROM QT_DailyQuote S, SecuMain M
            WHERE
                S.InnerCode = M.InnerCode AND
                M.SecuMarket in (83, 90) AND
                S.TradingDay <= \'{end_time:%Y-%m-%d}\' AND
                S.TradingDay >= \'{start_time:%Y-%m-%d}\' AND
                M.SecuCategory = 1
            ORDER BY S.TradingDay ASC, M.Secucode ASC
            '''
    if data_name.lower() not in ['openprice', 'highprice', 'lowprice']:
        sql = SQL_TEMPLATE.replace('data_type', 'S.' + data_name)
        cols = ['time', 'data', 'symbol']
        dtypes = {'data': 'float64'}
    else:
        sql = SQL_TEMPLATE.replace('data_type', 'S.PrevClosePrice, S.' + data_name)
        cols = ['time', 'prevclose', 'data', 'symbol']
        dtypes = {'prevclose': 'float64', 'data': 'float64'}

    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        nonlocal sql
        sql = sql.format(start_time=start_time, end_time=end_time)
        data = fetch_db_data(jydb, sql, cols, dtypes=dtypes)
        data.symbol = data.symbol.apply(add_stock_suffix)
        if len(cols) == 4:    # 当前数据为需要使用前收盘填充的数据
            data.loc[data.data == 0, 'data'] = data['prevclose']
            data = data.drop('prevclose', axis=1)
        data = data.pivot_table('data', index='time', columns='symbol')
        latest_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(latest_td).index)
        data = data.reindex(columns=universe).sort_index(ascending=True)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner


def mkv_factory(share_name, price_name):
    '''
    母函数，用于生成计算市值的函数

    Parameter
    ---------
    share_name: string
        股本数据的名称
    price_name: string
        价格数据的名称
    '''
    @drop_delist_data
    def inner(start_time, end_time):
        cache_size = 10
        share_data = pitcache_getter(share_name, cache_size).get_tsdata(start_time, end_time)
        price_data = pitcache_getter(price_name, cache_size).get_tsdata(start_time, end_time)
        data = share_data * price_data
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner


def ret_factory(freq, price_name):
    '''
    母函数，用于生成计算收益率的函数

    Parameter
    ---------
    freq: int
        计算收益率使用的数据频率
    price_name: string
        计算收益率所使用的数据的名称
    '''
    @drop_delist_data
    def inner(start_time, end_time):
        start_time_shifted = get_calendar('stock.sse').shift_tradingdays(start_time, - freq - 10)
        price_data = pitcache_getter(price_name, 50).get_tsdata(start_time_shifted, end_time)
        data = price_data.pct_change(freq)
        data = data.loc[(data.index >= start_time) & (data.index <= end_time)]
        # pdb.set_trace()
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner

def lnmkv_factory(mkvdata_name):
    """
    母函数，用于生成计算对数市值的函数

    Parameter
    ---------
    mkvdata_name: string
        市值数据名称

    Return
    ------
    func: function(start_time, end_time)
    """
    @drop_delist_data
    def inner(start_time, end_time):
        mkv_data = pitcache_getter(mkvdata_name, 50).get_tsdata(start_time, end_time)
        mask = np_isclose(mkv_data, 0, 0.01)
        data = mkv_data.where(~mask)
        data = np_log(mkv_data)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner

def nl_lnmkv_factory(lnmkvdata_name, exponent):
    """
    母函数，用于生成计算非线性对数市值的函数，此处仅用于计算高次非线性市值，而不进行
    正交化等处理

    Parameter
    ---------
    lnmkvdata_name: string
        原数据的名称
    exponent: int
        指数

    Return
    ------
    func: function(start_time, end_time)
    """
    def inner(start_time, end_time):
        data = pitcache_getter(lnmkvdata_name, 10).get_tsdata(start_time, end_time)
        data = np_power(data, exponent)
        return data
    return inner

def sto_factory(major_mul, minor_mul=1):
    """
    计算换手率数据

    Parameter
    ---------
    major_mul: int
        主乘数
    minor_mul: int, default 1
        次乘数

    Return
    ------
    func: function(start_time, end_time)

    Notes
    -----
    结果数据为过去major_mul*minor_mul个交易日(包含当日)的总换手率/major_mul
    """
    @drop_delist_data
    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        cache_size = 100
        offset = major_mul * minor_mul
        threshold = 1e-5
        start_time_shifted = get_calendar('stock.sse').\
                             shift_tradingdays(start_time, -offset - 20)
        to_data = pitcache_getter('TO_RATE', cache_size).\
                  get_tsdata(start_time_shifted, end_time)
        
        data = to_data.rolling(offset, min_periods=offset).sum().dropna(how='all')
        data[data <= threshold] = np_nan
        data = data / major_mul
        data = np_log(data)
        data = data.loc[(data.index >= start_time) & (data.index <= end_time)]
        if start_time > trans_date(DATA_START_DATE):
            if not check_completeness(data.index, start_time, end_time):
                raise ValueError('Data missed!')
        return data
    return inner
