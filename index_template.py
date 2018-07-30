#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-08 16:40:43
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import map2td
from pitdata import pitcache_getter
from pitdata.const import DATA_START_DATE
from data_checkers import check_completeness
from tdtools import trans_date, get_calendar
from datautils.toolkits import add_stock_suffix


def get_index_weights(index_symbol):
    '''
    母函数，用于生成获取给定指数的成分的函数, 1表示是指数成分，NA表示不是
    '''
    sql = '''
    SELECT  M2.SecuCode, S.Weight, S.EndDate
    FROM SecuMain M, LC_IndexComponentsWeight S, SecuMain M2
    WHERE
        M.InnerCode = S.IndexCode AND
        M2.InnerCode = S.InnerCode AND
        M.SecuCode = \'{code}\' AND
        M.SecuCategory = 4 AND
        S.EndDate >= \'{start_time:%Y-%m-%d}\' AND
        S.EndDate <= \'{end_time:%Y-%m-%d}\'
        ORDER BY M2.SecuCode ASC, S.EndDate ASC
        '''

    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shift = get_calendar('stock.sse').shift_tradingdays(start_time, -180)
        nonlocal sql
        sql = sql.format(code=index_symbol, start_time=start_time_shift, end_time=end_time)
        data = fetch_db_data(jydb, sql, ['symbol', 'weight', 'time'], dtypes={'weight': 'float64'})
        data.symbol = data.symbol.apply(add_stock_suffix)
        tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
        data = data.pivot_table('weight', index='time', columns='symbol')
        data = map2td(data, tds)
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Error, data missed!')
        return data
    return inner


def get_index_quote(index_symbol, field):
    '''
    母函数，用于生成获取指数行情的函数

    Parameter
    ---------
    index_symbol: string
        指数代码
    field: string
        行情类型，支持的数据见聚源数据库文档QT_IndexQuote
    '''
    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        sql = '''
        SELECT S.{field}, S.TradingDay
        FROM QT_IndexQuote S, SecuMain M
        WHERE
            S.InnerCode = M.InnerCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 4 AND
            S.TradingDay >= \'{start_time:%Y-%m-%d}\' AND
            S.TradingDay <= \'{end_time:%Y-%m-%d}\'
            ORDER BY S.TradingDay ASC
        '''.format(symbol=index_symbol, start_time=start_time, end_time=end_time, field=field)
        data = fetch_db_data(jydb, sql, ['data', 'time'], dtypes={'data': 'float64'})
        data = data.set_index('time').data
        if (start_time != trans_date(DATA_START_DATE) and 
            not check_completeness(data.index, start_time, end_time)):
            raise ValueError('Data Missed!')
        return data
    return inner
