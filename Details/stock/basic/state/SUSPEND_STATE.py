#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-26 18:12
"""

from sys import path as sys_path
from os.path import dirname
import pdb

import pandas as pd

from tdtools import trans_date, get_calendar
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
from datautils.toolkits import add_stock_suffix
from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb.utils import map2td
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from data_checkers import check_completeness, drop_delist_data

@drop_delist_data
def get_suspend_state(start_time, end_time):
    """
    获取股票停牌状态

    Notes
    -----
    其中，0表示未停牌，1表示整日停牌，2表示日内部分交易时间停牌
    """
    start_time, end_time = trans_date(start_time, end_time)
    sql = '''
    SELECT M.SecuCode, S.SuspendDate, S.ResumptionDate
    FROM LC_SuspendResumption S, SecuMain M
    WHERE
        S.InnerCode = M.InnerCode AND
        M.SecuMarket in (83, 90) AND
        (S.ResumptionDate >= \'{start_time: %Y-%m-%d}\' OR
         s.ResumptionDate = '1900-01-01') AND
        S.suspenddate <= \'{end_time: %Y-%m-%d}\' AND
        M.SecuCategory = 1
    ORDER BY M.Secucode ASC, S.SuspendDate ASC
    '''.format(start_time=start_time, end_time=end_time)
    data = fetch_db_data(jydb, sql, ['symbol', 'suspend_date', 'resumption_date'])
    data.symbol = data.symbol.apply(add_stock_suffix)
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)

    def process_per_symbol(df):
        state = {}
        for _, line in df.iterrows():
            _, start, end = line
            if start < end:     # 正常超过一个交易日的停牌
                state[start] = 1
                state[end] = 0
            elif end == pd.to_datetime('1900-01-01'):   # 停牌至计算日还未复牌
                state[start] = 1
            else:   # 日内停牌
                state[start] = 2
        state = pd.Series(state).sort_index()
        return state
    data = data.groupby('symbol').apply(process_per_symbol).unstack().T
    data = data.fillna(method='ffill')
    data = map2td(data, tds)
    latest_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(latest_td).index)
    data = data.reindex(columns=universe).fillna(0)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Data missed!')
    return data

dd = DataDescription(get_suspend_state, trans_date('2018-07-26'), DataType.PANEL_NUMERIC,
                     dep=['UNIVERSE', 'LIST_STATE'], desc='停牌状态，0：未停牌，1：整日停牌，2：日内停牌')