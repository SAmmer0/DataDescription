#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-03 11:00:45
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date, get_calendar
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
from datautils.toolkits import add_stock_suffix
from datasource.sqlserver.jydb import jydb, map2td
from datasource.sqlserver.utils import fetch_db_data
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from data_checkers import check_completeness, drop_delist_data


@drop_delist_data
def get_padjfactor(start_time, end_time):
    '''
    从数据库中获取复权因子
    '''
    sql = '''
        SELECT A.ExDiviDate, A.RatioAdjustingFactor, M.SecuCode
        FROM QT_AdjustingFactor A, SecuMain M
        WHERE
            A.InnerCode = M.InnerCode AND
            M.secuMarket in (83, 90) AND
            M.SECUCATEGORY = 1
        ORDER BY M.SecuCode ASC, A.ExDiviDate ASC
        '''
    data = fetch_db_data(jydb, sql, ['exdivdate', 'data', 'symbol'], dtypes={'data': 'float64'})
    data.symbol = data.symbol.apply(add_stock_suffix)
    by_symbol = data.groupby('symbol')
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    data = by_symbol.apply(map2td, days=tds, timecol='exdivdate',
                           fillna={'data': lambda x: 1, 'symbol': lambda x: x.symbol.iloc[0]})
    data = data.pivot_table('data', index='exdivdate', columns='symbol')
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    data = data.reindex(columns=universe).sort_index(ascending=True).fillna(1)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Data missed!')
    return data


dd = DataDescription(get_padjfactor, trans_date('2018-05-03'), DataType.PANEL_NUMERIC,
                     desc='复权因子', dep=['UNIVERSE', 'LIST_STATE'])
