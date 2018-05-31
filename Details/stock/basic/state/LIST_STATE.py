#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-03 13:54:23
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from sys import path as sys_path
from os.path import dirname
import pdb

from tdtools import trans_date, get_calendar
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
from datautils.toolkits import add_stock_suffix
from datasource.sqlserver.jydb import jydb, map2td
from datasource.sqlserver.utils import fetch_db_data
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from data_checkers import check_completeness


def get_liststatus(start_time, end_time):
    '''
    获取上市公司的上市状态，1表示正常上市，2表示暂停上市，3表示退市整理，4表示终止上市，NA表示非
    正常上市状态
    '''
    sql = '''
    SELECT M.Secucode, S.ChangeType, S.changeDate
    FROM SECUMAIN M, LC_ListStatus S
    WHERE M.INNERCODE = S.INNERCODE AND
        M.SecuCategory = 1 AND
        S.SecuMarket in (90, 83) AND
        S.ChangeType != 9
    ORDER BY M.SECUCODE ASC, S.changeDate ASC
    '''
    ls_data = fetch_db_data(jydb, sql, ['symbol', 'data', 'time'], dtypes={'data': 'int'})
    # 原数据库中1表示上市，2表示暂停上市，3表示恢复上市，4表示退市,6表示退市整理
    ls_map = {1: 1, 2: 2, 3: 1, 4: 4, 6: 3}
    ls_data['data'] = ls_data.data.map(ls_map)
    ls_data['symbol'] = ls_data.symbol.apply(add_stock_suffix)
    by_symbol = ls_data.groupby('symbol')
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    ls_data = by_symbol.apply(map2td, days=tds, timecol='time',
                              fillna={'symbol': lambda x: x.symbol.iloc[0]})
    ls_data = ls_data.pivot_table('data', index='time', columns='symbol')
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    ls_data = ls_data.reindex(columns=universe).sort_index(ascending=True)
    if not check_completeness(ls_data.index, start_time, end_time):
        raise ValueError('Error, data missed!')
    return ls_data


dd = DataDescription(get_liststatus, trans_date('2018-05-03'), DataType.PANEL_NUMERIC,
                     dep=['UNIVERSE'],
                     desc='上市状态数据，1表示正常上市，2表示暂停上市，3表示退市整理，4表示终止上市，NA表示非正常上市状态')
