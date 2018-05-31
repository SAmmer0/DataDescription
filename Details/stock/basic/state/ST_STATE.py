#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-04 15:51:54
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
from data_checkers import check_completeness, drop_delist_data

@drop_delist_data
def get_st_status(start_time, end_time):
    '''
    获取股票特殊处理的情况
    '''
    sql = '''
    SELECT S.SpecialTradeTime, S.SecurityAbbr, C.MS, M.SecuCode
    FROM LC_SpecialTrade S, SecuMain M, CT_SystemConst C
    WHERE
        S.InnerCode = M.InnerCode AND
        M.SecuMarket in (83, 90) AND
        S.SpecialTradeType = C.DM AND
        C.LB = 1185 AND
        M.SecuCategory = 1
    '''
    data = fetch_db_data(jydb, sql, ['time', 'abbr', 'ms', 'symbol'])

    def _assign_st(row):
        map_dict = {'ST': 1., 'PT': 5., '撤销ST': 0., '*ST': 2., '撤消*ST并实行ST': 1.,
                    '从ST变为*ST': 2., '撤销*ST': 0., '退市整理期': 3., '高风险警示': 4.}
        if row.ms in map_dict:
            return map_dict[row.ms]
        else:
            assert row.ms == '撤销PT', "Error, cannot handle tag '{tag}'".format(tag=row.ms)
            if 'ST' in row.abbr:
                return 1
            elif '*ST' in row.abbr:
                return 2
            else:
                return 0
    data = data.assign(tag=lambda x: x.apply(_assign_st, axis=1))
    data['symbol'] = data.symbol.apply(add_stock_suffix)
    # 剔除日期重复项，因为数字越大表示越风险越高，因而只保留数字大的
    data = data.sort_values(['symbol', 'time', 'tag'])
    by_snt = data.groupby(['symbol', 'time'])
    data = by_snt.tail(1)
    data = data.reset_index(drop=True)
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    by_symbol = data.groupby('symbol')
    data = by_symbol.apply(map2td, days=tds, timecol='time',
                           fillna={'symbol': lambda x: x.symbol.iloc[0]})
    data = data.pivot_table('tag', index='time', columns='symbol').dropna(axis=0, how='all')
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    data = data.reindex(columns=universe).fillna(0)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Error, data missed!')
    return data


dd = DataDescription(get_st_status, trans_date('2018-05-04'), DataType.PANEL_NUMERIC,
                     dep=['UNIVERSE', 'LIST_STATE'],
                     desc='股票ST状态：0表示正常，1表示ST，2表示*ST，3表示退市整理，4表示高风险警示，5表示特殊转让')
