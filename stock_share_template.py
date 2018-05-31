#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-10 14:26:49
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

# 获取股本数量的函数模板
import pdb

from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import jydb, map2td
from datautils.toolkits import add_stock_suffix
from tdtools import get_calendar
from pitdata import pitcache_getter
from qrtconst import NaS
from data_checkers import check_completeness, drop_delist_data


def get_shares(share_type):
    '''
    母函数，用于生成获取给定类型股本数据的函数

    Parameter
    ---------
    share_type: string
        具体可选类型见聚源数据库字典LC_ShareStru

    Return
    ------
    func: function(start_time, end_time)
    '''
    @drop_delist_data
    def inner(start_time, end_time):
        sql = '''
            SELECT S.{share_type}, S.EndDate, M.SecuCode
            FROM SecuMain M, LC_ShareStru S
            WHERE M.CompanyCode = S.CompanyCode AND
                M.SecuMarket in (83, 90) AND
                M.SecuCategory = 1 AND
                M.ListedState != 9
                ORDER BY M.SecuCode ASC, S.EndDate ASC
            '''.format(share_type=share_type)
        data = fetch_db_data(jydb, sql, ['data', 'time', 'symbol'], dtypes={'data': 'float64'})
        data.symbol = data.symbol.apply(add_stock_suffix)
        data = data.drop_duplicates(['symbol', 'time'])     # 若证券代码和时间相同则认为是相同数据，一般不会出现股本数据更正
        by_symbol = data.groupby('symbol')
        tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
        data = by_symbol.apply(map2td, days=tds, timecol='time',
                               fillna={'symbol': lambda x: x.symbol.iloc[0]})
        data = data.pivot_table('data', index='time', columns='symbol')
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Error, data missed!')
        return data
    return inner
