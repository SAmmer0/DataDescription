#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-22 14:13
"""
import pdb

from tdtools import trans_date, get_calendar
from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.jydb import (process_fundamental_data,
                                       calc_tnm, 
                                       calc_offsetdata,
                                       calc_seasonly_data)
from datasource.sqlserver.utils import fetch_db_data
from datautils.toolkits import add_stock_suffix
from pitdata import pitcache_getter

from data_checkers import drop_delist_data, check_completeness

def ttm_factory(data_name_sql, table_name):
    """
    母函数，用于生成计算利润表或者现金流量表TTM数据的函数

    Parameter
    ---------
    data_name_sql: string
        聚源数据库中数据的名称
    table_name: string
        数据表的名称，仅支持[IS, CFS]

    Return
    ------
    func: function(start_time, end_time)
    """
    table_map = {'IS': 'LC_IncomeStatementAll', 'CFS': 'LC_CashFlowStatementAll'}
    sql = '''
    SELECT S.InfoPublDate, S.EndDate, M.SecuCode, S.{data}
    FROM SecuMain M, {table_name_sql} S
    WHERE M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.BulletinType != 10 AND
        S.IfAdjusted not in (4, 5) AND
        S.IfMerged = 1 AND
        S.EndDate >= \'{start_time: %Y-%m-%d}\' AND
        S.InfoPublDate <= \'{end_time: %Y-%m-%d}\' AND
        S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
    ORDER BY M.SecuCode ASC, S.InfoPublDate ASC
    '''
    @drop_delist_data
    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shift = get_calendar('stock.sse').shift_tradingdays(start_time, -450)
        cur_sql = sql.format(data=data_name_sql, sql_type=table_map[table_name],
                             start_time=start_time_shift, end_time=end_time)
        raw_data = fetch_db_data(jydb, cur_sql, ['update_date', 'rpt_date', 'symbol', 'data'], 
                                {'data': 'float64'})
        raw_data = calc_seasonly_data(raw_data, ['symbol', 'data', 'udpate_date', 'rpt_date'])
        raw_data.symbol = raw_data.symbol.apply(add_stock_suffix)
        # pdb.set_trace()
        data = process_fundamental_data(raw_data, ['symbol', 'data', 'update_date', 'rpt_date'],
                                        start_time, end_time, 6, calc_tnm, 
                                        data_col='data', period_flag_col='rpt_date')
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner

def shift_factory(table_name, data_name_sql, offset, freq):
    """
    母函数，用于生成计算给定报表的特定季度、半年度或者年度数据的函数

    Parameter
    ---------
    table_name: string
        报表名称，当前仅支持[BS, CFSY, ISY]，分别对应资产负债表，利润表，现金流量表
    data_name_sql: string
        聚源数据库中数据的名称，需查询对应的数据库字典
    offset: int
        往前推的期数，offset=1表示计算最近一期的数据，offset=2表示计算次近一期的数据，
        以此类推
    freq: int
        数据推移的频率，仅支持[1, 2, 4]分别表示为季度、半年度和年度
        其中，2和4仅支持资产负债表

    Return
    ------
    func: function(start_time, end_time)
    """
    table_map = {'BS': 'LC_BalanceSheetAll', 'ISY': 'LC_IncomeStatementAll', 
                 'CFSY': 'LC_CashFlowStatementAll'}
    sql = '''
    SELECT S.InfoPublDate, S.EndDate, M.SecuCode, S.{data}
    FROM SecuMain M, {table_name_sql} S
    WHERE M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.BulletinType != 10 AND
        {if_adjusted}
        S.IfMerged = 1 AND
        S.EndDate >= \'{start_time: %Y-%m-%d}\' AND
        S.InfoPublDate <= \'{end_time: %Y-%m-%d}\' AND
        S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
    ORDER BY M.SecuCode ASC, S.InfoPublDate ASC
    '''
    if table_name in ['CFSY', 'ISY']:
        if_adjusted = 'S.IfAdjusted not in (4, 5) AND'
    else:
        if_adjusted = ''
    if offset < 1 or not isinstance(offset, int):
        raise ValueError('Offset parameter must be interger and larger than 1!')
    @drop_delist_data
    def inner(start_time, end_time):
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shifted = get_calendar('stock.sse').\
                             shift_tradingdays(start_time, -(66 * (offset + 1) * freq))
        cur_sql = sql.format(data=data_name_sql, table_name_sql=table_map[table_name], 
                             start_time=start_time_shifted, end_time=end_time, 
                             if_adjusted=if_adjusted)
        raw_data = fetch_db_data(jydb, cur_sql, ['update_date', 'rpt_date', 'symbol', 'data'], 
                                {'data': 'float64'})
        if freq == 1 and table_name in ['ISY', 'CFSY']:  # 季度数据推移
            raw_data = calc_seasonly_data(raw_data, ['symbol', 'data', 'update_date', 'rpt_date'])
        raw_data.symbol = raw_data.symbol.apply(add_stock_suffix)
        data = process_fundamental_data(raw_data, ['symbol', 'data', 'update_date', 'rpt_date'], 
                                        start_time, end_time, offset*freq+3, calc_offsetdata, 
                                        data_col='data', period_flag_col='rpt_date', 
                                        offset=offset, multiple=freq)
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner
