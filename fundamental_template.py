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

def data_fetcher_factory(sql_template, cols, db, sql_kwargs=None, dtypes=None):
    """
    母函数，用于生成从数据库中获取数据的函数

    Parameter
    ---------
    sql_template: string
        从数据库中获取数据的SQL模板，要求模板中必须有start_time和end_time两个可替代
        的参数，且这些参数均为datetime.datetime类型
    cols: iterable
        元素为string，表示对应数据列的列名
    db: datasource.sqlserver.utils.SQLConnector or the like
        数据库实例
    sql_kwargs: dict
        模板中其他的相关参数，格式为{parameter_name: parameter_value}，程序中
        采用sql_template.format来实现，使用None表示不需要提供额外参数
    dtypes: dict
        用于说明数据的类型，格式为{col_name: dtype}
    
    Return
    ------
    func: function(start_time, end_time)->pandas.DataFrame(data, columns=cols, dtypes=dtyps)
    """
    def inner(start_time, end_time):
        nonlocal sql_kwargs
        if sql_kwargs is None:
            sql_kwargs = {}
        sql = sql_template.format(start_time=start_time, end_time=end_time, **sql_kwargs)
        data = fetch_db_data(db, sql, cols, dtypes)
        return data
    return inner

def ttm_processor(raw_data, cols, start_time, end_time):
    """
    将原始的数据处理成TTM，仅支持流量类的数据，如利润表、现金流量表

    Parameter
    ---------
    raw_data: pandas.DataFrame
        待处理的原始报表数据(即季报、半年报和年报)
    cols: iterable
        数据列名，元素依次为[证券代码, 数据, 更新时间, 报告期时间]
    start_time: datetime like
        计算结果的开始时间
    end_time: datetime like
        计算结果的结束时间

    Return
    ------
    out: pandas.DataFrame
    index为时间，columns为证券代码轴
    """
    raw_data = calc_seasonly_data(raw_data, cols)
    # pdb.set_trace()
    raw_data.symbol = raw_data.symbol.apply(add_stock_suffix)
    data = process_fundamental_data(raw_data, cols, start_time, end_time, 6, calc_tnm, 
                                    data_col='data', period_flag_col='rpt_date')
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    data = data.reindex(columns=universe)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Data missed!')
    return data

def shift_processor(raw_data, cols, start_time, end_time, offset, freq, is_single_season=False):
    """
    偏移数据处理函数，即计算每个日期内可以看到的给定偏移的数据，同时支持流量数据和存量
    数据，支持的偏移频率包含[季度, 半年度, 年度]。对于存量数据，同时支持所有偏移频率；
    对于季度数据，仅支持[季度, 年度]这两种偏移频率

    Parameter
    ---------
    raw_data: pandas.DataFrame
        待处理的原始财报数据(即季报、半年报和年报)
    cols: iterable
        数据列名，元素依次为[证券代码, 数据, 更新时间, 报告期时间]
    start_time: datetime like
        计算结果开始时间
    end_time: datetime like
        计算结果结束时间
    offset: int
        往前推的期数，offset=1表示计算最近一期的数据，offset=2表示计算次近一期的数据，
        以此类推
    freq: int
        数据推移的频率，仅支持[1, 2, 4]分别表示为季度、半年度和年度
    is_single_season: boolean, default False
        是否计算季度数据，仅当freq为1是才会启用，该选项适用于流量数据进行季度偏移的情况，
        且目标是获取偏移后的单季度数据

    Return
    ------
    out: pandas.DataFrame
        index为时间，columns为证券代码轴
    """
    if freq == 1 and is_single_season:  # 计算单季度数据 
        raw_data = calc_seasonly_data(raw_data, cols)
    raw_data.symbol = raw_data.symbol.apply(add_stock_suffix)
    data = process_fundamental_data(raw_data, cols, start_time, end_time, 
                                    offset*freq+3, calc_offsetdata, 
                                    data_col=cols[1], period_flag_col=cols[3], 
                                    offset=offset, multiple=freq)
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    data = data.reindex(columns=universe)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Data missed!')
    return data

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
    SELECT M.SecuCode, S.{data}, S.InfoPublDate, S.EndDate
    FROM SecuMain M, {table_name_sql} S
    WHERE M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.BulletinType != 10 AND
        S.IfAdjusted not in (4, 5) AND
        S.IfMerged = 1 AND
        S.EndDate >= \'{start_time:%Y-%m-%d}\' AND
        S.InfoPublDate <= \'{end_time:%Y-%m-%d}\' AND
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
        cols = ['symbol', 'data', 'update_date', 'rpt_date']
        data_fetcher = data_fetcher_factory(sql, cols, jydb, 
                                            {'data': data_name_sql, 'table_name_sql': table_map[table_name]},
                                            {'data': 'float64'})
        raw_data = data_fetcher(start_time_shift, end_time)
        # pdb.set_trace()
        raw_data = raw_data.drop_duplicates(['symbol', 'update_date', 'rpt_date'])
        return ttm_processor(raw_data, cols, start_time, end_time)
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
        数据推移的频率，仅支持[1, 2, 4]，分别表示为季度、半年度和年度
        其中利润表和现金流量表仅支持[1, 4]，且当freq为1时，仅计算单季度数据
    Return
    ------
    func: function(start_time, end_time)
    """
    table_map = {'BS': 'LC_BalanceSheetAll', 'ISY': 'LC_IncomeStatementAll', 
                 'CFSY': 'LC_CashFlowStatementAll'}
    sql = '''
    SELECT M.SecuCode, S.{data}, S.InfoPublDate, S.EndDate
    FROM SecuMain M, {table_name_sql} S
    WHERE M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.BulletinType != 10 AND
        {if_adjusted}
        S.IfMerged = 1 AND
        S.EndDate >= \'{start_time:%Y-%m-%d}\' AND
        S.InfoPublDate <= \'{end_time:%Y-%m-%d}\' AND
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
        if table_name in ['ISY', 'CFSY'] and freq == 2:
            raise ValueError('Incompatible parameters(table_name={tn}, freq={f}'.\
                             format(tn=table_name, f=freq))
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shifted = get_calendar('stock.sse').\
                             shift_tradingdays(start_time, -(66 * (offset + 2) * freq))
        cols = ['symbol', 'data', 'update_date', 'rpt_date']
        data_fetcher = data_fetcher_factory(sql, cols, jydb, 
                                            {'data': data_name_sql, 'table_name_sql': table_map[table_name], 
                                            'if_adjusted': if_adjusted},
                                            {'data': 'float64'})
        raw_data = data_fetcher(start_time_shifted, end_time)
        raw_data = raw_data.drop_duplicates(['symbol', 'update_date', 'rpt_date'])
        if table_name in ['ISY', 'CFSY']:   # 当前利润表和现金流量表计算季度偏移时仅会计算单季度数据
            is_signal_season = True
        else:
            is_signal_season = False
        return shift_processor(raw_data, cols, start_time, end_time, offset, freq, is_signal_season)
    return inner

def xps_shift_factory(table_name, data_name_sql, offset, freq):
    """
    母函数，用于生成计算每股指标的函数

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
        数据推移的频率，仅支持[1, 2, 4]，分别表示为季度、半年度和年度
        其中利润表和现金流量表仅支持[1, 4]，且当freq为1时，仅计算单季度数据

    Return
    ------
    func: function(start_time, end_time)
    """
    table_map = {'BS': 'LC_BalanceSheetAll', 'ISY': 'LC_IncomeStatementAll', 
                 'CFSY': 'LC_CashFlowStatementAll'}
    sql = '''
    SELECT M.secuCode, S.{data}, S.TotalShares, S.InfoPublDate, S.EndDate
    from
    (SELECT S2.{data}, S2.EndDate, S1.TotalShares, S2.CompanyCode, S2.InfoPublDate
    FROM
        (LC_ShareStru S1 right outer join {table_name_sql} S2 
        on (S1.CompanyCode = S2.CompanyCode AND
            S1.EndDate = S2.EndDate AND 
            S2.BulletinType != 10 AND
            {if_adjusted}
            S2.IfMerged = 1
            ))) s, SecuMain M
    WHERE
        M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.EndDate >= \'{start_time:%Y-%m-%d}\' AND
        S.InfoPublDate <= \'{end_time:%Y-%m-%d}\' AND
        S.EndDate >= (SELECT TOP(1) S3.CHANGEDATE
                        FROM LC_ListStatus S3
                        WHERE
                            S3.INNERCODE = M.INNERCODE AND
                            S3.ChangeType = 1)
    ORDER BY M.SecuCode ASC, S.EndDate ASC, S.InfoPublDate ASC
    '''
    if table_name in ['CFSY', 'ISY']:
        if_adjusted = 'S2.IfAdjusted not in (4, 5) AND'
    else:
        if_adjusted = ''
    if offset < 1 or not isinstance(offset, int):
        raise ValueError('Offset parameter must be interger and larger than 1!')
    
    @drop_delist_data
    def inner(start_time, end_time):
        if table_name in ['ISY', 'CFSY'] and freq == 2:
            raise ValueError('Incompatible parameters(table_name={tn}, freq={f}'.\
                             format(tn=table_name, f=freq))
        start_time, end_time = trans_date(start_time, end_time)
        start_time_shifted = get_calendar('stock.sse').\
                             shift_tradingdays(start_time, -(66 * (offset + 2) * freq))
        cols = ['symbol', 'data', 'total_share', 'update_date', 'rpt_date']
        data_fetcher = data_fetcher_factory(sql, cols, jydb, 
                                            {'data': data_name_sql, 'table_name_sql': table_map[table_name], 
                                            'if_adjusted': if_adjusted},
                                            {'data': 'float64', 'total_share': 'float64'})
        raw_data = data_fetcher(start_time_shifted, end_time)
        raw_data['total_share'] = raw_data.groupby('symbol').total_share.fillna(method='ffill')
        raw_data['data'] = raw_data['data'] / raw_data['total_share']
        raw_data = raw_data.drop('total_share', axis=1).\
                   drop_duplicates(['symbol', 'update_date', 'rpt_date'])
        if table_name in ['ISY', 'CFSY']:   # 当前利润表和现金流量表计算季度偏移时仅会计算单季度数据
            is_signal_season = True
        else:
            is_signal_season = False
        return shift_processor(raw_data, ['symbol', 'data', 'update_date', 'rpt_date'], start_time, end_time, offset, freq, is_signal_season)
    return inner
