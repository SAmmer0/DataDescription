#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/2
"""
from random import sample
import pdb

import numpy as np
import pandas as pd

from pitdata import data_simu_calculation, query
from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.utils import fetch_db_data
from tdtools import get_calendar, trans_date
from datautils.toolkits import drop_suffix

def get_sample(date, sample_size):
    # date: 测试数据的时间
    # sample_size: 样本量大小
    universe = query('UNIVERSE', date).index.tolist()
    sample_res = sample(universe, sample_size)
    return sample_res

def ttm_db(symbol, date, data_name, tab_name):
    # 从SQL数据库中取出ttm数据
    # symbol: 测试股票代码，带后缀
    # date: 测试数据的时间
    # data_name: 数据名称(通过聚源查询)
    # tab_name: 表名称，仅支持[LC_IncomeStatementAll, LC_CashFlowStatementAll]
    symbol = drop_suffix(symbol)
    date = trans_date(date)
    sql = '''
    SELECT m.SecuCode, s.{data}, s.enddate, s.infopubldate
    FROM
            (SELECT EndDate, InfoPublDate, {data}, CompanyCode, BulletinType, IfAdjusted, IfMerged, ROW_NUMBER()
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.BulletinType != 10 AND
            S.InfoPublDate < \'{date:%Y-%m-%d}\' AND
            S.IfAdjusted NOT IN (4, 5) AND
            S.IfMerged = 1
    ORDER BY S.EndDate ASC'''.format(data=data_name, symbol=symbol, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    # pdb.set_trace()
    if len(data) <= 5 or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        return np.nan
    raw_data = data.set_index('rpt_date').data
    seasonly_data = raw_data - raw_data.shift(1)
    seasonly_data.loc[seasonly_data.index.month==3] = np.nan
    seasonly_data = seasonly_data.fillna(raw_data)
    return seasonly_data.iloc[-4:].sum()

def bs_shift_db(symbol, date, data_name, offset, tab_name='LC_BalanceSheetAll'):
    # 计算资产负债表偏移后的数据
    # symbol: 股票代码
    # date: 测试数据时间
    # data_name: 数据名称(查询聚源)
    # offset: 数据时间偏移量，以季度为单位
    # tab_name: 报表名称，默认为资产负债表
    symbol = drop_suffix(symbol)
    date = trans_date(date)
    sql = '''
    SELECT m.SecuCode, s.{data}, s.enddate, s.infopubldate
    FROM
            (SELECT EndDate, InfoPublDate, {data}, CompanyCode, BulletinType, IfMerged, ROW_NUMBER()
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.BulletinType != 10 AND
            S.InfoPublDate < \'{date:%Y-%m-%d}\' AND
            S.IfMerged = 1
    ORDER BY S.EndDate ASC
    '''.format(symbol=symbol, data=data_name, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    if len(data) < offset + 1 or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        return np.nan
    # pdb.set_trace()
    return data.iloc[-offset, 1]

def cshift_db(symbol, date, data_name, tab_name, offset):
    # 计算流量类报表偏移后的数据
    # symbol: 股票代码
    # date: 测试数据时间
    # data_name: 数据名称(查询聚源)
    # tab_name: 报表名称(查询聚源)
    # offset: 数据时间偏移量，以季为单位
    symbol = drop_suffix(symbol)
    date = trans_date(date)
    sql = '''
    SELECT m.SecuCode, s.{data}, s.enddate, s.infopubldate
    FROM
            (SELECT EndDate, InfoPublDate, {data}, CompanyCode, BulletinType, IfAdjusted, IfMerged, ROW_NUMBER()
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}
            WHERE
            BulletinType != 10 AND
            InfoPublDate < '2018-07-04' AND
            IfAdjusted NOT IN (4, 5) AND
            IfMerged = 1) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            --S.BulletinType != 10 AND
            --S.InfoPublDate < \'{date:%Y-%m-%d}\' AND
            --S.IfAdjusted NOT IN (4, 5) AND
            --S.IfMerged = 1 AND
            S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
    ORDER BY S.EndDate ASC'''.format(data=data_name, symbol=symbol, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    # pdb.set_trace()
    if len(data) < offset or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        return np.nan
    raw_data = data.set_index('rpt_date').data
    seasonly_data = raw_data - raw_data.shift(1)
    seasonly_data.loc[seasonly_data.index.month==3] = np.nan
    seasonly_data = seasonly_data.fillna(raw_data)
    return seasonly_data.iloc[-offset]

def test_NI5S(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    sample = get_sample(date, 500)
    # sample = ['300167.SZ']
    sql_result = pd.Series({symbol: cshift_db(symbol, date, 'NPParentCompanyOwners', 'LC_IncomeStatementAll', 5)
                            for symbol in sample}).fillna(-1000)
    db_result = query('NI5S', date).reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in NI5S test!"
    print('All test passed!')

if __name__ == '__main__':
    test_NI5S('2018-07-04')
