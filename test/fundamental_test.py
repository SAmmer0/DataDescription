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
            (SELECT EndDate, InfoPublDate, {data}, CompanyCode, BulletinType, IfMerged, ROW_NUMBER()
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}
            WHERE
            BulletinType != 10 AND
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            IfMerged = 1
            ) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
               ORDER BY S.EndDate ASC'''.format(data=data_name, symbol=symbol, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    # pdb.set_trace()
    if len(data) <= 4 or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        if len(data) == 4:
            if data['rpt_date'].iloc[0].month != 3:
                return np.nan
        else:
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
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}
            WHERE
            BulletinType != 10 AND
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            IfMerged = 1
            ) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
               ORDER BY S.EndDate ASC
    '''.format(symbol=symbol, data=data_name, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    if len(data) < offset or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
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
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            IfAdjusted NOT IN (4, 5) AND
            IfMerged = 1) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
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

def bs_shift_db_year(symbol, date, offset, data_name, tab_name='LC_BalanceSheetAll'):
    # 按年度进行偏移的资产负债表数据
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
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}
            WHERE
            BulletinType != 10 AND
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            IfMerged = 1
            ) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
               ORDER BY S.EndDate ASC
    '''.format(symbol=symbol, data=data_name, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    if len(data) < 1:   # 没有发不过年报
        return np.nan
    data = data.loc[data.rpt_date.dt.month == 12]
    if len(data) < offset or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        return np.nan
    # pdb.set_trace()
    return data.iloc[-offset, 1]


def cshift_year_db(symbol, date, data_name, tab_name, offset):
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
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            IfAdjusted NOT IN (4, 5) AND
            IfMerged = 1) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
    ORDER BY S.EndDate ASC'''.format(data=data_name, symbol=symbol, date=date, tab_name=tab_name)
    data = fetch_db_data(jydb, sql, ['symbol', 'data', 'rpt_date', 'update_time'], {'data': 'float64'})
    # pdb.set_trace()
    if len(data) < 1:
        return np.nan
    data = data.loc[data.rpt_date.dt.month==12]
    if len(data) < offset or get_calendar('stock.sse').count(data.iloc[-1, 3], date) > 120:
        return np.nan
    return data.iloc[-offset, 1]

def xps_shift_year(symbol, date, data_name, tab_name, offset):
    # 计算年度偏移后的XPS指标
    symbol = drop_suffix(symbol)
    date = trans_date(date)
    if tab_name == 'LC_BalanceSheetAll':
        if_adjusted = ''
    else:
        if_adjusted = 'IfAdjusted NOT IN (4, 5) AND'
    sql = '''
    SELECT m.SecuCode, s.TotalShares, s.{data}, s.enddate, s.infopubldate
    FROM
	    (select s1.EndDate, s1.InfoPublDate, s1.{data}, s1.CompanyCode, s1.rnum, s2.TotalShares from
            (SELECT EndDate, InfoPublDate, {data}, CompanyCode, BulletinType, IfAdjusted, IfMerged, ROW_NUMBER()
            OVER(PARTITION BY COMPANYCODE, ENDDATE ORDER BY INFOPUBLDATE DESC) as rnum FROM {tab_name}
            WHERE
            BulletinType != 10 AND
            InfoPublDate < \'{date:%Y-%m-%d}\' AND
            {if_adjusted}
            IfMerged = 1) s1 left outer join LC_ShareStru s2 on (s1.CompanyCode = s2.CompanyCode AND s1.EndDate = s2.EndDate)) s, SecuMain M
    where
            s.rnum = 1 AND
            S.CompanyCode = M.CompanyCode AND
            M.SecuCode = \'{symbol}\' AND
            M.SecuCategory = 1 AND
            M.SecuMarket IN (83, 90) AND
            S.EndDate >= (SELECT TOP(1) S3.CHANGEDATE
                      FROM LC_ListStatus S3
                      WHERE
                          S3.INNERCODE = M.INNERCODE AND
                          S3.ChangeType = 1)
    ORDER BY S.EndDate ASC
    '''.format(data=data_name, tab_name=tab_name, date=date, symbol=symbol, if_adjusted=if_adjusted)
    data = fetch_db_data(jydb, sql, ['symbol', 'total_share', 'data', 'rpt_date', 'update_time'], {'data': 'float64', 'total_share': 'float64'})
    data['total_share'] = data.total_share.fillna(method='ffill')
    if len(data) < 1:
        return np.nan
    data = data.loc[data.rpt_date.dt.month == 12]
    if len(data) < offset or get_calendar('stock.sse').count(data.iloc[-1, -1], date) > 120:
        return np.nan
    return data.iloc[-offset, 2] / data.iloc[-offset, 1]

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

def test_TA(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    sample = get_sample(date, 500)
    # sample = ['300167.SZ']
    sql_result = pd.Series({symbol: bs_shift_db(symbol, date, 'TotalAssets', 1)
                            for symbol in sample}).fillna(-1000)
    db_result = query('TA', date).reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')

def test_FIEXP_TTM(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    last_date = get_calendar('stock.sse').shift_tradingdays(date, -3)
    sample = get_sample(date, 1000)
    # sample = ['300167.SZ']
    sql_result = pd.Series({symbol: ttm_db(symbol, date, 'FinancialExpense', 'LC_IncomeStatementAll')
                            for symbol in sample}).fillna(-1000)
    db_result = query('FIEXP_TTM', date).reindex(sql_result.index).fillna(-1000)
    # db_result = data_simu_calculation('FIEXP_TTM', last_date, date).iloc[-1].reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')

def test_TA2Y(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    last_date = get_calendar('stock.sse').shift_tradingdays(date, -3)
    sample = get_sample(date, 500)
    # sample = ['300167.SZ']
    sql_result = pd.Series({symbol: bs_shift_db_year(symbol, date, 2, 'TotalAssets')
                            for symbol in sample}).fillna(-1000)
    db_result = query('TA2Y', date).reindex(sql_result.index).fillna(-1000)
    # db_result = data_simu_calculation('FIEXP_TTM', last_date, date).iloc[-1].reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')

def test_NI3Y(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    last_date = get_calendar('stock.sse').shift_tradingdays(date, -3)
    sample = get_sample(date, 500)
    # sample = ['300167.SZ']
    sql_result = pd.Series({symbol: cshift_year_db(symbol, date, 'NPParentCompanyOwners', 'LC_IncomeStatementAll', 3)
                            for symbol in sample}).fillna(-1000)
    db_result = query('NI3Y', date).reindex(sql_result.index).fillna(-1000)
    # db_result = data_simu_calculation('FIEXP_TTM', last_date, date).iloc[-1].reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')


def test_EPS1Y(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    last_date = get_calendar('stock.sse').shift_tradingdays(date, -3)
    sample = get_sample(date, 500)
    # sample = ['600571.SH']
    sql_result = pd.Series({symbol: xps_shift_year(symbol, date, 'NPParentCompanyOwners', 'LC_IncomeStatementAll', 1)
                            for symbol in sample}).fillna(-1000)
    db_result = query('EPS1Y', date).reindex(sql_result.index).fillna(-1000)
    # db_result = data_simu_calculation('FIEXP_TTM', last_date, date).iloc[-1].reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')


def test_SPS3Y(date):
    if not get_calendar('stock.sse').is_tradingday(date):
        date = get_calendar('stock.sse').shift_tradingdays(date, -1)
    last_date = get_calendar('stock.sse').shift_tradingdays(date, -3)
    sample = get_sample(date, 500)
    # sample = ['600571.SH']
    sql_result = pd.Series({symbol: xps_shift_year(symbol, date, 'TotalOperatingRevenue', 'LC_IncomeStatementAll', 3)
                            for symbol in sample}).fillna(-1000)
    db_result = query('SPS3Y', date).reindex(sql_result.index).fillna(-1000)
    # db_result = data_simu_calculation('FIEXP_TTM', last_date, date).iloc[-1].reindex(sql_result.index).fillna(-1000)
    if not np.all(np.isclose(sql_result, db_result)):
        diff = ~np.isclose(sql_result, db_result)
        print(sql_result[diff])
        print(db_result[diff])
        raise "Error in TA test!"
    print('All test passed!')

if __name__ == '__main__':
    test_SPS3Y('2018-07-04')
