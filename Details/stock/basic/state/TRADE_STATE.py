#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-04 16:48:13
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from sys import path as sys_path
from os.path import dirname
import pdb

import numpy as np
import pandas as pd

from tdtools import trans_date, get_calendar
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
from datautils.toolkits import add_stock_suffix
from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.utils import fetch_db_data
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from data_checkers import check_completeness, drop_delist_data


@drop_delist_data
def get_trade_status(start_time, end_time):
    '''
    获取股票的交易状态

    Notes
    -----
    将成交量为0或者最高价等于最低价视为不能交易，返回值为1表示正常交易，0表示不能交易，NA表示未上市而不能交易
    '''
    sql = '''
    SELECT S.TradingDay, S.TurnoverVolume, S.HighPrice, S.LowPrice, M.Secucode
    FROM QT_DailyQuote S, SecuMain M
    WHERE
        S.InnerCode = M.InnerCode AND
        M.SecuMarket in (83, 90) AND
        S.TradingDay <= \'{end_time:%Y-%m-%d}\' AND
        S.TradingDay >= \'{start_time:%Y-%m-%d}\' AND
        M.SecuCategory = 1
    ORDER BY M.Secucode ASC, S.TradingDay ASC
    '''
    start_time, end_time = trans_date(start_time, end_time)
    offset = 30
    start_time_shifted = get_calendar('stock.sse').shift_tradingdays(start_time, -offset - 10)
    sql = sql.format(start_time=start_time_shifted, end_time=end_time)
    data = fetch_db_data(jydb, sql, ['time', 'vol', 'high', 'low', 'symbol'],
                         dtypes={'vol': 'float64', 'high': 'float64', 'low': 'float64'})
    data.symbol = data.symbol.apply(add_stock_suffix)
    ma_vol = data.groupby('symbol', as_index=False).vol.rolling(offset, min_periods=offset).mean()
    ma_vol = ma_vol.reset_index(level=0, drop=True)
    data = data.assign(ma_vol=ma_vol)
    data = data.assign(flag=1)
    # pdb.set_trace()
    data.loc[np.isclose(data.ma_vol, 0, 0.1), 'ma_vol'] = np.nan  # (上个交易日)移动平均成交量过低
    data.ma_vol = data.vol / data.ma_vol
    data.loc[np.isclose(data.vol, 0, 0.1), 'flag'] = 0     # 成家量过低，不可交易
    data.loc[data.high == data.low, 'flag'] = 0     # 最高价等于最低价，不可交易
    # 移动平均成交量过低，不可交易
    data.loc[np.isclose(data.ma_vol, 0, 0.05) | (pd.isnull(data.ma_vol)), 'flag'] = 0
    data = data.pivot_table('flag', index='time', columns='symbol')
    last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
    universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
    data = data.loc[(data.index >= start_time) & (data.index <= end_time)].reindex(columns=universe)
    if not check_completeness(data.index, start_time, end_time):
        raise ValueError('Error, data missed!')
    return data


dd = DataDescription(get_trade_status, trans_date('2018-05-04'), DataType.PANEL_NUMERIC,
                     dep=['UNIVERSE', 'LIST_STATE'],
                     desc='股票可交易状态：0表示不可交易，1表示可交易。可交易的定义为：' +
                          '最高价不等于最低价，当前成交量高于（30个交易日）移动平均成交量的1%，' +
                          '当前成交量大于0')
