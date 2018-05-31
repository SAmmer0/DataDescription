#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-20 14:32:08
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from pandas import DataFrame, to_datetime

from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import jydb
from pitdata.utils import DataDescription
from pitdata.const import DataType
from datautils.toolkits import add_stock_suffix
from tdtools import get_calendar


def get_universe(start_time, end_time):
    sql = '''
    SELECT SecuCode
    FROM SecuMain
    WHERE
        SecuCategory = 1 AND
        SecuMarket in (83, 90) AND
        ListedState != 9
    '''
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    data = fetch_db_data(jydb, sql, ['symbol'])
    symbols = sorted(add_stock_suffix(s) for s in data.symbol)
    out = DataFrame(1, index=tds, columns=symbols)
    return out


dd = DataDescription(get_universe, to_datetime('2018-04-23'), DataType.PANEL_NUMERIC,
                     desc='A stock universe')
