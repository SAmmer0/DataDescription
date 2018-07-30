#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-20 14:32:08
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from sys import path as sys_path
from os.path import dirname

from pandas import DataFrame, to_datetime

from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import jydb
from pitdata.utils import DataDescription
from pitdata.const import DataType, CALCULATION_FOLDER_PATH
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from datautils.toolkits import add_stock_suffix
from tdtools import get_calendar
from data_checkers import check_jydb_update_state


def get_universe(start_time, end_time):
    sql = '''
    SELECT SecuCode
    FROM SecuMain
    WHERE
        SecuCategory = 1 AND
        SecuMarket in (83, 90) AND
        ListedState != 9
    '''
    if not check_jydb_update_state(end_time):
        raise ValueError('JYDB has not been updated!')
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    data = fetch_db_data(jydb, sql, ['symbol'])
    symbols = sorted(add_stock_suffix(s) for s in data.symbol)
    out = DataFrame(1, index=tds, columns=symbols)
    return out


dd = DataDescription(get_universe, to_datetime('2018-04-23'), DataType.PANEL_NUMERIC,
                     desc='A stock universe')
