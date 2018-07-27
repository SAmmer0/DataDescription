#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-25 16:49
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory

def get_ev(start_time, end_time):
    mkv_data = pitcache_getter('TOTAL_CMKV', 10).get_tsdata(start_time, end_time)
    ncl_data = pitcache_getter('TNCL', 10).get_tsdata(start_time, end_time).fillna(0)
    cash_data = pitcache_getter('CASH', 10).get_tsdata(start_time, end_time).fillna(0)
    return mkv_data + ncl_data - cash_data

dd = DataDescription(divide_data_factory(pitcache_getter('OPR_TTM', 10).get_tsdata, 
                                         get_ev),
                    trans_date('2018-05-25'), DataType.PANEL_NUMERIC, 
                    ['CASH', 'TNCL', 'OPR_TTM', 'TOTAL_CMKV', 'LIST_STATE'], 
                    '营业收入TTM/(总市值+非流动性负债合计-现金)')