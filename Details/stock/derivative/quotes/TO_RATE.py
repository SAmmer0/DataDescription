#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-16 16:16
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from data_checkers import check_completeness, drop_delist_data

@drop_delist_data
def get_torate(start_time, end_time):
    """
    计算换手率，计算公式为当日交易量/流通股数

    Parameter
    ---------
    start_time: datetime like
    end_time: datetime like

    Return
    ------
    out: pandas.DataFrame
    """
    volume_data = pitcache_getter('TO_VOLUME', 50).get_tsdata(start_time, end_time)
    float_shares = pitcache_getter('FLOAT_SHARES', 50).get_tsdata(start_time, end_time)
    data = volume_data / float_shares
    if not check_completeness(data.index, start_time, end_time):
        raise  ValueError('Data missed!')
    return data

dd = DataDescription(get_torate, trans_date('2018-05-16'), DataType.PANEL_NUMERIC, 
                     dep=['TO_VOLUME', 'FLOAT_SHARES', 'LIST_STATE'], desc='换手率，由当日交易量/流通股数得到')