#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-30 13:34
"""

from sys import path as sys_path
from os.path import dirname

import numpy as np

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory

def ta_dropped_negetive(start_time, end_time):
    '''
    将总资产为负的总资产数据设置为NA
    '''
    data = pitcache_getter('TA5S', 10).get_tsdata(start_time, end_time)
    data = data.where(data>0, np.nan)
    return data

dd = DataDescription(divide_data_factory(pitcache_getter('NI_TTM', 10).get_tsdata, 
                                         ta_dropped_negetive),
                     trans_date('2018-07-30'), DataType.PANEL_NUMERIC, 
                     ['NI_TTM', 'TA5S'], 'ROA TTM等于NI_TTM/TA5S')