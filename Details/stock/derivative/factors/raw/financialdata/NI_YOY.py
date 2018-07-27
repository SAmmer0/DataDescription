#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-06-07 14:43
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

def denominator(start_time, end_time):
    """
    用于计算分母的数据，分母 = NI1S - NI5S

    Parameter
    ---------
    start_time: datetime like
        开始时间
    end_time: datetime like
        结束时间

    Return
    ------
    out: pandas.DataFrame
    """
    ni1s = pitcache_getter('NI1S', 10).get_tsdata(start_time, end_time)
    ni5s = pitcache_getter('NI5S', 10).get_tsdata(start_time, end_time)
    return ni1s - ni5s

def numerator(start_time, end_time):
    """
    用于计算分子数据，分子 = abs(NI5S)
    Parameter
    ---------
    start_time: datetime like
        开始时间
    end_time: datetime like
        结束时间

    Return
    ------
    out: pandas.DataFrame
    """
    ni5s = pitcache_getter('NI5S', 10).get_tsdata(start_time, end_time)
    return np.abs(ni5s)

dd = DataDescription(divide_data_factory(denominator, numerator), trans_date('2018-06-07'),
                     DataType.PANEL_NUMERIC, ['NI1S', 'NI5S', 'LIST_STATE'],
                     '单季度归属股东净利润同比增长率')