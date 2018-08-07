#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-08-07 16:12
"""

from sys import path as sys_path
from os.path import dirname

import numpy as np

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory, abs_wrapper

def numerator(start_time, end_time):
    # 毛利 = 营业收入 - 营业成本
    opr_ttm = pitcache_getter('OPR_TTM', 10).get_tsdata(start_time, end_time)
    opcost_ttm = pitcache_getter('OPCOST_TTM', 10).get_tsdata(start_time, end_time)
    return opr_ttm - opcost_ttm

dd = DataDescription(divide_data_factory(numerator,
                                         abs_wrapper(pitcache_getter('OPR_TTM', 10).get_tsdata)),
                     trans_date('2018-08-07'), DataType.PANEL_NUMERIC, 
                     ['OPR_TTM', 'OPCOST_TTM'],
                     '毛利率 = (营业收入-营业成本) / abs(营业收入)，该值低于-1表示营业收入为负')