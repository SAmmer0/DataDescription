#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-30 16:51
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
    # 营业利润=营业收入-营业成本-销售费用-管理费用-财务费用
    opr_ttm = pitcache_getter('OPR_TTM', 10).get_tsdata(start_time, end_time)
    opcost_ttm = pitcache_getter('OPCOST_TTM', 10).get_tsdata(start_time, end_time)
    opexp_ttm = pitcache_getter('OPEXP_TTM', 10).get_tsdata(start_time, end_time)
    adminexp_ttm = pitcache_getter('ADMINEXP_TTM', 10).get_tsdata(start_time, end_time)
    fiexp_ttm = pitcache_getter('FIEXP_TTM', 10).get_tsdata(start_time, end_time)
    return opr_ttm - opcost_ttm - opexp_ttm - adminexp_ttm - fiexp_ttm

dd = DataDescription(divide_data_factory(numerator,
                                         abs_wrapper(pitcache_getter('OPR_TTM', 10).get_tsdata)),
                     trans_date('2018-07-30'), DataType.PANEL_NUMERIC, 
                     ['OPR_TTM', 'OPCOST_TTM', 'OPEXP_TTM', 'ADMINEXP_TTM', 'FIEXP_TTM'],
                     '总营业利润率 = (营业收入-营业成本-销售费用-管理费用-财务费用) / abs(营业收入)', in_test=True)