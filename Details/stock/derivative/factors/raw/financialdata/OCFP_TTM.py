#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-25 16:44
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory

dd = DataDescription(divide_data_factory(pitcache_getter('OPNCF_TTM', 10).get_tsdata, 
                                         pitcache_getter('TOTAL_CMKV', 10).get_tsdata),
                    trans_date('2018-05-25'), DataType.PANEL_NUMERIC, 
                    ['OPNCF_TTM', 'TOTAL_CMKV', 'LIST_STATE'], '经营活动产生的现金流净额TTM/总市值')