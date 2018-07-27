#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-25 16:27
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory

dd = DataDescription(divide_data_factory(pitcache_getter('EQUITY', 10).get_tsdata, 
                                         pitcache_getter('TOTAL_CMKV', 10).get_tsdata),
                    trans_date('2018-05-25'), DataType.PANEL_NUMERIC, 
                    ['EQUITY', 'TOTAL_CMKV', 'LIST_STATE'], '归属母公司的权益TTM/总市值')