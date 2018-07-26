#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-26 16:42
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import vol_factory

dd = DataDescription(vol_factory(20), trans_date('2018-07-26'), DataType.PANEL_NUMERIC,
                     dep=['CLOSE_DRET', 'LIST_STATE', 'UNIVERSE'], 
                     desc='20个交易日收盘价计算的波动率')