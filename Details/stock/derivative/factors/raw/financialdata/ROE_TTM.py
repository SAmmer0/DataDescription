#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-27 17:08
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import divide_data_factory

dd = DataDescription(divide_data_factory(pitcache_getter('NI_TTM').get_tsdata, 
                                         pitcache_getter('EQUITY').get_tsdata),
                     trans_date('2018-07-27'), DataType.PANEL_NUMERIC, 
                     ['NI_TTM', 'EQUITY'], 'ROE TTM')