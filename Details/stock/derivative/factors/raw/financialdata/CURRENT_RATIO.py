#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-08-07 16:32
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

dd = DataDescription(divide_data_factory(pitcache_getter('TCA', 10).get_tsdata,
                                         pitcache_getter('TCL', 10).get_tsdata),
                     trans_date('2018-08-07'), DataType.PANEL_NUMERIC, 
                     ['TCA', 'TCL'],
                     '流动比率 = 流动资产 / 流动负债')