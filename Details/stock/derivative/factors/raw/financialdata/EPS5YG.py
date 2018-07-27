#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-18 11:08
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import growth_rate_factory

dd = DataDescription(growth_rate_factory('EPS{i:d}Y', 5), trans_date('2018-07-18'),
                     DataType.PANEL_NUMERIC, ['EPS{i:d}Y'.format(i=i) for i in range(1, 6)],
                     desc='过去5年EPS增长率')