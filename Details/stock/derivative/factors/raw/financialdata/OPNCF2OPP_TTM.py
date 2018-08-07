#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-08-07 16:44
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

dd = DataDescription(divide_data_factory(pitcache_getter('OPNCF_TTM', 10).get_tsdata,
                                         pitcache_getter('OPP_TTM', 10).get_tsdata),
                     trans_date('2018-08-07'), DataType.PANEL_NUMERIC, 
                     ['OPNCF_TTM', 'OPP_TTM'],
                     '经营活动中的现金流量净额TTM/营业利润TTM')