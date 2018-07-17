#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-07-17 13:38
"""
from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from fundamental_template import xps_shift_factory 

dd = DataDescription(xps_shift_factory('ISY', 'TotalOperatingRevenue', 1, 4), trans_date('2018-07-17'),
                     DataType.PANEL_NUMERIC, dep=['UNIVERSE', 'LIST_STATE'], 
                     desc='最近一个年度的SPS=最近财年的总营业收入/总股本')