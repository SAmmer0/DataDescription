#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-23 16:28
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from fundamental_template import shift_factory 

dd = DataDescription(shift_factory('BS', 'SEWithoutMI', 1, 1), trans_date('2018-05-23'),
                     DataType.PANEL_NUMERIC, dep=['UNIVERSE', 'LIST_STATE'], 
                     desc='最近一个报告期的归属母公司的权益')

