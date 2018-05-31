#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-22 14:08
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from quotes_template import nl_lnmkv_factory

dd = DataDescription(nl_lnmkv_factory('FLOAT_LNCMKV', 3), trans_date('2018-05-22'), 
                    DataType.PANEL_NUMERIC, dep=['FLOAT_LNCMKV'], 
                    desc='对数流通市值的3次方')