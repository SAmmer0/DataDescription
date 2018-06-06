#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-06-05 13:23
"""

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from index_template import get_index_quote

dd = DataDescription(get_index_quote('000985', 'ClosePrice'),
                     trans_date('2018-06-05'), DataType.TS_NUMERIC, desc='中证全指收盘价')
