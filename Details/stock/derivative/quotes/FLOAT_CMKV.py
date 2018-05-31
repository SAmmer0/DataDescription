#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-14 16:43:54
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from sys import path as sys_path
from os.path import dirname

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from quotes_template import mkv_factory

dd = DataDescription(mkv_factory('FLOAT_SHARES', 'CLOSE'), trans_date('2018-05-14'),
                     DataType.PANEL_NUMERIC, dep=['CLOSE', 'FLOAT_SHARES', 'LIST_STATE'],
                     desc='流通市值（由收盘价计算）')
