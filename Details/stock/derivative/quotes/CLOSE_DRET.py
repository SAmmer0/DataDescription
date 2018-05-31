#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-14 16:57:55
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
from quotes_template import ret_factory

dd = DataDescription(ret_factory(1, 'CLOSE_ADJ'), trans_date('2018-05-14'),
                     DataType.PANEL_NUMERIC, dep=['CLOSE_ADJ', 'LIST_STATE'],
                     desc='根据后复权收盘价计算的日收益率')
