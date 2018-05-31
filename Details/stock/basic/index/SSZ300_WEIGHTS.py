#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-09 11:04:25
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
from index_template import get_index_weights

dd = DataDescription(get_index_weights('000300'), trans_date('2018-05-09'),
                     DataType.PANEL_NUMERIC, dep=['UNIVERSE'],
                     desc='沪深300指数成分权重，数值表示权重((0, 100))，其他(NA)表示非指数成分')
