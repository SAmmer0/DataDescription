#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-02 16:40:24
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
from quotes_template import quotes_factory


dd = DataDescription(quotes_factory('OpenPrice'), trans_date('2018-05-02'), DataType.PANEL_NUMERIC,
                     dep=['UNIVERSE'], desc='未复权开盘价')
