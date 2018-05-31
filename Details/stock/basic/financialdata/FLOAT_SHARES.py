#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-10 15:22:15
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
from stock_share_template import get_shares

dd = DataDescription(get_shares('NonResiSharesJY'), trans_date('2018-05-10'),
                     DataType.PANEL_NUMERIC, dep=['UNIVERSE', 'LIST_STATE'], desc='流通股本')
