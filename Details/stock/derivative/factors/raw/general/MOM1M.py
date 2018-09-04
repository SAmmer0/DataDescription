#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-08-21 16:37
"""
from sys import path as sys_path
from os.path import dirname

import numpy as np

from tdtools import trans_date
from pitdata.const import CALCULATION_FOLDER_PATH, DataType
from pitdata import DataDescription, pitcache_getter
if CALCULATION_FOLDER_PATH not in sys_path:
    sys_path.append(dirname(CALCULATION_FOLDER_PATH))
from derivate_template import momentum_factory

dd = DataDescription(momentum_factory(22), trans_date('2018-08-21'), 
                     DataType.PANEL_NUMERIC, ['UNIVERSE', 'CLOSE_ADJ', 'LIST_STATE'], 
                     '1月动量', True)