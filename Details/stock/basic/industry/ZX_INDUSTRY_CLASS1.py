#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-09 13:36:37
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
from industry_template import get_industry_classification

ZX_CLASS1_TRANSLATE_TABLE = {'银行': 'bank', '房地产': 'realestate', '计算机': 'computer',
                             '医药': 'med', '餐饮旅游': 'food&tour', '有色金属': 'metal',
                             '商贸零售': 'retail', '交通运输': 'transportation', '机械': 'machine',
                             '综合': 'comprehensive', '电子元器件': 'electronic_component',
                             '建筑': 'building', '建材': 'building_materials', '家电': 'home_appliances',
                             '纺织服装': 'textiles', '食品饮料': 'food&drink', '石油石化': 'petroche',
                             '汽车': 'auto', '轻工制造': 'manufacturing',
                             '电力及公用事业': 'electricity&utility', '通信': 'communication',
                             '农林牧渔': 'agriculture', '电力设备': 'power_equipment',
                             '基础化工': 'chemical', '传媒': 'media', '煤炭': 'coal',
                             '非银行金融': 'non-bank_finance', '钢铁': 'steel', '国防军工': 'war_industry'}
dd = DataDescription(get_industry_classification(3, 1, ZX_CLASS1_TRANSLATE_TABLE),
                     trans_date('2018-05-09'), DataType.PANEL_CHAR, dep=['UNIVERSE'],
                     desc='中信一级行业分类')
