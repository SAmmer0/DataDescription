#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-25 15:23
"""
from data_checkers import drop_delist_data, check_completeness

def divide_data_factory(denominator_func, numerator_func, denominator_kwargs=None,
                        numerator_kwargs=None):
    """
    母函数，用于生成计算相除的数据的函数

    Parameter
    ---------
    denominator_func: function
        用于获取分母部分的数据，格式为function(start_time, end_time, **denominator_kwargs)
    numerator_func: function
        用于获取分子部分的数据，格式为function(start_time, end_time, **numerator_kwargs)
    denominator_kwargs: dict, default None
        denominator_func的字典格式的参数，None表示对应函数不需要参数
    numerator_kwargs: dict, default None
        numerator_func的字典格式参数，None表示对应函数不需要参数

    Return
    ------
    func: function(start_time, end_time)
    """
    if denominator_kwargs is None:
        denominator_kwargs = {}
    if numerator_kwargs is None:
        numerator_kwargs = {}

    @drop_delist_data
    def inner(start_time, end_time):
        denominator_data = denominator_func(start_time, end_time, **denominator_kwargs)
        numerator_data = numerator_func(start_time, end_time, **numerator_kwargs)
        data = denominator_data / numerator_data
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Data missed!')
        return data
    return inner