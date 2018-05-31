#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-02 15:57:19
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
import pdb
from functools import wraps
from numpy import logical_or, nan

from tdtools import get_calendar
from pitdata import pitcache_getter


def check_completeness(time_index, start_time, end_time):
    '''
    检查数据在时间轴上的完整性

    Parameter
    ---------
    time_index: iterable
        数据的时间轴
    start_time: datetime like
        设定的数据的开始时间
    end_time: datetime like
        设定的数据的结束时间

    Return
    ------
    result: boolean
        若数据完整，返回True
    '''
    expect = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    return sorted(time_index) == expect


def drop_delist_data(func):
    '''
    装饰器，用于将已经退市的股票的数据设置为NaN
    任何使用了该装饰器的数据都需要在依赖项中添加'LIST_STATE'

    Parameter
    ---------
    func: function(start_time, end_time)
    '''
    @wraps(func)
    def inner(start_time, end_time):
        list_state = pitcache_getter('LIST_STATE', 10).get_tsdata(start_time, end_time)
        valid_mask = logical_or(list_state == 1, list_state == 2)
        data = func(start_time, end_time)
        data[~valid_mask] = nan
        return data
    return inner
