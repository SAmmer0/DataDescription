#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-11 16:06:21
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
# 计算股票后复权价格
from tdtools import get_calendar
from pitdata import pitcache_getter
from data_checkers import check_completeness


def get_adjprice(adjprice_name, adjfactor_name):
    '''
    母函数，用于生成计算后复权价格的函数

    Parameter
    ---------
    adjprice_name: string
        复权的价格的名称
    adjfactor_name: string
        复权因子名称
    '''
    def inner(start_time, end_time):
        offset = 10
        price_data = pitcache_getter(adjprice_name, offset).get_tsdata(start_time, end_time)
        adj_factor = pitcache_getter(adjfactor_name, offset).get_tsdata(start_time, end_time)
        data = price_data * adj_factor
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Error, data missed!')
        return data
    return inner
