#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-09 13:39:52
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from numpy import unicode as np_unicode
from numpy import any as np_any

from datasource.sqlserver.utils import fetch_db_data
from datasource.sqlserver.jydb import jydb, map2td
from datautils.toolkits import add_stock_suffix
from tdtools import get_calendar
from pitdata import pitcache_getter
from qrtconst import NaS
from data_checkers import check_completeness


INDUSTRY_LEVEL_MAP = {
    1: 'FirstIndustryName',
    2: 'SecondIndustryName',
    3: 'ThirdIndustryName',
    4: 'FourthIndustryName'
}


def get_industry_classification(class_standard, class_level, translate_table):
    '''
    母函数，用于生成获取行业分类的函数

    Parameter
    ---------
    class_standard: int
        行业分类标准，参见聚源数据库字典LC_exgIndustry表关于standard的相关说明
    class_level: int
        行业分类划分规则，具体映射键INDUSTRY_LEVEL_MAP
    translate_table: dict
        将数据中的中文转换为英文的翻译表

    Return
    ------
    func: function(start_time, end_time)
    '''
    def inner(start_time, end_time):
        sql = '''
        SELECT S.{level}, S.InfoPublDate, M.SecuCode
        FROM LC_exgIndustry S, SecuMain M
        WHERE S.CompanyCOde = M.CompanyCode AND
            S.Standard = {standard} AND
            M.SecuCategory = 1 AND
            M.SecuMarket in (90, 83)
        ORDER BY M.Secucode, S.InfoPublDate ASC
        '''.format(level=INDUSTRY_LEVEL_MAP[class_level], standard=class_standard)
        data = fetch_db_data(jydb, sql, ['data', 'time', 'symbol'])
        data['data'] = data.data.map(translate_table)
        data['symbol'] = data.symbol.apply(add_stock_suffix)
        by_symbol = data.groupby('symbol')
        tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
        data = by_symbol.apply(map2td, days=tds, timecol='time',
                               fillna={'data': lambda x: NaS, 'symbol': lambda x: x.symbol.iloc[0]})
        data = data.pivot_table('data', index='time', columns='symbol',
                                aggfunc=lambda x: ';'.join(x))
        check_agg_error(data)
        last_td = get_calendar('stock.sse').latest_tradingday(end_time, 'PAST')
        universe = sorted(pitcache_getter('UNIVERSE', 10).get_csdata(last_td).index)
        data = data.reindex(columns=universe).fillna(NaS).astype(np_unicode)
        if not check_completeness(data.index, start_time, end_time):
            raise ValueError('Error, data missed!')
        return data
    return inner


def check_agg_error(data):
    '''
    检查是否有数据在pivot_table中使用了join
    '''
    for row in data.iterrows():
        if np_any(row[1].str.contains(';')):
            raise ValueError('\'join\' has been performed!')
