import pandas as pd
import numpy as np
from miller_value import MillerValueStrategy
from miller_value import FinancialDataRepository
from miller_value import MillerStrategyRunner
from config import DB_CONFIG

def make_series(values):
    return pd.Series(values)

def test_compute():
    s = MillerValueStrategy()
    data_repo = FinancialDataRepository(DB_CONFIG)
    assert data_repo.connect()
    runner = MillerStrategyRunner(data_repo, "2023-12-31")
    result = runner.run_for_stock("000028")
    profit = data_repo.get_profit_quarterly("000002", 4, "2023-12-31")
    data_repo.close()
    '''
    income = pd.DataFrame({
        '营业利润': make_series([100,120,130,140,110,115,118,122,125,128,130,135]),
        '所得税费用': make_series([20,24,26,28,22,23,24,25,25,26,27,27]),
        '利润总额': make_series([150,160,170,180,155,158,160,165,168,170,172,175]),
        '净利润': make_series([80,90,95,100,85,88,90,92,94,96,98,100]),
        '营业成本': make_series([300,320,310,330,305,315,318,320,322,325,328,330])
    })
    '''
    balance = pd.DataFrame({
        '归属于母公司所有者权益': make_series([1000]*12),
        '短期借款': make_series([100]*12),
        '一年内到期非流动负债': make_series([50]*12),
        '长期借款': make_series([200]*12),
        '应付债券': make_series([0]*12),
        '应收账款': make_series([200]*12),
        '预付账款': make_series([50]*12),
        '存货': make_series([400,420,410,430,405,415,418,420,422,425,428,430]),
        '应收票据': make_series([30]*12),
        '应付账款': make_series([250]*12),
        '预收账款': make_series([60]*12),
        '应付票据': make_series([40]*12),
        '应付职工薪酬': make_series([30]*12),
        '应交税费': make_series([20]*12)
    })
    cash = pd.DataFrame({
        '经营活动现金流量净额': make_series([120,130,125,135,122,128,130,132]),
        '购建固定资产、无形资产和其他长期资产支付的现金': make_series([40,42,41,43,40,41,42,43])
    })
    roc_q = s.compute_roc_quarterly(profit, balance)
    rooc_q = s.compute_rooc_quarterly(profit, balance)
    it_q = s.compute_inventory_turnover_quarterly(profit, balance)
    assert not roc_q.tail(4).isna().all()
    assert not rooc_q.tail(4).isna().all()
    assert not it_q.tail(4).isna().all()
    market_cap = 5000.0
    pe_ttm = s.compute_pe_ttm(market_cap, profit['净利润'])
    cfo = cash['经营活动现金流量净额']
    capex = cash['购建固定资产、无形资产和其他长期资产支付的现金']
    pfcf_ttm = s.compute_pfcf_ttm(market_cap, cfo, capex)
    assert not np.isnan(pe_ttm)
    assert not np.isnan(pfcf_ttm)

if __name__ == '__main__':
    test_compute()
    print('OK')

