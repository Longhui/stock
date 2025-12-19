'''
## 选股标淮:

A.近四季资本报酬率> 市场平均值。
B.近四季资本报酬率> 一年期定存利率。
C.近四季资本报酬率> 最近 5~8 季资本报酬率。
D.近四季营运报酬率> 产业平均值。
E.最近四季营运报酬率> 最近 5~8 季营运报酬率。
F.最近四季存货周转次数> 产业平均值。
G.最近四季存货周转次数> 最近 5~8 季存货周转次数。

## 买入条件:

以下五个条件中符合其中三个及以上即符合买入条件
A.最近一季股价净值比< 市场平均值的 2.0 倍。

B.最近四季本益比< 1/一年期定存利率。
C.近四季资本益比< 五年平均本益比。
D.最近四季股价自由现金流量比< 1/通货膨胀率。
E.市值/10 年自由现金流量折现值< 1.0。
'''

import logging
import pymysql
import pandas as pd
import numpy as np
from typing import List
from config import DB_CONFIG
# 计算从start_date开始的90天每一天的日期
from datetime import datetime, timedelta

# 配置日志直接输出到终端
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 直接输出到终端
    ]
)
logger = logging.getLogger(__name__)

class FinancialDataRepository:
    """财务数据仓库类，负责从数据库获取股票财务数据"""
    
    def __init__(self, db_config: dict):
        """
        初始化财务数据仓库
        
        Args:
            db_config: 数据库连接配置字典，包含host、port、user、password、database等字段
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.roc_avg_cache = {}  # 内存缓存字典，存储(end_date, roc_avg)键值对
        self.rooc_avg_cache = {}  # 内存缓存字典，存储(end_date, rooc_avg)键值对
        self.pb_avg_cache = {}  # 内存缓存字典，存储(end_date, pb_avg)键值对
        self.inventory_turnover_cache = {}  # 内存缓存字典，存储(end_date, inventory_turnover)键值对

    def connect(self) -> bool:
        """
        连接到数据库
        
        Returns:
            bool: 连接成功返回True，失败返回False
        """
        try:
            self.connection = pymysql.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 3306),
                user=self.db_config.get('user', 'root'),
                password=self.db_config.get('password', ''),
                database=self.db_config.get('database', 'stock'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_profit_quarterly(self, stock_code: str, limit: int = 12, end_date: str = None) -> pd.DataFrame:
        """
        获取季度利润表数据
        
        Args:
            stock_code: 股票代码
            limit: 获取的季度数量，默认12个季度
            end_date: 截止日期（格式：YYYY-MM-DD），默认为当前日期
            
        Returns:
            pd.DataFrame: 季度利润表数据
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # 构建SQL查询语句
            sql = """
            SELECT * FROM profit 
            WHERE Stkcd = %s 
            AND Accper <= %s 
            ORDER BY Accper DESC 
            LIMIT %s
            """
            
            # 执行查询
            self.cursor.execute(sql, (stock_code, end_date, limit))
            result = self.cursor.fetchall()
            
            # 转换为DataFrame
            if result:
                df = pd.DataFrame(result)
                return df
            else:   
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"查询季度利润表数据失败: {e}")
            return pd.DataFrame()

    def get_balance_quarterly(self, stock_code: str, limit: int = 12, end_date: str = None) -> pd.DataFrame:
        """
        获取季度资产负债表数据
        
        Args:
            stock_code: 股票代码
            limit: 获取的季度数量，默认12个季度
            end_date: 截止日期（格式：YYYY-MM-DD），默认为当前日期
            
        Returns:
            pd.DataFrame: 季度资产负债表数据
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # 构建SQL查询语句
            sql = """
            SELECT * FROM balance 
            WHERE Stkcd = %s 
            AND Accper <= %s 
            ORDER BY Accper DESC 
            LIMIT %s
            """
            
            # 执行查询
            self.cursor.execute(sql, (stock_code, end_date, limit))
            result = self.cursor.fetchall()
            
            # 转换为DataFrame
            if result:
                df = pd.DataFrame(result)
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"查询季度资产负债表数据失败: {e}")
            return pd.DataFrame()

    def get_cashflow_quarterly(self, stock_code: str, limit: int = 12, end_date: str = None) -> pd.DataFrame:
        """
        获取季度现金流量表数据
        
        Args:
            stock_code: 股票代码
            limit: 获取的季度数量，默认12个季度
            end_date: 截止日期（格式：YYYY-MM-DD），默认为当前日期
            
        Returns:
            pd.DataFrame: 季度现金流量表数据
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # 构建SQL查询语句
            sql = """
            SELECT * FROM cash_flow 
            WHERE Stkcd = %s 
            AND Accper <= %s 
            ORDER BY Accper DESC 
            LIMIT %s
            """
            
            # 执行查询
            self.cursor.execute(sql, (stock_code, end_date, limit))
            result = self.cursor.fetchall()
            
            # 转换为DataFrame
            if result:
                df = pd.DataFrame(result)
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"查询季度现金流量表数据失败: {e}")
            return pd.DataFrame()

    def get_roc_avg(self, end_date: str = None) -> float:
        """
        获取市场平均roc
        
        Args:
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期
            
        Returns:
            float: 市场平均指标值
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 首先从内存缓存中查询
        if end_date in self.roc_avg_cache:
            return self.roc_avg_cache[end_date]
        
        try:
            sql = """
            select avg(p.OpProfit*(1 - p.IncomeTax/p.ProfitBefTax)/(b.ParOwnEquity + b.ShortBorrow + NonCurLia1Y + b.LTBorrow+b.BondPay)) as avg_roc
             from stock1.balance b join stock1.profit p 
             on b.Stkcd = p.Stkcd and b.Accper = p.Accper
             where b.Accper = (select Accper from balance where Accper <= %s order by Accper desc limit 1)
            """
            
            # 执行查询
            self.cursor.execute(sql, (end_date,))
            result = self.cursor.fetchone()
            
            # 返回单个浮点数值
            if result and result['avg_roc'] is not None:
                roc_avg = float(result['avg_roc'])
                # 2. 将查询结果存储到内存缓存中
                self.roc_avg_cache[end_date] = roc_avg
                return roc_avg
            else:
                logger.warning(f"未找到{end_date}的roc_avg数据")
                return np.nan
                
        except Exception as e:
            logger.error(f"查询市场平均roc失败: {e}")
            return np.nan

    def get_rooc_avg(self, end_date: str = None) -> float:
        """
        获取市场平均rooc

        Args:
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期
            
        Returns:
            float: 市场平均指标值
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 首先从内存缓存中查询
        if end_date in self.rooc_avg_cache:
            return self.rooc_avg_cache[end_date]
        
        try:
            # 构建SQL查询语句
            sql = """
            SELECT avg(rooc) as avg_rooc from (
              SELECT nopat/nowc as rooc
              FROM (
                SELECT
                  (p.OpProfit*(1 - p.IncomeTax/p.ProfitBefTax)) AS nopat,
                  (b.AcctRecNet + b.PrepayNet + b.InventNet + b.NotesRecNet
                  - b.AcctPay - b.AdvFromCust - b.NotesPay
                  - b.EmpBenefitPay - b.TaxPay) AS nowc
                FROM balance AS b
                   JOIN profit  AS p
                   ON b.Stkcd = p.Stkcd AND b.Accper = p.Accper
                WHERE b.Accper = (select Accper from balance where Accper <= %s order by Accper desc limit 1)
              ) AS t
              WHERE nopat > 0 and nowc > 0
            ) as s;
            """
            
            # 执行查询
            self.cursor.execute(sql, (end_date,))
            result = self.cursor.fetchone()
            
            # 返回单个浮点数值
            if result and result['avg_rooc'] is not None:
                rooc_avg = float(result['avg_rooc'])
                # 2. 将查询结果存储到内存缓存中
                self.rooc_avg_cache[end_date] = rooc_avg
                return rooc_avg
            else:
                logger.warning(f"未找到{end_date}的rooc_avg数据")
                return np.nan
                
        except Exception as e:
            logger.error(f"查询市场平均rooc失败: {e}")
            return np.nan
        
    def get_avg_inventory_turnover(self, end_date: str = None) -> float:
        """
        计算季度平均存货周转率
        
        Args:
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期
            
        Returns:
            float: 季度平均存货周转率
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 首先从内存缓存中查询
        if end_date in self.inventory_turnover_cache:
            return self.inventory_turnover_cache[end_date]
        
        try:
            # 构建SQL查询语句
            sql = """
            select 2 * (t3.OpCost / (t1.InventNet + t2.InventNet)) as avg_it
            from
              (select InventNet,Stkcd from balance b where b.Accper = (select Accper from balance where Accper <= %s order by Accper desc limit 1)) t1,
              (select InventNet,Stkcd from balance b where b.Accper < %s order by b.Accper desc limit 1) t2,
              (select Stkcd,OpCost from profit p where p.Accper = (select Accper from balance where Accper <= %s order by Accper desc limit 1)) t3
            where t1.Stkcd = t2.Stkcd and t1.Stkcd = t3.Stkcd;
            """
            
            # 执行查询
            self.cursor.execute(sql, (end_date,end_date,end_date,))
            result = self.cursor.fetchone()
            
            # 返回单个浮点数值
            if result and result['avg_it'] is not None:
                inventory_turnover = float(result['avg_it'])
                # 2. 将查询结果存储到内存缓存中
                self.inventory_turnover_cache[end_date] = inventory_turnover
                return inventory_turnover
            else:
                logger.warning(f"未找到{end_date}的存货周转率数据")
                return np.nan
        except Exception as e:
            logger.error(f"查询存货周转率失败: {e}")
            return float('nan')
    
    def get_pb_avg(self, end_date: str = None) -> float:
        """
        获取市场平均pb
        
        Args:
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期
            
        Returns:
            float: 市场平均指标值
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 首先从内存缓存中查询
        if end_date in self.pb_avg_cache:
            return self.pb_avg_cache[end_date]
        
        try:
            # 构建SQL查询语句
            sql = """
            select  
             avg(t.Clsprc/(b.ParOwnEquity/s.Nshrttl)) as avg_pb
            FROM
              (select ParOwnEquity,Stkcd FROM balance where Accper = 
                (select Accper from balance where Accper <= %s order by Accper desc limit 1)) b
              join LATERAL
              (select Clsprc,Stkcd FROM trade WHERE Trddt <= %s and Stkcd = b.Stkcd order by Trddt desc limit 1) t 
               on b.Stkcd = t.Stkcd
              join LATERAL
             (SELECT Nshrttl,Stkcd FROM shares WHERE Reptdt <= %s and Stkcd = b.Stkcd order by Reptdt desc limit 1) s
             on t.Stkcd = s.Stkcd;
            """
            
            # 执行查询
            self.cursor.execute(sql, (end_date,end_date,end_date,))
            result = self.cursor.fetchone()
            
            # 返回单个浮点数值
            if result and result['avg_pb'] is not None:
                pb_avg = float(result['avg_pb'])
                # 2. 将查询结果存储到内存缓存中
                self.pb_avg_cache[end_date] = pb_avg
                return pb_avg
            else:
                logger.warning(f"未找到{end_date}的pb_avg数据")
                return np.nan
        except Exception as e:
            logger.error(f"查询pb指标失败: {e}")
            return float('nan')
    
    def get_deposit_rate(self, end_date: str = None) -> float:
        """
        获取存款利率
        
        Returns:
            float: 当前存款利率
        """
        return 0.015

    def get_inflation_rate(self, end_date: str = None) -> float:
        """
        获取通货膨胀率
        
        Args:
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则使用当前日期
            
        Returns:
            float: 指定年份的通货膨胀率，如果查询失败返回NaN
        """
        if end_date is None:
            end_date = self.end_date
        
        try:
            # 从日期中提取年份
            year = int(end_date[:4])
            
            # 查询inflation_cn表获取对应年份的通货膨胀率
            sql = "SELECT rate FROM inflation_cn WHERE year = %s"
            self.cursor.execute(sql, (year,))
            result = self.cursor.fetchone()
            
            if result:
                inflation_rate = float(result['rate'])
                return inflation_rate
            else:
                logger.warning(f"未找到{year}年的通货膨胀率数据")
                return float('nan')
                
        except Exception as e:
            logger.error(f"查询通货膨胀率失败: {e}")
            return float('nan')

    def get_discount_rate(self) -> float:
        """
        获取折现率
        
        Returns:
            float: 折现率
        """
        return float(0.09)

    def get_market_cap(self, stock_code: str, end_date: str) -> float:
        """
        获取股票市值
        
        Args:
            stock_code: 股票代码
            
        Returns:
            float: 股票市值（单位：元）
        """
        try:
            # 查询shares表获取股票市值
            sql = """
            select
            (t.Clsprc * s.Nshrttl) as cap
            from ( SELECT Clsprc,Stkcd FROM trade 
                    WHERE Stkcd = %s AND Trddt <= %s order by Trddt desc limit 1) t join
                 ( SELECT Nshrttl,Stkcd FROM shares 
                    WHERE Stkcd = %s AND Reptdt <= %s order by Reptdt desc limit 1) s
            on t.Stkcd = s.Stkcd;
            """
            
            self.cursor.execute(sql, (stock_code, end_date, stock_code, end_date))
            result = self.cursor.fetchone()
            
            if result:
                total_shares = float(result['cap'])
                return total_shares
            else:
                logger.warning(f"未找到股票{stock_code}在{end_date}的股票市值数据")
                return float('nan')
                
        except Exception as e:
            logger.error(f"查询股票市值失败: {e}")
            return float('nan')

    def get_total_shares(self, stock_code: str, end_date: str) -> float:
        """
        获取总股本
        
        Args:
            stock_code: 股票代码
            end_date: 结束日期，格式为'YYYY-MM-DD'
            
        Returns:
            float: 总股本（单位：股），如果查询失败返回NaN
        """
        try:
            # 查询shares表获取总股本数
            sql = """
            SELECT Nshrttl 
            FROM shares 
            WHERE Stkcd = %s AND Reptdt <= %s order by Reptdt desc limit 1
            """
            
            self.cursor.execute(sql, (stock_code, end_date))
            result = self.cursor.fetchone()
            
            if result:
                total_shares = float(result['Nshrttl'])
                return total_shares
            else:
                logger.warning(f"未找到股票{stock_code}在{end_date}的总股本数据")
                return float('nan')
                
        except Exception as e:
            logger.error(f"查询总股本失败: {e}")
            return float('nan')

    def get_latest_close_price(self, stock_code: str, end_date: str) -> float:
        """
        获取最新收盘价
        
        Args:
            stock_code: 股票代码
            end_date: 结束日期，格式为'YYYY-MM-DD'
            
        Returns:
            float: 最新收盘价（单位：元），如果查询失败返回NaN
        """
        try:
            # 查询trade表获取收盘价
            sql = """
            SELECT Clsprc 
            FROM trade 
            WHERE Stkcd = %s AND Trddt <= %s order by Trddt desc limit 1
            """
            
            self.cursor.execute(sql, (stock_code, end_date))
            result = self.cursor.fetchone()
            
            if result:
                close_price = float(result['Clsprc'])
                return close_price
            else:
                logger.warning(f"未找到股票{stock_code}在{end_date}的收盘价数据")
                return float('nan')
                
        except Exception as e:
            logger.error(f"查询收盘价失败: {e}")
            return float('nan')

    def get_five_year_avg_pe(self, stock_code: str, end_date: str) -> float:
        """
        获取五年平均市盈率
        
        Args:
            stock_code: 股票代码
            
        Returns:
            float: 五年平均市盈率
        """
        try:
            # 查询市场值
            sql_cap = """
            SELECT
            (t.Clsprc * s.Nshrttl) as cap
            from ( SELECT Clsprc,Stkcd FROM trade 
                    WHERE Stkcd = %s AND Trddt <= %s order by Trddt desc limit 1) t join
                 ( SELECT Nshrttl,Stkcd FROM shares 
                    WHERE Stkcd = %s AND Reptdt <= %s order by Reptdt desc limit 1) s
            on t.Stkcd = s.Stkcd;
            """ 
            # 查询净利润
            sql_profit = """
            select sum(ParNetProfit) as sumProfit from (SELECT ParNetProfit from profit p where p.Stkcd = %s and Accper <= %s order by Accper desc limit 4) as ParNetProfit
            """
            pe = 0
            for year in range(5):
                year = int(end_date[:4]) - year
                month_day = end_date[5:]
                start_date = f"{year}-{month_day}"
                self.cursor.execute(sql_cap, (stock_code, start_date, stock_code, start_date))
                cap_result = self.cursor.fetchone()
                self.cursor.execute(sql_profit, (stock_code, start_date))
                profit_result = self.cursor.fetchone()
                
                if cap_result and profit_result:
                    cap = float(cap_result['cap'])
                    profit = float(profit_result['sumProfit'])
                    pe = pe + (cap / profit)
                else:
                    logger.warning(f"未找到股票{stock_code}在{end_date}五年平均市盈率数据")
                    return float('nan')
            return (pe/5)        
        except Exception as e:
            logger.error(f"查询股票市值失败: {e}")
            return float('nan')      

class MillerValueStrategy:
    """米勒价值投资策略实现类"""
    
    def _col(self, df: pd.DataFrame, names: list) -> pd.Series:
        """
        从DataFrame中按列名列表查找列，返回第一个存在的列
        
        Args:
            df: 数据框
            names: 列名列表，按优先级排序
            
        Returns:
            pd.Series: 找到的列数据，如果都不存在则返回空Series
        """
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series(dtype=float)

    def _safe_mean(self, s: pd.Series) -> float:
        """
        安全计算Series的平均值，处理空值和异常情况
        
        Args:
            s: 数据序列
            
        Returns:
            float: 平均值，如果序列为空则返回NaN
        """
        s = s.dropna()
        if len(s) == 0:
            return np.nan
        return float(np.mean(s))

    def _safe_max(self, s: pd.Series) -> float:
        """
        安全计算Series的平均值，处理空值和异常情况
        
        Args:
            s: 数据序列
            
        Returns:
            float: 平均值，如果序列为空则返回NaN
        """
        s = s.dropna()
        if len(s) == 0:
            return np.nan
        return float(np.max(s))

    def _safe_min(self, s: pd.Series) -> float:
        """
        安全计算Series的平均值，处理空值和异常情况
        
        Args:
            s: 数据序列
            
        Returns:
            float: 平均值，如果序列为空则返回NaN
        """
        s = s.dropna()
        if len(s) == 0:
            return np.nan
        return float(np.min(s))

    def _ttm_sum(self, s: pd.Series, n: int = 4) -> float:
        """
        计算最近n个季度的TTM（滚动年度）总和
        
        Args:
            s: 数据序列
            n: 季度数，默认4个季度（一年）
            
        Returns:
            float: TTM总和，如果数据不足则返回NaN
        """
        # 将字符串转换为浮点数
        s = pd.to_numeric(s, errors='coerce')
        s = s.tail(n).dropna()
        if len(s) == 0:
            return np.nan
        return float(np.sum(s))

    def compute_tax_rate_quarterly(self, income_df: pd.DataFrame) -> pd.Series:
        """
        计算季度所得税率
        
        Args:
            income_df: 利润表数据
            
        Returns:
            pd.Series: 季度所得税率序列，限制在0-1之间
        """
        tax = self._col(income_df, ['所得税费用', 'IncomeTax'])
        total_profit = self._col(income_df, ['利润总额', 'ProfitBefTax'])
        # 将字符串转换为浮点数
        tax = pd.to_numeric(tax, errors='coerce')
        total_profit = pd.to_numeric(total_profit, errors='coerce')
        r = tax / total_profit.replace(0, np.nan)
        return r.clip(lower=0.0, upper=1.0)

    def compute_roc_quarterly(self, income_df: pd.DataFrame, balance_df: pd.DataFrame) -> pd.Series:
        """
        计算季度资本回报率（Return on Capital）
        
        Args:
            income_df: 利润表数据
            balance_df: 资产负债表数据
            
        Returns:
            pd.Series: 季度ROC序列
        """
        ebit = self._col(income_df, ['营业利润', 'EBIT', 'OpProfit'])
        # 将字符串转换为浮点数
        ebit = pd.to_numeric(ebit, errors='coerce')
        tax_rate = self.compute_tax_rate_quarterly(income_df)
        nopat = ebit * (1.0 - tax_rate)
        equity = self._col(balance_df, ['归属于母公司所有者权益', 'ParOwnEquity'])
        # 将字符串转换为浮点数
        equity = pd.to_numeric(equity, errors='coerce')
        std_loan = self._col(balance_df, ['短期借款', 'ShortBorrow'])
        # 将字符串转换为浮点数
        std_loan = pd.to_numeric(std_loan, errors='coerce')
        cur_nc_liab = self._col(balance_df, ['一年内到期非流动负债', 'NonCurLia1Y'])
        # 将字符串转换为浮点数
        cur_nc_liab = pd.to_numeric(cur_nc_liab, errors='coerce')
        long_loan = self._col(balance_df, ['长期借款', 'LTBorrow'])
        # 将字符串转换为浮点数
        long_loan = pd.to_numeric(long_loan, errors='coerce')   
        bonds = self._col(balance_df, ['应付债券', 'BondPay'])
        # 将字符串转换为浮点数
        bonds = pd.to_numeric(bonds, errors='coerce')
        invested = equity + std_loan + cur_nc_liab + long_loan + bonds
        roc = nopat / invested.replace(0, np.nan)
        return roc

    def compute_rooc_quarterly(self, income_df: pd.DataFrame, balance_df: pd.DataFrame) -> pd.Series:
        """
        计算季度营运资本回报率（Return on Operating Capital）
        
        Args:
            income_df: 利润表数据
            balance_df: 资产负债表数据
            
        Returns:
            pd.Series: 季度ROOC序列
        """
        ebit = self._col(income_df, ['营业利润', 'EBIT', 'OpProfit'])
        # 将字符串转换为浮点数
        ebit = pd.to_numeric(ebit, errors='coerce')
        tax_rate = self.compute_tax_rate_quarterly(income_df)
        nopat = ebit * (1.0 - tax_rate)
        # 将字符串转换为浮点数
        ar = pd.to_numeric(self._col(balance_df, ['应收账款', 'AcctRecNet']), errors='coerce')
        prepay = pd.to_numeric(self._col(balance_df, ['预付账款', 'PrepayNet']), errors='coerce')
        inv = pd.to_numeric(self._col(balance_df, ['存货', 'InventNet']), errors='coerce')
        nr = pd.to_numeric(self._col(balance_df, ['应收票据', 'NotesRecNet']), errors='coerce')
        ap = pd.to_numeric(self._col(balance_df, ['应付账款', 'AcctPay']), errors='coerce')
        adv = pd.to_numeric(self._col(balance_df, ['预收账款', 'AdvFromCust']), errors='coerce')
        npay = pd.to_numeric(self._col(balance_df, ['应付票据', 'NotesPay']), errors='coerce')
        payroll = pd.to_numeric(self._col(balance_df, ['应付职工薪酬', 'EmpBenefitPay']), errors='coerce')
        taxpay = pd.to_numeric(self._col(balance_df, ['应交税费', 'TaxPay']), errors='coerce')
        
        nowc = (ar + prepay + inv + nr) - (ap + adv + npay + payroll + taxpay)
        rooc = nopat / nowc.replace(0, np.nan)
        return rooc

    def compute_inventory_turnover_quarterly(self, income_df: pd.DataFrame, balance_df: pd.DataFrame) -> pd.Series:
        """
        计算季度存货周转率
        
        Args:
            income_df: 利润表数据
            balance_df: 资产负债表数据
            
        Returns:
            pd.Series: 季度存货周转率序列
        """
        cogs = self._col(income_df, ['营业成本', 'OpCost'])
        # 将字符串转换为浮点数
        cogs = pd.to_numeric(cogs, errors='coerce')
        inv = pd.to_numeric(self._col(balance_df, ['存货', 'InventNet']), errors='coerce')
        avg_inv = (inv.shift(1) + inv) / 2.0
        it = cogs / avg_inv.replace(0, np.nan)
        return it
        
    def compute_pb_latest(self, price: float, equity: float, shares: float) -> float:
        """
        计算最新市净率（Price-to-Book Ratio）
        
        Args:
            price: 最新股价
            equity: 归属于母公司所有者权益
            shares: 总股本
            
        Returns:
            float: 市净率，如果数据无效则返回NaN
        """
        if shares is None or shares == 0:
            return np.nan
        bvps = equity / shares
        if bvps == 0:
            return np.nan
        return float(price / bvps)

    def compute_pe_ttm(self, market_cap: float, net_profit_series: pd.Series) -> float:
        """
        计算TTM市盈率（Price-to-Earnings Ratio）
        
        Args:
            market_cap: 市值
            net_profit_series: 净利润序列
            
        Returns:
            float: TTM市盈率，如果数据无效则返回NaN
        """
        np_ttm = self._ttm_sum(net_profit_series, 4)
        if np.isnan(np_ttm) or np_ttm == 0:
            return np.nan
        return float(market_cap / np_ttm)

    def compute_pfcf_ttm(self, market_cap: float, cfo_series: pd.Series, capex_series: pd.Series) -> float:
        """
        计算TTM市现率（Price-to-Free-Cash-Flow Ratio）
        
        Args:
            market_cap: 市值
            cfo_series: 经营活动现金流序列
            capex_series: 资本支出序列
            
        Returns:
            float: TTM市现率，如果数据无效则返回NaN
        """
        fcf_series = pd.to_numeric(cfo_series, errors='coerce') - pd.to_numeric(capex_series, errors='coerce')
        fcf_ttm = self._ttm_sum(fcf_series, 4)
        if np.isnan(fcf_ttm) or fcf_ttm == 0:
            return np.nan
        return float(market_cap / fcf_ttm)

    def discounted_10y_fcf(self, stock_code: str, end_date: str, discount_rate: float, growth_rate: float = 0.0) -> float:
        """
        计算10年自由现金流折现值
        
        Args:
            fcf_base: 基础自由现金流
            discount_rate: 折现率
            growth_rate: 增长率，默认0%
            
        Returns:
            float: 10年自由现金流折现值，如果折现率无效则返回NaN
        """
        sql = """
            select 
                sum((NetOpCF - AssetPurchase) * 
                    pow((1.0 + %s), year(%s)-year(Accper))/
                    pow((1.0 + %s), year(%s)-year(Accper))) as free_cash 
            from cash_flow 
            where Stkcd=%s and Accper like "%12-31" 
            order by Accper desc limit 10;
        """
        try:
        # 执行查询
            self.cursor.execute(sql, (growth_rate, end_date ,discount_rate, end_date, stock_code))
            result = self.cursor.fetchone()

            # 返回单个浮点数值
            if result and result['free_cash'] is not None:
                fcf_10y = float(result['free_cash'])
                return fcf_10y
            else:
                logger.warning(f"未找到{end_date}fcf_10y")
                return np.nan
        except Exception as e:
            logger.error(f"查询{end_date}的roc_avg数据时出错: {e}")
            return np.nan

    def evaluate_selection(self, stock_code: str, repo: FinancialDataRepository, end_date: str) -> bool:
        """
        评估股票是否符合选股标准
        
        Args:
            stock_code: 股票代码
            repo: 财务数据仓库实例
            
        Returns:
            dict: 包含各项指标和评估结果的字典
        """
        profit = repo.get_profit_quarterly(stock_code, 12, end_date)
        balance = repo.get_balance_quarterly(stock_code, 12, end_date)
        roc_q = self.compute_roc_quarterly(profit, balance)
        rooc_q = self.compute_rooc_quarterly(profit, balance)
        it_q = self.compute_inventory_turnover_quarterly(profit, balance)
        roc_4_min = self._safe_min(roc_q.tail(4))
        roc_5_8_max = self._safe_max(roc_q.tail(8).head(4))
        rooc_4_min = self._safe_min(rooc_q.tail(4))
        rooc_5_8_max = self._safe_max(rooc_q.tail(8).head(4))
        it_4_min = self._safe_min(it_q.tail(4))
        it_5_8_max = self._safe_max(it_q.tail(8).head(4))
        avg_roc = repo.get_roc_avg(end_date)
        deposit = repo.get_deposit_rate()
        avg_rooc = repo.get_rooc_avg(end_date)
        industry_it = repo.get_avg_inventory_turnover(end_date)
        cA = roc_4_min > avg_roc if not np.isnan(roc_4_min) and avg_roc is not None else False
        cB = roc_4_min > deposit if not np.isnan(roc_4_min) and deposit is not None else False
        cC = roc_4_min > roc_5_8_max if not np.isnan(roc_4_min) and not np.isnan(roc_5_8_max) else False
        cD = rooc_4_min > avg_rooc if not np.isnan(rooc_4_min) and avg_rooc is not None else False
        cE = rooc_4_min > rooc_5_8_max if not np.isnan(rooc_4_min) and not np.isnan(rooc_5_8_max) else False
        cF = it_4_min > industry_it if not np.isnan(it_4_min) and industry_it is not None else False
        cG = it_4_min > it_5_8_max if not np.isnan(it_4_min) and not np.isnan(it_5_8_max) else False
        return (cA and cB and cC and cD and cE and cF and cG)

    def evaluate_buy(self, stock_code: str, repo: FinancialDataRepository, end_date: str) -> dict:
        """
        评估股票是否符合买入标准
        
        Args:
            stock_code: 股票代码
            repo: 财务数据仓库实例
            
        Returns:
            dict: 包含各项估值指标和买入条件的字典
        """
        income = repo.get_profit_quarterly(stock_code, 12, end_date)
        balance = repo.get_balance_quarterly(stock_code, 12, end_date)
        cash = repo.get_cashflow_quarterly(stock_code, 12, end_date)
        market_cap = repo.get_market_cap(stock_code, end_date)
        shares = repo.get_total_shares(stock_code, end_date)
        price = repo.get_latest_close_price(stock_code, end_date)
        equity_latest = self._col(balance, ['归属于母公司所有者权益', 'ParOwnEquity']).tail(1)
        net_profit = self._col(income, ['净利润', '归属于母公司净利润', 'ParNetProfit'])
        cfo = self._col(cash, ['经营活动现金流量净额', 'NetOpCF'])
        capex = self._col(cash, ['购建固定资产、无形资产和其他长期资产支付的现金', 'AssetPurchase'])
        pb_latest = np.nan
        if not equity_latest.empty:
            pb_latest = self.compute_pb_latest(price, float(equity_latest.iloc[-1]), shares)
        pe_ttm = self.compute_pe_ttm(market_cap, net_profit)
        pfcf_ttm = self.compute_pfcf_ttm(market_cap, cfo, capex)
        market_pb_avg = repo.get_pb_avg(end_date)
        deposit = repo.get_deposit_rate(end_date)
        infl = repo.get_inflation_rate(end_date)
        five_year_avg_pe = repo.get_five_year_avg_pe(stock_code, end_date)
        discount = repo.get_discount_rate()
        # 将字符串转换为数值类型再进行计算
        # cfo_numeric = pd.to_numeric(cfo, errors='coerce')
        # capex_numeric = pd.to_numeric(capex, errors='coerce')
        # fcf_base = self._ttm_sum(cfo_numeric - capex_numeric, 2)
        dcf_10y = self.discounted_10y_fcf(stock_code, end_date, discount, 0.0)
        condA = pb_latest < 2.0 * market_pb_avg if market_pb_avg is not None and not np.isnan(pb_latest) else False
        condB = pe_ttm < (1.0 / deposit) if deposit is not None and not np.isnan(pe_ttm) and deposit > 0 else False
        condC = pb_latest < five_year_avg_pe if not np.isnan(pb_latest) and five_year_avg_pe is not None else False
        condD = pfcf_ttm < (1.0 / infl) if infl is not None and not np.isnan(pfcf_ttm) and infl > 0 else False
        condE = (market_cap / dcf_10y) < 1.0 if dcf_10y is not None and not np.isnan(dcf_10y) and dcf_10y > 0 else False
        count = int(condA) + int(condB) + int(condC) + int(condD) + int(condE)
        return count,price

class MillerStrategyRunner:
    """米勒策略执行器类"""
    
    def __init__(self, repo: FinancialDataRepository, end_date: str = None):
        """
        初始化策略执行器
        
        Args:
            repo: 财务数据仓库实例
        """
        self.repo = repo
        self.end_date = end_date
        self.strategy = MillerValueStrategy()

    def get_stock_codes(self) -> List[str]:
        """从stock.balance表获取所有股票代码"""
        try:
            with self.repo.connection.cursor() as cursor:
                sql = "SELECT DISTINCT Stkcd FROM stock1.balance WHERE Stkcd IS NOT NULL AND Stkcd != ''"
                cursor.execute(sql)
                results = cursor.fetchall()
                
                stock_codes = [row['Stkcd'] for row in results]
                return stock_codes
                
        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []
        
    def get_sel_codes(self, end_date: str) -> List[str]:
        """从stock.balance表获取所有股票代码"""
        try:
            with self.repo.connection.cursor() as cursor:
                sql = "SELECT DISTINCT Stkcd FROM stock1.sel_stocks WHERE end_date = " \
                "(select end_date from stock1.sel_stocks where end_date <= %s order by end_date desc limit 1)"
                cursor.execute(sql, (end_date,))
                results = cursor.fetchall()
                
                stock_codes = [row['Stkcd'] for row in results]
                return stock_codes
                
        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []

    def insert_trade_stock(self, stock_code: str, end_date: str, price: str, op:str, score: int) -> bool:
        """
        向sel_stock表中写入一行记录
        
        Args:
            stock_code: 股票代码
            end_date: 截止日期（格式：YYYY-MM-DD）
            
        Returns:
            bool: 插入成功返回True，失败返回False
        """
        try:
            with self.repo.connection.cursor() as cursor:
                # 构建SQL插入语句
                """
                create table trade_stocks (
                    Stkcd varchar(30),
                    end_date date,
                    price decimal(10, 2),
                    op varchar(2),
                    score varchar(2),
                    primary key(Stkcd, end_date)
                );
                """
                sql = """
                INSERT INTO trade_stocks(Stkcd, end_date, price, op, score) 
                VALUES (%s, %s, %s, %s, %s)
                """
                
                # 执行插入操作
                cursor.execute(sql, (stock_code, end_date, price, op, score))
                self.repo.connection.commit()
                logger.info(f"向trade_stock表插入记录成功：股票代码={stock_code}, 日期={end_date}, 价格={price}, op={op}, score={score}")
                return True
        except Exception as e:
            logger.error(f"向trade_stock表插入记录失败：股票代码={stock_code}, 日期={end_date}, 错误={e}")
            self.repo.connection.rollback()
            return False
                
    def insert_sel_stock(self, stock_code: str, end_date: str) -> bool:
        """
        向sel_stock表中写入一行记录
        
        Args:
            stock_code: 股票代码
            end_date: 截止日期（格式：YYYY-MM-DD）
            
        Returns:
            bool: 插入成功返回True，失败返回False
        """
        """
        CREATE TABLE `sel_stocks` (
            `Stkcd` varchar(100) NOT NULL COMMENT '证券代码',
            `end_date` date NOT NULL COMMENT '截止日期'
        )
        """
        try:
            with self.repo.connection.cursor() as cursor:
                # 构建SQL插入语句
                sql = """
                INSERT INTO sel_stocks(Stkcd, end_date) 
                VALUES (%s, %s)
                """
                
                # 执行插入操作
                cursor.execute(sql, (stock_code, end_date))
                self.repo.connection.commit()
                return True
                
        except Exception as e:
            logger.error(f"向sel_stock表插入记录失败：股票代码={stock_code}, 截止日期={end_date}, 错误={e}")
            self.repo.connection.rollback()
            return False
    
    def sel_for_stocks(self, end_date: str) -> dict:
        """
        对单个股票执行米勒价值投资策略
        
        Args:
            stock_code: 股票代码
            
        Returns:
            dict: 包含选股评估和买入评估结果的字典
        """
        stock_codes = self.get_stock_codes()
        for stock_code in (stock_codes):
            sel = self.strategy.evaluate_selection(stock_code, self.repo, end_date)
            logger.info(f"股票代码={stock_code}, 截止日期={end_date}, 评估结果={sel}")
            if sel:
                self.insert_sel_stock(stock_code, end_date)

    def buy_for_stocks(self, end_date: str):
        """
        对单个股票执行米勒价值投资策略
        
        Args:
            stock_code: 股票代码
            
        Returns:
            dict: 包含选股评估和买入评估结果的字典
        """ 
        stock_codes = self.get_sel_codes(end_date)
        for i, stock_code in enumerate(stock_codes, 1):
            sel,price = self.strategy.evaluate_buy(stock_code, self.repo, end_date=end_date)
            logger.info(f"股票代码={stock_code}, 截止日期={end_date}, 评估结果={sel}, 价格={price}")
            if sel >= 3:
                self.insert_trade_stock(stock_code, end_date, price, '1', sel)
            elif sel <= 1:
                self.insert_trade_stock(stock_code, end_date, price, '0', sel)

    def plot_trade_stocks(self, save_path: str = None):
        """
        从trade_stocks表读取数据并绘制图表
        
        Args:
            save_path: 图片保存路径，如果为None则不保存
        """
        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            from datetime import datetime
            
            # 从trade_stocks表读取数据
            with self.repo.connection.cursor() as cursor:
                sql = """
                SELECT Stkcd, end_date, price, op 
                FROM trade_stocks 
                ORDER BY Stkcd, end_date
                """
                cursor.execute(sql)
                results = cursor.fetchall()
            
            if not results:
                logger.warning("trade_stocks表中没有数据")
                return
            
            # 按股票代码分组数据
            stock_data = {}
            for row in results:
                stock_code = row['Stkcd']
                if stock_code not in stock_data:
                    stock_data[stock_code] = {'dates': [], 'prices': [], 'op_0_dates': [], 'op_0_prices': []}
                
                # 转换日期格式
                date_obj = row['end_date']
                if isinstance(date_obj, str):
                    date_obj = datetime.strptime(date_obj, '%Y-%m-%d')
                
                price = float(row['price'])
                op = row['op']
                
                stock_data[stock_code]['dates'].append(date_obj)
                stock_data[stock_code]['prices'].append(price)
                
                # 记录op为0的数据点
                if op == '0':
                    stock_data[stock_code]['op_0_dates'].append(date_obj)
                    stock_data[stock_code]['op_0_prices'].append(price)
            
            # 创建图表
            plt.figure(figsize=(12, 8))
            
            # 生成不同的颜色
            colors = plt.cm.Set3(np.linspace(0, 1, len(stock_data)))
            
            # 为每个股票绘制数据线
            for i, (stock_code, data) in enumerate(stock_data.items()):
                if len(data['dates']) > 1:  # 只有多个数据点才绘制连线
                    plt.plot(data['dates'], data['prices'], 
                            label=stock_code, color=colors[i], linewidth=2, marker='o', markersize=4)
                else:  # 单个数据点只绘制散点
                    plt.scatter(data['dates'], data['prices'], 
                               label=stock_code, color=colors[i], s=80)
                
                # 标记op为0的数据点
                if data['op_0_dates']:
                    plt.scatter(data['op_0_dates'], data['op_0_prices'], 
                               color='red', s=100, marker='X', zorder=5, 
                               label=f'{stock_code} (op=0)' if len(data['dates']) <= 1 else None)
            
            # 设置图表属性
            plt.title('Trade Stocks Price Trend', fontsize=16, fontweight='bold')
            plt.xlabel('Date', fontsize=12)
            plt.ylabel('Price', fontsize=12)
            
            # 格式化x轴日期显示
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.gcf().autofmt_xdate()  # 自动旋转日期标签
            
            # 添加图例
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # 添加网格
            plt.grid(True, alpha=0.3)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存或显示图表
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"图表已保存到: {save_path}")
            
            plt.show()
            
        except ImportError:
            logger.error("matplotlib库未安装，无法绘制图表")
            print("请安装matplotlib: pip install matplotlib")
        except Exception as e:
            logger.error(f"绘制图表失败: {e}")
            raise

if __name__ == '__main__':
    
    repo = FinancialDataRepository(DB_CONFIG)
    repo.connect()
    runner = MillerStrategyRunner(repo)
    # runner.strategy.evaluate_selection("300529", repo, "2023-12-31")
    # runner.sel_for_stocks('2024-03-31')
    
    dates = ['2023-12-31','2024-03-31','2024-06-30','2024-09-30','2024-12-31','2025-03-31','2025-06-30','2025-09-30']

    for end_date in dates:
        runner.sel_for_stocks(end_date)
    '''
    start_date = '2024-01-01'

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    date_list = []
    
    for i in range(90):
        current_date = start_dt + timedelta(days=i)
        date_list.append(current_date.strftime('%Y-%m-%d'))
    
    print(f"从{start_date}开始的90天日期列表:")
    for i, date in enumerate(date_list, 1):
        print(f"第{i}天: {date}")
    
    # 示例：对每个日期执行策略
    for end_date in date_list:
        print(f"\n处理日期: {end_date}")
        runner.buy_for_stocks(end_date)
    '''
    # 绘制trade_stocks数据图表
    # print("\n开始绘制trade_stocks数据图表...")
    # runner.plot_trade_stocks(save_path='trade_stocks_chart.png') 
