#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票现金流量表数据获取与处理脚本
功能：
1. 连接MySQL数据库实例
2. 从stock.balance表获取Stkcd列的全部值
3. 使用akshare获取每个股票代码的现金流量表数据
4. 筛选报告日以'0930'结尾的行并写入stock.cash_flow表
"""

import pymysql
import akshare as ak
import pandas as pd
import time
import logging
from datetime import datetime
from typing import List, Optional
from miller_value import FinancialDataRepository, MillerStrategyRunner

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_cash_flow.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockCashFlowProcessor:
    """股票现金流量表数据处理类"""
    
    def __init__(self, db_config: dict):
        """
        初始化数据库连接
        
        Args:
            db_config: 数据库连接配置字典
        """
        self.db_config = db_config
        self.connection = None
        
    def connect_to_mysql(self) -> bool:
        """连接MySQL数据库"""
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
            logger.info("成功连接到MySQL数据库")
            return True
        except Exception as e:
            logger.error(f"连接MySQL数据库失败: {e}")
            return False
    
    def get_stock_codes(self) -> List[str]:
        """从stock.balance表获取所有股票代码"""
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT DISTINCT Stkcd FROM stock.balance WHERE Stkcd IS NOT NULL AND Stkcd != ''"
                cursor.execute(sql)
                results = cursor.fetchall()
                
                stock_codes = [row['Stkcd'] for row in results]
                logger.info(f"从balance表获取到 {len(stock_codes)} 个股票代码")
                return stock_codes
                
        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []
    
    def get_cash_flow_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        """使用akshare获取指定股票代码的现金流量表数据"""
        try:
            # 使用akshare获取现金流量表数据
            df = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")
            
            if df.empty:
                logger.warning(f"股票代码 {stock_code} 的现金流量表数据为空")
                return None
            
            # 添加股票代码列
            df['stock_code'] = stock_code
            df['update_time'] = datetime.now()
            
            logger.info(f"成功获取股票代码 {stock_code} 的现金流量表数据，共 {len(df)} 行")
            return df
            
        except Exception as e:
            logger.error(f"获取股票代码 {stock_code} 的现金流量表数据失败: {e}")
            return None
    
    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选报告日以'0930'或'0331'结尾的数据"""
        try:
            # 假设报告日列名为'报告日'或'report_date'
            date_column = None
            for col in df.columns:
                if '报告日' in col or 'report_date' in col.lower():
                    date_column = col
                    break
            
            if date_column is None:
                logger.warning("未找到报告日列")
                return pd.DataFrame()
            
            # 筛选以'0930'或'0331'结尾的报告日
            date_str = df[date_column].astype(str)
            filtered_df = df[date_str.str.endswith('0930') | date_str.str.endswith('0331')]
            
            logger.info(f"筛选出 {len(filtered_df)} 条报告日以'0930'或'0331'结尾的数据")
            return filtered_df
            
        except Exception as e:
            logger.error(f"筛选数据失败: {e}")
            return pd.DataFrame()
    
    def insert_cash_flow_data(self, df: pd.DataFrame):
        """将现金流量表数据插入到数据库"""
        try:
            if df.empty:
                return
            
            with self.connection.cursor() as cursor:
                # 批量插入数据
                insert_sql = """
                insert into cash_flow(Stkcd, Accper, NetOpCF, AssetPurchase) 
                VALUES (%s, %s, %s, %s)
                """
                
                # 准备数据
                data_to_insert = []
                for _, row in df.iterrows():
                    # 假设数据格式：需要根据akshare返回的实际格式调整
                    # 这里需要根据akshare返回的列名进行调整
                    report_date_col = None
                    NetOpCF_col = None
                    AssetPurchase_col = None
                    
                    # 尝试找到合适的列名
                    for col in df.columns:
                        if '报告日' in col:
                            report_date_col = col
                        elif '经营活动产生的现金流量' in col:
                            NetOpCF_col = col
                        elif '购建固定资产、无形资产和其他长期资产支付的现金' in col:
                            AssetPurchase_col = col
                    
                    if report_date_col and NetOpCF_col:
                        data_to_insert.append((
                            row['stock_code'],
                            str(row[report_date_col]),
                            str(row[NetOpCF_col]),
                            float(row[AssetPurchase_col]) if pd.notna(row[AssetPurchase_col]) else 0
                        ))
                
                if data_to_insert:
                    cursor.executemany(insert_sql, data_to_insert)
                    self.connection.commit()
                    logger.info(f"成功插入 {len(data_to_insert)} 条现金流量表数据")
                
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            self.connection.rollback()
    
    def process_all_stocks(self, delay: float = 1.0):
        """处理所有股票代码"""        
        # 获取股票代码
        stock_codes = self.get_stock_codes()
        
        if not stock_codes:
            logger.error("未获取到股票代码，程序终止")
            return
        
        total_processed = 0
        total_inserted = 0
        
        for i, stock_code in enumerate(stock_codes, 1):
            try:
                logger.info(f"处理第 {i}/{len(stock_codes)} 个股票代码: {stock_code}")
                
                # 获取现金流量表数据
                cash_flow_df = self.get_cash_flow_data(stock_code)
                
                if cash_flow_df is not None and not cash_flow_df.empty:
                    
                    # 筛选0930或0331数据
                    filtered_df = self.filter_data(cash_flow_df)
                    
                    if not filtered_df.empty:
                        # 插入数据库
                        self.insert_cash_flow_data(filtered_df)
                        total_inserted += len(filtered_df)
                    
                    total_processed += 1
                
                # 添加延迟避免请求过于频繁
                if i < len(stock_codes):
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"处理股票代码 {stock_code} 时发生错误: {e}")
                continue
        
        logger.info(f"处理完成！共处理 {total_processed} 个股票，插入 {total_inserted} 条数据")
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def create_miller_result_table(self):
        try:
            with self.connection.cursor() as cursor:
                sql = """
                CREATE TABLE IF NOT EXISTS stock.miller_value_result (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    roc_4 DECIMAL(20,6),
                    rooc_4 DECIMAL(20,6),
                    it_4 DECIMAL(20,6),
                    roc_5_8 DECIMAL(20,6),
                    rooc_5_8 DECIMAL(20,6),
                    it_5_8 DECIMAL(20,6),
                    sel_A TINYINT,
                    sel_B TINYINT,
                    sel_C TINYINT,
                    sel_D TINYINT,
                    sel_E TINYINT,
                    sel_F TINYINT,
                    sel_G TINYINT,
                    pb_latest DECIMAL(20,6),
                    pe_ttm DECIMAL(20,6),
                    pfcf_ttm DECIMAL(20,6),
                    dcf_10y DECIMAL(20,6),
                    buy_A TINYINT,
                    buy_B TINYINT,
                    buy_C TINYINT,
                    buy_D TINYINT,
                    buy_E TINYINT,
                    buy_count INT,
                    buy_flag TINYINT,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_code (stock_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(sql)
                self.connection.commit()
                logger.info("stock.miller_value_result表创建/检查完成")
        except Exception as e:
            logger.error(f"创建miller结果表失败: {e}")

    def insert_miller_result(self, result: dict):
        try:
            with self.connection.cursor() as cursor:
                sql = """
                INSERT INTO stock.miller_value_result (
                    stock_code, roc_4, rooc_4, it_4, roc_5_8, rooc_5_8, it_5_8,
                    sel_A, sel_B, sel_C, sel_D, sel_E, sel_F, sel_G,
                    pb_latest, pe_ttm, pfcf_ttm, dcf_10y,
                    buy_A, buy_B, buy_C, buy_D, buy_E, buy_count, buy_flag
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s
                )
                """
                s = result['selection']
                b = result['buy']
                args = (
                    result['stock_code'], s['roc_4'], s['rooc_4'], s['it_4'], s['roc_5_8'], s['rooc_5_8'], s['it_5_8'],
                    int(s['A']), int(s['B']), int(s['C']), int(s['D']), int(s['E']), int(s['F']), int(s['G']),
                    b['pb_latest'], b['pe_ttm'], b['pfcf_ttm'], b['dcf_10y'],
                    int(b['A']), int(b['B']), int(b['C']), int(b['D']), int(b['E']), b['count'], int(b['buy'])
                )
                cursor.execute(sql, args)
                self.connection.commit()
        except Exception as e:
            logger.error(f"插入miller结果失败: {e}")
            self.connection.rollback()

    def run_miller_value(self, limit: Optional[int] = None, delay: float = 1.0):
        codes = self.get_stock_codes()
        if limit is not None:
            codes = codes[:limit]
        repo = FinancialDataRepository(self.db_config)
        if not repo.connect():
            logger.error("无法连接数据库用于Miller策略")
            return
        runner = MillerStrategyRunner(repo)
        self.create_miller_result_table()
        for i, code in enumerate(codes, 1):
            try:
                r = runner.run_for_stock(code)
                self.insert_miller_result(r)
                if i < len(codes):
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"计算{code}失败: {e}")
        repo.close()

def main():
    """主函数"""
    
    
    try:
        # 导入配置文件
        from config import DB_CONFIG, PROCESS_CONFIG
        
        # 创建处理器实例
        processor = StockCashFlowProcessor(DB_CONFIG)
        processor.connect_to_mysql()
        processor.process_all_stocks(delay=1000)
        
    except ImportError:
        logger.error("配置文件config.py不存在，请先创建配置文件")
        # 使用默认配置
        db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'your_password',  # 请修改为实际密码
            'database': 'stock'
        }
        
        processor = StockCashFlowProcessor(db_config)
        
        processor.process_all_stocks(delay=1000)
        
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
    finally:
        # 关闭连接
        if 'processor' in locals() and not args.no_db:
            processor.close_connection()

if __name__ == "__main__":
    main()
