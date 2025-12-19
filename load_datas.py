#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV数据加载器
将外部CSV文件数据导入到MySQL数据库表中
"""

import csv
import pymysql
import logging
import argparse
import os
import sys
from typing import List, Dict, Any
from config import DB_CONFIG
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CSVDataLoader:
    """CSV数据加载器类"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化数据加载器
        
        Args:
            db_config: 数据库配置字典
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        连接到MySQL数据库
        
        Returns:
            bool: 连接成功返回True，失败返回False
        """
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            logger.info("成功连接到MySQL数据库")
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
            logger.info("数据库连接已关闭")
    
    def process_value(self, value: str) -> str:
        """
        处理单个数据值
        
        Args:
            value: 原始数据值
            
        Returns:
            str: 处理后的数据值
        """
        if value is None:
            return '0'
        
        # 去掉两边的引号
        processed_value = value.strip().strip('"')
        processed_value = value.strip().strip("'")
        
        # 如果是空字符串，返回'0'
        if processed_value == '':
            return '0'
        
        return processed_value
    
    def process_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        处理单行数据
        
        Args:
            row: 原始行数据字典
            
        Returns:
            Dict[str, str]: 处理后的行数据字典
        """
        processed_row = {}
        for key, value in row.items():
            processed_row[key] = self.process_value(value)
        return processed_row
    
    def read_csv_file(self, csv_file_path: str) -> List[Dict[str, str]]:
        """
        读取CSV文件
        
        Args:
            csv_file_path: CSV文件路径
            
        Returns:
            List[Dict[str, str]]: CSV数据列表
        """
        if not os.path.exists(csv_file_path):
            logger.error(f"CSV文件不存在: {csv_file_path}")
            return []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                # 自动检测CSV文件的编码和分隔符
                sample = file.read(1024)
                file.seek(0)
                
                # 尝试不同的分隔符
                dialect = csv.Sniffer().sniff(sample)
                
                reader = csv.DictReader(file, dialect=dialect)
                data = list(reader)
                
                logger.info(f"成功读取CSV文件，共{len(data)}行数据")
                return data
        except Exception as e:
            logger.error(f"读取CSV文件失败: {e}")
            return []
    
    def read_xlsx_file(self, xlsx_file_path: str, sheet_name: str = 0) -> List[Dict[str, str]]:
        """
        读取Excel文件（.xlsx格式）
        
        Args:
            xlsx_file_path: Excel文件路径
            sheet_name: 工作表名称或索引，默认为第一个工作表
            
        Returns:
            List[Dict[str, str]]: Excel数据列表
        """
        if not os.path.exists(xlsx_file_path):
            logger.error(f"Excel文件不存在: {xlsx_file_path}")
            return []
        
        try:
            # 读取Excel文件
            df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name, dtype=str)
            
            # 将DataFrame转换为字典列表
            data = df.replace({pd.isna: None}).to_dict('records')
            
            # 将None值转换为空字符串
            for row in data:
                for key, value in row.items():
                    if value is None:
                        row[key] = ''
                    else:
                        row[key] = str(value)
            
            logger.info(f"成功读取Excel文件，共{len(data)}行数据")
            return data
            
        except Exception as e:
            logger.error(f"读取Excel文件失败: {e}")
            return []
    
    def create_table_if_not_exists(self, table_name: str, headers: List[str]) -> bool:
        """
        如果表不存在则创建表
        
        Args:
            table_name: 表名
            headers: 表头列表
            
        Returns:
            bool: 创建成功返回True，失败返回False
        """
        try:
            # 检查表是否存在
            self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = self.cursor.fetchone() is not None
            
            if not table_exists:
                # 创建表，所有字段都设为VARCHAR(255)
                columns = [f"`{header}` VARCHAR(255)" for header in headers]
                create_table_sql = f"CREATE TABLE `{table_name}` (\n"
                create_table_sql += ",\n".join(columns)
                create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                
                self.cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info(f"成功创建表: {table_name}")
            else:
                logger.info(f"表已存在: {table_name}")
            
            return True
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            return False
    
    def insert_data(self, table_name: str, data: List[Dict[str, str]]) -> bool:
        """
        插入数据到数据库表
        
        Args:
            table_name: 表名
            data: 数据列表
            
        Returns:
            bool: 插入成功返回True，失败返回False
        """
        if not data:
            logger.warning("没有数据需要插入")
            return True
        
        try:
            headers = list(data[0].keys())
            
            # 准备插入SQL
            placeholders = ", ".join(["%s"] * len(headers))
            columns = ", ".join([f"`{header}`" for header in headers])
            ## insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
            
            # 批量插入数据
            batch_size = 500
            total_rows = len(data)
            
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batch_values = []
                
                for row in batch:
                    values = [row[header] for header in headers]
                    batch_values.append(values)
                
                try:
                    self.cursor.executemany(insert_sql, batch_values)
                    self.connection.commit()
                except Exception as e:
                    logger.error(f"插入数据时发生异常: {e}")
                    self.connection.rollback()
                logger.info(f"已插入 {min(i + batch_size, total_rows)}/{total_rows} 行数据")
            logger.info(f"成功插入所有 {total_rows} 行数据到表 {table_name}")
            return True
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            self.connection.rollback()
            return False
    
    def load_file_to_mysql(self, csv_file_path: str, table_name: str, type: str) -> bool:
        """
        主函数：加载CSV数据到MySQL
        
        Args:
            csv_file_path: CSV文件路径
            table_name: 目标表名
            
        Returns:
            bool: 加载成功返回True，失败返回False
        """
        logger.info(f"开始加载CSV文件: {csv_file_path} 到表: {table_name}")
        
        # 连接到数据库
        if not self.connect():
            return False
        
        try:
            # 读取CSV文件
            if type == 'csv':
                raw_data = self.read_csv_file(csv_file_path)
            elif type == 'xlsx':
                raw_data = self.read_xlsx_file(csv_file_path)
            else:
                logger.error(f"不支持的文件类型: {type}")
                return False
            
            if not raw_data:
                return False
            
            # 处理数据
            processed_data = []
            for i, row in enumerate(raw_data, 1):
                processed_row = self.process_row(row)
                processed_data.append(processed_row)
                
                if i % 100 == 0:
                    logger.info(f"已处理 {i} 行数据")
            
            logger.info(f"数据预处理完成，共处理 {len(processed_data)} 行")
            
            # 插入数据到数据库
            success = self.insert_data(table_name, processed_data)
            
            return success
        
        finally:
            # 确保关闭连接
            self.close()

def load_csv_datas(csv_file, table_name):    
    # 创建数据加载器
    loader = CSVDataLoader(DB_CONFIG)
    # 执行数据加载
    success = loader.load_file_to_mysql(csv_file, table_name, 'csv')
    
    if success:
        logger.info("数据加载完成！")
    else:
        logger.error("数据加载失败！")
        sys.exit(1)

def load_xlsx_datas(xlsx_file, table_name):    
    # 创建数据加载器
    loader = CSVDataLoader(DB_CONFIG)
    # 执行数据加载
    success = loader.load_file_to_mysql(xlsx_file, table_name, 'xlsx')
    
    if success:
        logger.info("数据加载完成！")
    else:
        logger.error("数据加载失败！")
        sys.exit(1)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='将CSV文件数据导入到MySQL数据库')
    parser.add_argument('csv_file', help='CSV文件路径')
    parser.add_argument('table_name', help='目标表名')
    parser.add_argument('--config', default='config.py', help='配置文件路径')
    args = parser.parse_args()
    # 检查文件是否存在
    if not os.path.exists(args.csv_file):
        logger.error(f"CSV文件不存在: {args.csv_file}")
        sys.exit(1)
    load_csv_datas(args.csv_file, args.table_name)


# 使用示例
if __name__ == "__main__":
    # main()W
    # load_csv_datas("/Users/bytedance/Desktop/stock/trade/TRD_Dalyr1.csv", "trade")
    # load_xlsx_datas("/Users/bytedance/Desktop/stock/shares/CG_Capchg.xlsx", "shares")
    load_csv_datas("/Users/bytedance/Desktop/stock/cash_flow.csv", "cash_flow")