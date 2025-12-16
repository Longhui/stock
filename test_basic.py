#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基本功能测试脚本
用于验证程序在指定虚拟环境中的基本功能
"""

import sys
import os

# 添加项目路径到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """测试导入功能"""
    print("=== 测试导入功能 ===")
    
    try:
        import pymysql
        print("✓ pymysql 导入成功")
    except ImportError as e:
        print(f"✗ pymysql 导入失败: {e}")
        return False
    
    try:
        import akshare as ak
        print("✓ akshare 导入成功")
    except ImportError as e:
        print(f"✗ akshare 导入失败: {e}")
        return False
    
    try:
        import pandas as pd
        print("✓ pandas 导入成功")
    except ImportError as e:
        print(f"✗ pandas 导入失败: {e}")
        return False
    
    try:
        import numpy as np
        print("✓ numpy 导入成功")
    except ImportError as e:
        print(f"✗ numpy 导入失败: {e}")
        return False
    
    return True

def test_akshare():
    """测试akshare基本功能"""
    print("\n=== 测试akshare功能 ===")
    
    try:
        import akshare as ak
        
        # 测试获取股票基本信息
        stock_info = ak.stock_info_a_code_name()
        if not stock_info.empty:
            print(f"✓ 获取股票列表成功，共 {len(stock_info)} 只股票")
            print(f"  示例股票: {stock_info.iloc[0]['code']} - {stock_info.iloc[0]['name']}")
        else:
            print("✗ 获取股票列表失败")
            return False
            
        # 测试获取单个股票的现金流量表（简化测试）
        try:
            # 使用一个常见的股票代码进行测试
            test_stock = "000001"  # 平安银行
            cash_flow = ak.stock_financial_report_sina(stock=test_stock, symbol="现金流量表")
            if not cash_flow.empty:
                print(f"✓ 获取股票 {test_stock} 现金流量表成功，共 {len(cash_flow)} 行数据")
                print(f"  数据列: {list(cash_flow.columns)}")
            else:
                print(f"⚠ 股票 {test_stock} 现金流量表为空，可能是数据问题")
        except Exception as e:
            print(f"⚠ 获取现金流量表时出现警告: {e}")
            # 这可能是正常情况，不视为失败
            
        return True
        
    except Exception as e:
        print(f"✗ akshare功能测试失败: {e}")
        return False

def test_config():
    """测试配置文件"""
    print("\n=== 测试配置文件 ===")
    
    try:
        from config import DB_CONFIG, PROCESS_CONFIG
        print("✓ 配置文件导入成功")
        print(f"  数据库配置: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        print(f"  处理配置: 延迟 {PROCESS_CONFIG['delay_between_requests']}秒")
        return True
    except ImportError as e:
        print("⚠ 配置文件不存在，将使用默认配置")
        return True  # 配置文件不存在不是致命错误
    except Exception as e:
        print(f"✗ 配置文件测试失败: {e}")
        return False

def test_main_module():
    """测试主模块"""
    print("\n=== 测试主模块 ===")
    
    try:
        # 测试导入主模块
        from cash_flows_data import StockCashFlowProcessor
        print("✓ 主模块导入成功")
        
        # 测试创建处理器实例（不连接数据库）
        test_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        processor = StockCashFlowProcessor(test_config)
        print("✓ 处理器实例创建成功")
        
        return True
        
    except Exception as e:
        print(f"✗ 主模块测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始基本功能测试...")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    print(f"Python路径: {sys.path[0]}")
    
    tests = [
        test_imports,
        test_akshare,
        test_config,
        test_main_module
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        if test_func():
            passed += 1
    
    print(f"\n=== 测试结果 ===")
    print(f"通过: {passed}/{total}")
    
    if passed == total:
        print("🎉 所有测试通过！程序基本功能正常")
        print("\n下一步建议:")
        print("1. 修改 config.py 中的数据库配置")
        print("2. 运行: python main.py --test --no-db 进行数据获取测试")
        print("3. 运行: python main.py --test --limit=2 进行完整流程测试")
    else:
        print("❌ 部分测试失败，请检查环境配置")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)