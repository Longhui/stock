#!/usr/bin/env python3
"""
打印cash_flow_df中所有元素的列的名称，并演示如何按序号获取元素值
"""
import pandas as pd
import akshare as ak

# 获取一个股票代码的现金流量表数据作为示例
stock_code = "000001"  # 平安银行作为示例

try:
    # 使用akshare获取现金流量表数据
    cash_flow_df = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")
    
    if cash_flow_df.empty:
        print(f"股票代码 {stock_code} 的现金流量表数据为空")
    else:
        print(f"股票代码 {stock_code} 的现金流量表数据列名：")
        print("=" * 50)
        
        # 打印所有列名
        for i, column in enumerate(cash_flow_df.columns, 1):
            print(f"{i:2d}. {column}")
        
        print("=" * 50)
        print(f"总共有 {len(cash_flow_df.columns)} 列")
        print(f"总共有 {len(cash_flow_df)} 行")
        
        # 演示如何按序号获取元素值
        print("\n=== 按序号获取元素值示例 ===")
        
        # 1. 获取特定行列的元素
        if len(cash_flow_df) > 0 and len(cash_flow_df.columns) > 0:
            print("\n1. 获取第0行第1列的元素值：")
            value = cash_flow_df.iloc[0, 1]
            print(f"   cash_flow_df.iloc[0, 1] = {value}")
            
            print("\n2. 获取第0行第2列的元素值：")
            value = cash_flow_df.iloc[0, 2]
            print(f"   cash_flow_df.iloc[0, 2] = {value}")
        
        # 2. 获取整行数据
        if len(cash_flow_df) > 0:
            print("\n3. 获取第0行的所有数据：")
            row_data = cash_flow_df.iloc[0]
            print(f"   数据类型: {type(row_data)}")
            print(f"   数据内容:\n{row_data}")
        
        # 3. 获取整列数据
        if len(cash_flow_df.columns) > 0:
            print("\n4. 获取第1列的所有数据：")
            column_data = cash_flow_df.iloc[:, 1]
            print(f"   列名: {cash_flow_df.columns[1]}")
            print(f"   数据类型: {type(column_data)}")
            print(f"   前5个值: {column_data.head().tolist()}")
        
        # 4. 遍历所有元素
        print("\n5. 遍历前3行前3列的元素：")
        for i in range(min(3, len(cash_flow_df))):
            for j in range(min(3, len(cash_flow_df.columns))):
                value = cash_flow_df.iloc[i, j]
                print(f"   行{i}列{j}({cash_flow_df.columns[j]}) = {value}")
        
        # 打印前几行数据以了解数据结构
        print("\n=== 前3行数据示例 ===")
        print(cash_flow_df.head(3))
        
except Exception as e:
    print(f"获取数据失败: {e}")