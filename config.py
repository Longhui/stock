#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库配置文件
请根据您的实际环境修改以下配置
"""

# MySQL数据库配置
DB_CONFIG = {
    'host': '121.5.42.254',      # 数据库主机地址
    'port': 3306,            # 数据库端口
    'user': 'stock',          # 数据库用户名
    'password': 'raolh@@834507',  # 数据库密码
    'database': 'stock1'      # 数据库名称
}

# 数据处理配置
PROCESS_CONFIG = {
    'delay_between_requests': 1.0,  # 请求间隔时间（秒），避免频繁请求
    'max_retries': 3,               # 最大重试次数
    'log_level': 'INFO'             # 日志级别
}

# akshare配置
AKSHARE_CONFIG = {
    'timeout': 30,                  # 请求超时时间（秒）
    'retry_times': 3                # 重试次数
}