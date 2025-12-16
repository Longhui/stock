#!/bin/bash
# 激活虚拟环境脚本

# 设置虚拟环境路径
ENV_PATH="/Users/bytedance/work/qbot-env"

# 检查虚拟环境是否存在
if [ ! -d "$ENV_PATH" ]; then
    echo "错误: 虚拟环境路径不存在: $ENV_PATH"
    exit 1
fi

# 激活虚拟环境
source "$ENV_PATH/bin/activate"

# 检查Python版本
echo "Python版本:"
python --version

# 检查关键依赖包
echo "检查依赖包..."
pip list | grep -E "(pymysql|akshare|pandas|numpy)"

# 设置环境变量
export PYTHONPATH="/Users/bytedance/work/trae_projects/stock"
export AKSHARE_DEBUG=1
export QBOT_DEBUG=1
export DYLD_LIBRARY_PATH="/Users/bytedance/work/qbot-env/lib"

echo "虚拟环境已激活，环境变量已设置"
echo "现在可以运行: python main.py"