name: 🚀 A股超跌反弹量化选股雷达

on:
  schedule:
    # 每天北京时间 15:30 左右运行 (UTC 7:30)
    # 专为 A 股收盘后筛选右侧拐点标的设计
    - cron: '30 7 * * 1-5'
  workflow_dispatch: # 允许你随时手动点击运行

jobs:
  run-screener:
    runs-on: ubuntu-latest
    
    steps:
    - name: 📥 检出代码
      uses: actions/checkout@v4

    - name: 🐍 设置 Python 环境
      uses: actions/setup-python@v5
      with:
        python-version: '3.10'
        cache: 'pip' # 缓存 pip 依赖加速运行

    - name: 📦 安装依赖
      run: |
        python -m pip install --upgrade pip
        if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
        pip install akshare pandas numpy # 确保选股雷达需要的库安装好

    - name: 🎯 运行量化选股雷达
      run: |
        python -m src.screener
