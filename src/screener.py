# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 超跌反弹特化版
===================================

【重要声明】：此模块为全新加入的独立选股雷达，绝不影响或删减原有的 analyzer.py 核心分析引擎功能。

设计理念：专治“一买就跌”，寻找跌无可跌、主力刚刚进场点火的右侧反弹拐点。
过滤逻辑：
1. 宏观过滤：剔除ST、北交所，选取 20亿~500亿 市值区间的标的。
2. 超跌判定：近60日跌幅 > 15%。
3. 异动点火：今日涨幅 > 1.5%，量比 > 1.2，换手率健康。
4. 核心形态：KDJ 极度超卖后金叉向上，MACD 绿柱缩短（下跌动能枯竭）。

使用方法：
1. 独立运行此脚本 `python -m src.screener` 获取股票池。
2. 将选出的看好代码，填入你的 Google 云端表格。
3. 运行主程序 `python main.py`，让强大的 analyzer.py 为你进行防诱多排雷和网格测算。
"""

import akshare as ak
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime

# 设置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🚀 %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class ReboundScreener:
    def __init__(self):
        self.target_count = 0

    def run_screen(self):
        logger.info("========== 启动【超跌反弹】量化选股雷达 ==========")
        
        # 1. 获取全市场实时行情
        logger.info("正在获取 A股 5000+ 标的实时数据，请稍候...")
        try:
            df = ak.stock_zh_a_spot_em()
        except Exception as e:
            logger.error(f"获取全市场数据失败，请检查网络: {e}")
            return []

        # 2. 粗筛阶段 (Coarse Filter)
        initial_count = len(df)
        
        # 剔除 ST 股和退市股
        df = df[~df['名称'].str.contains('ST|退')]
        # 剔除北交所 (8开头、4开头)
        df = df[~df['代码'].str.startswith(('8', '4'))]
        
        # 数值转换兜底
        df['60日涨跌幅'] = pd.to_numeric(df['60日涨跌幅'], errors='coerce').fillna(0)
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
        df['量比'] = pd.to_numeric(df['量比'], errors='coerce').fillna(0)
        df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce').fillna(0)
        df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce').fillna(0)
        
        # 【核心逻辑 1：跌了一段时间】 60日跌幅超过 15%
        df = df[df['60日涨跌幅'] < -15.0]
        
        # 【核心逻辑 2：准备反弹】 今天开始异动红盘，且有增量资金进来
        df = df[df['涨跌幅'] >= 1.5]
        df = df[df['量比'] >= 1.2]
        df = df[df['换手率'] >= 2.0]
        
        # 【核心逻辑 3：资金承载力】 剔除容易退市的微盘和拉不动的大笨象 (20亿~500亿)
        df = df[df['总市值'] >= 20 * 100000000]
        df = df[df['总市值'] <= 500 * 100000000]
        
        candidates = df.head(80) # 提取前80个最有潜力的去查K线形态
        logger.info(f"粗筛完成：从 {initial_count} 只股票中锁定 {len(candidates)} 只潜伏标的。进入深度形态扫描...")

        final_stocks = []
        
        # 3. 精筛阶段 (Fine Filter) - K 线深度扫描
        for idx, row in candidates.iterrows():
            code = row['代码']
            name = row['名称']
            try:
                # 获取近 100 天的日 K 线
                hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20240101", adjust="qfq")
                if len(hist) < 60:
                    continue
                
                sp = hist['收盘']
                
                # --- KDJ 黄金坑计算 ---
                low_min9 = hist['最低'].rolling(9, min_periods=1).min()
                high_max9 = hist['最高'].rolling(9, min_periods=1).max()
                denom = (high_max9 - low_min9).replace(0, 1e-9)
                rsv = (sp - low_min9) / denom * 100
                k = rsv.ewm(com=2, adjust=False).mean()
                d = k.ewm(com=2, adjust=False).mean()
                j = 3 * k - 2 * d
                
                # 判定：J 值在过去 3 天内去过 0 以下（极度超卖/黄金坑），今天拐头向上
                j_min_3d = j.iloc[-4:-1].min()
                j_today = j.iloc[-1]
                j_yest = j.iloc[-2]
                
                # --- MACD 动能计算 ---
                exp1 = sp.ewm(span=12, adjust=False).mean()
                exp2 = sp.ewm(span=26, adjust=False).mean()
                macd = exp1 - exp2
                signal = macd.ewm(span=9, adjust=False).mean()
                hist_bar = macd - signal
                
                # 判定：MACD 绿柱缩短（下跌动能衰竭），或者红柱放大
                macd_improving = hist_bar.iloc[-1] > hist_bar.iloc[-2]
                
                # === 终极点火确认 ===
                if j_min_3d < 5 and j_today > j_yest and macd_improving:
                    final_stocks.append({
                        "代码": code,
                        "名称": name,
                        "现价": row['最新价'],
                        "今日涨幅": f"{row['涨跌幅']:.2f}%",
                        "60日跌幅": f"{row['60日涨跌幅']:.2f}%",
                        "量比": f"{row['量比']:.2f}",
                        "换手率": f"{row['换手率']:.2f}%",
                        "市值": f"{row['总市值']/100000000:.1f}亿"
                    })
                    logger.info(f"🎯 捕获超跌反弹金股: {name} ({code})")

                # 礼貌休眠，防止东方财富接口封 IP
                time.sleep(0.3)
                
            except Exception as e:
                continue
                
        self.target_count = len(final_stocks)
        self._print_report(final_stocks)
        return final_stocks

    def _print_report(self, stocks):
        print("\n" + "="*80)
        print("                 🏆 A股【超跌反弹·右侧点火】绝密股票池")
        print("="*80)
        if not stocks:
            print("🧊 当前市场环境恶劣，没有符合【超跌且底部放量反弹】的标的，建议管住手！")
            return
            
        print(f"共锁定 {self.target_count} 只主力资金正在点火的底部标的：\n")
        
        # 打印表头
        header = f"{'代码':<10} | {'名称':<10} | {'现价':<8} | {'今日涨幅':<8} | {'60日深跌':<10} | {'量比(异动)':<10} | {'市值'}"
        print(header)
        print("-" * 80)
        
        for s in stocks:
            # 格式化对齐打印
            row_str = f"{s['代码']:<10} | {s['名称']:<10} | {s['现价']:<8} | {s['今日涨幅']:<8} | {s['60日跌幅']:<10} | {s['量比']:<10} | {s['市值']}"
            print(row_str)
            
        print("\n" + "="*80)
        print("💡 【下一步操作建议】：")
        print("1. 请挑选上方列表中 1~2 只你熟悉的股票。")
        print("2. 将它们的『代码』填入你的 Google 云端表格中。")
        print("3. 让强大的 AI 天网（analyzer.py）为你进行终极风险体检和买卖点测算！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
