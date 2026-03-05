# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 超跌反弹特化版 (抗断流装甲版)
===================================

设计理念：专治“一买就跌”，寻找跌无可跌、主力刚刚进场点火的右侧反弹拐点。
"""

import akshare as ak
import pandas as pd
import numpy as np
import logging
import time
import random
from datetime import datetime

# 设置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🚀 %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class ReboundScreener:
    def __init__(self):
        self.target_count = 0

    def _fetch_with_retry(self, func, retries=5, delay=3, *args, **kwargs):
        """强力网络重试装甲：遇到断网自动休眠并重试"""
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                err_str = str(e)
                if attempt == retries - 1:
                    logger.error(f"❌ 接口请求彻底失败 (已重试 {retries} 次): {err_str}")
                    raise e
                sleep_time = delay + attempt * 2
                logger.warning(f"⚠️ 接口遭遇拦截或断流，{sleep_time} 秒后发起第 {attempt + 2} 次冲锋... ({err_str[:50]})")
                time.sleep(sleep_time)

    def run_screen(self):
        logger.info("========== 启动【超跌反弹】量化选股雷达 ==========")
        
        logger.info("正在请求东方财富服务器，获取 A股 5000+ 标的实时数据...")
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot_em, retries=5, delay=3)
        except Exception as e:
            logger.error("无法获取全市场行情，雷达任务中止。建议稍后再试或检查代理配置。")
            return []

        initial_count = len(df)
        
        df = df[~df['名称'].str.contains('ST|退')]
        df = df[~df['代码'].str.startswith(('8', '4'))]
        
        df['60日涨跌幅'] = pd.to_numeric(df['60日涨跌幅'], errors='coerce').fillna(0)
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
        df['量比'] = pd.to_numeric(df['量比'], errors='coerce').fillna(0)
        df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce').fillna(0)
        df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce').fillna(0)
        
        df = df[df['60日涨跌幅'] < -15.0]
        df = df[df['涨跌幅'] >= 1.5]
        df = df[df['量比'] >= 1.2]
        df = df[df['换手率'] >= 2.0]
        df = df[df['总市值'] >= 20 * 100000000]
        df = df[df['总市值'] <= 500 * 100000000]
        
        candidates = df.head(80)
        logger.info(f"粗筛完成：从 {initial_count} 只股票中锁定 {len(candidates)} 只潜伏标的。进入深度形态扫描...")

        final_stocks = []
        
        for idx, row in candidates.iterrows():
            code = row['代码']
            name = row['名称']
            try:
                hist = self._fetch_with_retry(
                    ak.stock_zh_a_hist, 
                    retries=3, 
                    delay=1, 
                    symbol=code, 
                    period="daily", 
                    start_date="20240101", 
                    adjust="qfq"
                )
                
                if hist is None or len(hist) < 60:
                    continue
                
                sp = hist['收盘']
                
                low_min9 = hist['最低'].rolling(9, min_periods=1).min()
                high_max9 = hist['最高'].rolling(9, min_periods=1).max()
                denom = (high_max9 - low_min9).replace(0, 1e-9)
                rsv = (sp - low_min9) / denom * 100
                k = rsv.ewm(com=2, adjust=False).mean()
                d = k.ewm(com=2, adjust=False).mean()
                j = 3 * k - 2 * d
                
                j_min_3d = j.iloc[-4:-1].min()
                j_today = j.iloc[-1]
                j_yest = j.iloc[-2]
                
                exp1 = sp.ewm(span=12, adjust=False).mean()
                exp2 = sp.ewm(span=26, adjust=False).mean()
                macd = exp1 - exp2
                signal = macd.ewm(span=9, adjust=False).mean()
                hist_bar = macd - signal
                
                macd_improving = hist_bar.iloc[-1] > hist_bar.iloc[-2]
                
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

                time.sleep(random.uniform(0.3, 0.8))
                
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
        
        header = f"{'代码':<10} | {'名称':<10} | {'现价':<8} | {'今日涨幅':<8} | {'60日深跌':<10} | {'量比(异动)':<10} | {'市值'}"
        print(header)
        print("-" * 80)
        
        for s in stocks:
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
