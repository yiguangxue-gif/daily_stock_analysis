# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 超跌反弹特化版 (抗断流·双引擎装甲版)
===================================

【重要声明】：此模块为全新加入的独立选股雷达，绝不影响或删减原有的 analyzer.py 核心分析引擎功能。

设计理念：专治“一买就跌”，寻找跌无可跌、主力刚刚进场点火的右侧反弹拐点。

核心升级：
1. 引入【新浪财经】备用行情源。若东方财富限流，瞬间无缝切换。
2. 本地全量强算：不依赖外部接口提供的“量比”和“60日跌幅”，直接通过 K 线底层推算，实现降维打击。
"""

import akshare as ak
import pandas as pd
import numpy as np
import logging
import time
import random
import re

# 设置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🚀 %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class ReboundScreener:
    def __init__(self):
        self.target_count = 0

    def _fetch_with_retry(self, func, retries=3, delay=2, *args, **kwargs):
        """基础网络重试模块"""
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(delay + attempt)

    def get_market_spot(self):
        """双引擎全市场数据抓取：东财主引擎 -> 新浪备用引擎"""
        try:
            logger.info("尝试获取 [东方财富] 全量行情 (主引擎)...")
            df = self._fetch_with_retry(ak.stock_zh_a_spot_em, retries=2, delay=2)
            df['code'] = df['代码'].astype(str)
            df['name'] = df['名称']
            df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
            df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
            df['market_cap'] = pd.to_numeric(df['总市值'], errors='coerce').fillna(0)
            return df, "EastMoney"
        except Exception as e:
            logger.warning(f"东方财富接口受限或被墙 ({str(e)[:50]})")
            logger.warning("🔄 正在自动无缝切换至【新浪财经】备用全市场接口...")
            
        # 降级切换到新浪财经接口
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=3, delay=3)
            # 新浪返回的代码带 sh/sz 前缀，需清理
            df['code'] = df['代码'].str.replace(r'^[a-zA-Z]+', '', regex=True)
            df['name'] = df['名称']
            df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
            df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
            df['market_cap'] = 0  # 新浪接口无总市值，我们用成交额替代过滤
            return df, "SinaFinance"
        except Exception as e:
            logger.error(f"❌ 双引擎全军覆没，请检查代理或 GitHub Actions 网络设置: {e}")
            return pd.DataFrame(), "NONE"

    def run_screen(self):
        logger.info("========== 启动【超跌反弹】量化选股雷达 ==========")
        
        # 1. 获取全市场实时行情
        df, source = self.get_market_spot()
        if df.empty:
            return []

        initial_count = len(df)
        logger.info(f"成功获取全市场 {initial_count} 只股票数据 (数据源: {source})")
        
        # 2. 基础排雷与粗筛
        df = df[~df['name'].str.contains('ST|退')]
        df = df[~df['code'].str.startswith(('8', '4'))] # 剔除北交所
        
        # 今天必须是红盘 (涨幅 >= 1.5%)，且成交额大于 3000 万（剔除流动性枯竭的死股）
        df = df[df['pct_chg'] >= 1.5]
        df = df[df['amount'] >= 30000000]
        
        # 如果是东财数据，我们可以顺便用市值过滤；如果是新浪数据，这一步跳过
        if source == "EastMoney":
            df = df[(df['market_cap'] >= 20 * 100000000) & (df['market_cap'] <= 500 * 100000000)]
            candidates = df.head(80) # 东方财富数据全，可以直接取前80
        else:
            # 新浪数据：按今日涨跌幅从大到小排，抽取前 150 名进去查 K 线强算
            candidates = df.sort_values(by='pct_chg', ascending=False).head(150)

        logger.info(f"粗筛完成：锁定 {len(candidates)} 只异动标的。进入 K 线底层强算阶段...")

        final_stocks = []
        
        # 3. 精筛阶段 (K线强算与形态识别)
        for idx, row in candidates.iterrows():
            code = row['code']
            name = row['name']
            try:
                # 获取近一年 K 线
                hist = self._fetch_with_retry(
                    ak.stock_zh_a_hist, 
                    retries=2, 
                    delay=1, 
                    symbol=code, 
                    period="daily", 
                    start_date="20231001", 
                    adjust="qfq"
                )
                
                if hist is None or len(hist) < 65:
                    continue
                
                sp = hist['收盘']
                sv = hist['成交量']
                
                # 【核心强算 1：真实 60日跌幅】(自己算，不求人)
                drop_60d = (sp.iloc[-1] - sp.iloc[-60]) / sp.iloc[-60] * 100
                if drop_60d > -15.0:
                    continue # 没跌透，不要！
                    
                # 【核心强算 2：真实量比】(今天成交量 / 过去5天平均量)
                avg_vol_5 = sv.iloc[-6:-1].mean()
                vr = sv.iloc[-1] / avg_vol_5 if avg_vol_5 > 0 else 1.0
                if vr < 1.2:
                    continue # 没放量，主力没进场，不要！
                
                # --- KDJ 黄金坑计算 ---
                low_min9 = hist['最低'].rolling(9, min_periods=1).min()
                high_max9 = hist['最高'].rolling(9, min_periods=1).max()
                denom = (high_max9 - low_min9).replace(0, 1e-9)
                rsv = (sp - low_min9) / denom * 100
                k = rsv.ewm(com=2, adjust=False).mean()
                d = k.ewm(com=2, adjust=False).mean()
                j = 3 * k - 2 * d
                
                # 必须在过去3天内下过水（极度超卖/黄金坑），今天开始上翘
                j_min_3d = j.iloc[-4:-1].min()
                j_today = j.iloc[-1]
                j_yest = j.iloc[-2]
                
                # --- MACD 动能计算 ---
                exp1 = sp.ewm(span=12, adjust=False).mean()
                exp2 = sp.ewm(span=26, adjust=False).mean()
                macd = exp1 - exp2
                signal = macd.ewm(span=9, adjust=False).mean()
                hist_bar = macd - signal
                
                # 绿柱必须正在缩短，或者红柱正在放大
                macd_improving = hist_bar.iloc[-1] > hist_bar.iloc[-2]
                
                # === 终极点火确认 ===
                if j_min_3d < 5 and j_today > j_yest and macd_improving:
                    final_stocks.append({
                        "代码": code,
                        "名称": name,
                        "现价": sp.iloc[-1],
                        "今日涨幅": f"{row['pct_chg']:.2f}%",
                        "60日跌幅": f"{drop_60d:.2f}%",
                        "量比": f"{vr:.2f}",
                        "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                    logger.info(f"🎯 捕获超跌反弹金股: {name} ({code}) - 跌幅:{drop_60d:.1f}% 量比:{vr:.1f}")

                # 轻微随机休眠，防反爬
                time.sleep(random.uniform(0.2, 0.5))
                
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
        
        header = f"{'代码':<10} | {'名称':<10} | {'现价':<8} | {'今日涨幅':<8} | {'60日深跌':<10} | {'量比(强算)':<10} | {'今日成交额'}"
        print(header)
        print("-" * 80)
        
        for s in stocks:
            row_str = f"{s['代码']:<10} | {s['名称']:<10} | {s['现价']:<8.2f} | {s['今日涨幅']:<8} | {s['60日跌幅']:<10} | {s['量比']:<10} | {s['成交额']}"
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
