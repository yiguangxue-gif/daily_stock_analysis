# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 八大策略赛马 + 智能顺延出击 + 打脸追踪 (究极版)
===================================

核心重构 (全景轮动系统):
1. 【智能顺延出击】：当12个月回测的“冠军策略”今日选不出标的时，系统会自动向下寻找“亚军”、“季军”，直到找到有标的且胜率达标的战法，绝不白白踏空！
2. 【打脸处刑台升级】：加入了“推演日期”和“当时AI定调反思”，让所有吹过的牛逼都有迹可循，亏损强制检讨。
3. 【终极熔断防卡死】：保留了 6 次失败强制跳过、6秒极速超时的防黑洞功能。
4. 【八大神级战法内置】：趋势低吸、底部起爆、强庄首阴、均线粘合、龙头断板、N字反包、新高突破、极致双底。
"""

import os
import warnings
warnings.filterwarnings("ignore")
os.environ["GRPC_PYTHON_LOG_LEVEL"] = "error"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import akshare as ak
import pandas as pd
import numpy as np
import logging
import time
import random
import re
import csv
import json
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from datetime import datetime, timedelta
from json_repair import repair_json

from src.config import get_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🚀 %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

socket.setdefaulttimeout(6.0)

class ReboundScreener:
    def __init__(self):
        self.config = get_config()
        self.history_file = "data/screener_history.csv"
        self.lessons_file = "data/ai_lessons.txt"
        os.makedirs("data", exist_ok=True)
        
        self.pro = None
        if self.config.tushare_token:
            try:
                import tushare as ts
                ts.set_token(self.config.tushare_token)
                self.pro = ts.pro_api()
                logger.info("✅ 检测到 Tushare Token，已激活 VIP 护盾引擎！")
            except Exception: pass

    def _fetch_with_retry(self, func, retries=1, delay=0.5, *args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1: raise e
                time.sleep(delay)

    def get_market_spot(self):
        try:
            logger.info("尝试获取全量行情 (主引擎)...")
            df = self._fetch_with_retry(ak.stock_zh_a_spot_em, retries=2, delay=1)
            df['code'] = df['代码'].astype(str)
            df['name'] = df['名称']
            df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
            df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
            df['market_cap'] = pd.to_numeric(df.get('总市值', 0), errors='coerce').fillna(0)
            df['circ_mv'] = pd.to_numeric(df.get('流通市值', df['market_cap']), errors='coerce').fillna(0)
            df['close'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
            df['open'] = pd.to_numeric(df.get('今开', df['close']), errors='coerce').fillna(0)
            df['prev_close'] = pd.to_numeric(df.get('昨收', df['close']), errors='coerce').fillna(0)
            return df
        except Exception as e:
            logger.warning("东方财富接口受限，🔄 切换至新浪财经...")
            try:
                df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=1, delay=1)
                col_map = {'symbol': '代码', 'name': '名称', 'changepercent': '涨跌幅', 'amount': '成交额', 'trade': '最新价', 'open': '今开', 'settlement': '昨收'}
                for eng, chn in col_map.items():
                    if chn not in df.columns and eng in df.columns: df[chn] = df[eng]
                df['code'] = df['代码'].str.replace(r'^[a-zA-Z]+', '', regex=True)
                df['name'] = df['名称']
                df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
                df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
                df['close'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
                df['open'] = pd.to_numeric(df.get('今开', df['close']), errors='coerce').fillna(0)
                df['prev_close'] = pd.to_numeric(df.get('昨收', df['close']), errors='coerce').fillna(0)
                df['market_cap'] = 0  
                df['circ_mv'] = 0  
                return df
            except Exception:
                return pd.DataFrame()

    def _get_daily_kline(self, code):
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y%m%d')
        try:
            return self._fetch_with_retry(ak.stock_zh_a_hist, retries=1, delay=0.5, symbol=code, period="daily", start_date=start_date, adjust="qfq")
        except Exception: pass
        try:
            symbol_sina = f"sh{code}" if code.startswith('6') else f"sz{code}"
            df = self._fetch_with_retry(ak.stock_zh_a_daily, retries=1, delay=0.5, symbol=symbol_sina, start_date=start_date, adjust="qfq")
            if df is not None and not df.empty:
                res = pd.DataFrame()
                res['日期'] = df['date']
                res['收盘'] = df['close']
                res['开盘'] = df['open']
                res['最高'] = df['high']
                res['最低'] = df['low']
                res['成交量'] = df['volume']
                return res
        except: pass
        return None

    def fetch_macro_news(self):
        news_text = "今日无重大宏观新闻"
        try:
            query = urllib.parse.quote("中国 A股 政策 央行")
            url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=4) as res:
                root = ET.fromstring(res.read())
                lines = [f"- {it.find('title').text}" for it in root.findall('.//item')[:4]]
                if lines: news_text = "\n".join(lines)
        except Exception: pass
        return news_text

    def load_ai_lessons(self):
        if not os.path.exists(self.lessons_file): return "暂无历史避坑教训。"
        try:
            with open(self.lessons_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            lessons = [line.strip() for line in lines if line.strip()]
            return "\n".join(lessons[-5:]) if lessons else "暂无历史避坑教训。"
        except: return "读取历史教训失败。"

    def save_ai_lesson(self, lesson):
        if not lesson or len(lesson) < 5 or "无" in lesson.strip() or "未" in lesson.strip(): return
        try:
            with open(self.lessons_file, 'a', encoding='utf-8') as f:
                date_str = datetime.now().strftime('%Y-%m-%d')
                f.write(f"[{date_str} 铁律]: {lesson}\n")
        except: pass

    def process_review_and_history(self, market_df, lookback_days=5):
        today_str = datetime.now().strftime('%Y-%m-%d')
        review_summary = "暂无往期复盘数据。"
        review_records = []
        recent_stats = {"avg_ret": 0.0, "win_rate": 0.0, "days": 0, "total_count": 0}

        if not os.path.exists(self.history_file): return review_summary, review_records, recent_stats

        try:
            df_hist = pd.read_csv(self.history_file)
            unreviewed = df_hist[df_hist['Date_T1'].isna() | (df_hist['Date_T1'] == '')]
            
            if not unreviewed.empty:
                logger.info(f"🔍 发现待结算的历史金股，正在核算真实盈亏进行打脸分析...")
                for idx, row in unreviewed.iterrows():
                    code = str(row['Code']).zfill(6)
                    match = market_df[market_df['code'] == code]
                    if not match.empty:
                        t1_price = float(match.iloc[0]['close'])
                        t0_price = float(row['Price_T0'])
                        if t0_price > 0:
                            ret_pct = ((t1_price - t0_price) / t0_price) * 100
                            df_hist.at[idx, 'Date_T1'] = today_str
                            df_hist.at[idx, 'Price_T1'] = t1_price
                            df_hist.at[idx, 'Return_Pct'] = round(ret_pct, 2)
                            
                            review_records.append({
                                "推演日": row['Date_T0'], # 新增推演日期
                                "代码": code, "名称": row['Name'], "买入价": t0_price, 
                                "当前价": t1_price, "真实涨跌幅": f"{ret_pct:+.2f}%", 
                                "当时逻辑": str(row.get('AI_Reason', ''))[:60] # 包含当时的AI反思
                            })
                df_hist.to_csv(self.history_file, index=False)
                
            reviewed_df = df_hist.dropna(subset=['Return_Pct']).copy()
            if not reviewed_df.empty:
                recent_dates = sorted(reviewed_df['Date_T0'].unique())[-lookback_days:]
                recent_records = reviewed_df[reviewed_df['Date_T0'].isin(recent_dates)]
                
                if not recent_records.empty:
                    total_return = recent_records['Return_Pct'].sum()
                    win_count = (recent_records['Return_Pct'] > 0).sum()
                    total_count = len(recent_records)
                    
                    avg_ret = total_return / total_count if total_count > 0 else 0
                    win_rate = (win_count / total_count) * 100 if total_count > 0 else 0
                    
                    recent_stats = {
                        "avg_ret": avg_ret, "win_rate": win_rate, 
                        "days": len(recent_dates), "total_count": total_count
                    }
                    
                    review_summary = f"【AI近期选股打脸追踪 - 近 {len(recent_dates)} 批次】\n"
                    review_summary += f"前期共推演 {total_count} 只标的，目前平均波段收益: {avg_ret:+.2f}%，胜率: {win_rate:.1f}%。\n"
                    
                    for date in recent_dates:
                        day_df = recent_records[recent_records['Date_T0'] == date]
                        review_summary += f"👉 [{date} 推荐]: "
                        for _, r in day_df.iterrows():
                            review_summary += f"{r['Name']}({r['Return_Pct']:+.2f}%) "
                        review_summary += "\n"
        except Exception as e:
            logger.error(f"复盘核算异常: {e}")
            
        return review_summary, review_records, recent_stats

    def calculate_technical_indicators(self, hist):
        df = hist.copy()
        for c in ['收盘', '开盘', '最高', '最低', '成交量']: 
            df[c] = pd.to_numeric(df[c], errors='coerce')
            
        if '涨跌幅' not in df.columns:
            df['涨跌幅'] = df['收盘'].pct_change() * 100
            
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA30'] = df['收盘'].rolling(30).mean()
        df['MA60'] = df['收盘'].rolling(60).mean()
        df['Vol_MA5'] = df['成交量'].rolling(5).mean()
        
        exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
        df['MACD_DIF'] = exp1 - exp2
        df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
        df['MACD'] = 2 * (df['MACD_DIF'] - df['MACD_DEA'])
        
        delta = df['收盘'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=6, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=6, min_periods=1).mean()
        rs = gain / loss
        df['RSI6'] = 100 - (100 / (1 + rs))
        
        df['BIAS20'] = (df['收盘'] - df['MA20']) / df['MA20'] * 100
        
        df['High_120d_shift'] = df['最高'].shift(1).rolling(120, min_periods=1).max()
        df['High_20d_shift'] = df['最高'].shift(1).rolling(20, min_periods=1).max()
        df['Min_20d'] = df['最低'].rolling(20, min_periods=1).min()
        df['Max_Pct_10d'] = df['涨跌幅'].rolling(10, min_periods=1).max()
        
        df['Body'] = abs(df['收盘'] - df['开盘'])
        df['Upper_Shadow'] = df['最高'] - df[['收盘', '开盘']].max(axis=1)

        df['Close_T5'] = df['收盘'].shift(-5)
        df['High_5D'] = df['最高'].shift(-1)[::-1].rolling(5, min_periods=1).max()[::-1]
        
        df['Pct_Chg_Shift1'] = df['涨跌幅'].shift(1)
        df['Pct_Chg_Shift2'] = df['涨跌幅'].shift(2)
        df['Pct_Chg_Shift3'] = df['涨跌幅'].shift(3)
        df['Vol_Shift3'] = df['成交量'].shift(3)
        df['Open_Shift3'] = df['开盘'].shift(3)
        
        return df

    def evaluate_strategies(self, df):
        # 战法A: 趋势低吸
        sA_trend = df['MA20'] > df['MA60']
        sA_support = (abs(df['收盘'] - df['MA20']) / df['MA20']) <= 0.03
        sA_vol = df['成交量'] < df['Vol_MA5'] * 0.8
        df['Sig_A_Trend_Pullback'] = sA_trend & sA_support & sA_vol

        # 战法B: 底部起爆
        sB_base = df['收盘'].shift(1) < df['MA60'].shift(1)
        sB_break = df['收盘'] > df['MA60']
        sB_vol = df['成交量'] > df['Vol_MA5'] * 2.0
        sB_pct = df['涨跌幅'] > 4.0
        df['Sig_B_Bottom_Breakout'] = sB_base & sB_break & sB_vol & sB_pct

        # 战法C: 强庄首阴
        sC_gene = df['Max_Pct_10d'] > 8.0
        sC_pct = (df['涨跌幅'] < 0) & (df['涨跌幅'] >= -6.0)
        sC_vol = df['成交量'] < df['Vol_MA5'] * 0.7
        df['Sig_C_Strong_Dip'] = sC_gene & sC_pct & sC_vol

        # 战法D: 均线粘合
        ma_max = df[['MA5', 'MA10', 'MA20']].max(axis=1)
        ma_min = df[['MA5', 'MA10', 'MA20']].min(axis=1)
        sD_squeeze = (ma_max - ma_min) / ma_min < 0.03 
        sD_up = (df['收盘'] > ma_max) & (df['开盘'] < ma_min) & (df['涨跌幅'] > 3.0)
        df['Sig_D_MA_Squeeze'] = sD_squeeze & sD_up
        
        # 战法E: 龙头断板分歧
        sE_gene = df['Pct_Chg_Shift1'] > 9.0
        sE_pct = (df['涨跌幅'] > -5.0) & (df['涨跌幅'] < 4.0)
        sE_vol = df['成交量'] > df['Vol_MA5'] * 1.5
        df['Sig_E_Dragon_Relay'] = sE_gene & sE_pct & sE_vol
        
        # 战法F: N字反包
        sF_day3 = df['Pct_Chg_Shift3'] > 6.0
        sF_day21 = (df['Pct_Chg_Shift2'] < 2.0) & (df['Pct_Chg_Shift1'] < 2.0) & (df['收盘'].shift(1) > df['Open_Shift3'])
        sF_today = df['涨跌幅'] > 0
        sF_vol = df['成交量'] < df['Vol_Shift3']
        df['Sig_F_N_Shape'] = sF_day3 & sF_day21 & sF_today & sF_vol
        
        # 战法G: 新高突破
        sG_high = df['收盘'] >= df['High_120d_shift']
        sG_vol = df['成交量'] > df['Vol_MA5'] * 2.0
        sG_pct = df['涨跌幅'] > 4.0
        df['Sig_G_ATH_Breakout'] = sG_high & sG_vol & sG_pct
        
        # 战法H: 缩量双底
        sH_low = (df['收盘'] - df['Min_20d']) / df['Min_20d'] < 0.05
        sH_macd = df['MACD'] > df['MACD'].shift(5)
        sH_vol = df['成交量'] < df['Vol_MA5'] * 0.7
        df['Sig_H_Double_Bottom'] = sH_low & sH_macd & sH_vol

        return df

    def ai_select_top5(self, candidates, macro_news, actual_used_strategy, strategy_reason, review_summary):
        logger.info(f"🧠 正在唤醒 AI 执行今日实战波段出击策略: 【{actual_used_strategy}】")
        if not self.config.gemini_api_key: return {"top_5": []}

        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | 策略:{c['匹配策略']} | 现价:{c['现价']} | 涨跌幅:{c['今日涨幅']} | 量比:{c['量比']}\n"

        prompt = f"""你是一位A股神级游资总舵主。
根据我们量化系统的【12个月全景赛马回测】及【智能动态顺延机制】，今日实战出击的最优波段策略是：【{actual_used_strategy}】！
出击理由：{strategy_reason}。

这证明该战法是目前最适应今天A股盘面生态的印钞机。我已使用该战法为你筛选出以下【今日完美备选池】！

### 🧠 你的避坑记忆：
{past_lessons}

### 📉 【核心指令】近期选股 AI 打脸回测 (多日连贯追踪)：
{review_summary}
(⚠️ 警告：仔细看上面的打脸复盘数据，如果最近你选的股出现了亏损或胜率暴跌，你必须在下方的 `ai_reflection` 字段里进行【严厉的自我检讨与打脸反思】，指出当时是接盘了高位还是被洗盘了，并端正今天的选股态度！)

### 🌍 今日宏观大势：
{macro_news}

### 📊 实战备选池 (请从中优选5只)：
{cand_text}

### 🎯 你的任务：
1. 输出不多不少恰好 5 只股票！(不足5只则全选)
2. 这次操作模式是【波段操作】，买入后持仓观察 3-5 天。
3. `reason` 说明买入逻辑。
4. `target_price` 设定为未来5天内的波段冲高目标价，`stop_loss` 为防守破位价。

请严格输出 JSON 格式：
```json
{{
    "ai_reflection": "对大盘环境的洞察，以及结合【近期选股打脸回测】进行深刻的自我审视与检讨...",
    "new_lesson_learned": "提取的新避坑/顺势铁律(无则填：无)",
    "macro_view": "大盘未来一周情绪推演...",
    "top_5": [
        {{
            "code": "代码",
            "name": "名称",
            "strategy": "原样保留",
            "current_price": 现价,
            "reason": "入选逻辑（50字左右）",
            "target_price": "波段冲高目标价",
            "stop_loss": "破位止损价"
        }}
    ]
}}
```
"""
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.config.gemini_api_key)
            model = genai.GenerativeModel(model_name=self.config.gemini_model)
            response = model.generate_content(prompt, generation_config={"temperature": 0.4})
            m = re.search(r'(\{.*\})', response.text, re.DOTALL)
            json_str = m.group(1) if m else response.text
            return json.loads(repair_json(json_str))
        except Exception: return None

    def send_email_report(self, ai_data, tournament_stats, overall_best_strategy, actual_used_strategy, target_count, review_records, recent_stats):
        logger.info("📧 正在生成八路诸侯波段赛马战报邮件...")
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        if not sender or not pwd: return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 打脸处刑台 (增加推演日期和AI反思)
        review_html = ""
        if review_records:
            avg_ret = recent_stats.get('avg_ret', 0.0)
            win_rate = recent_stats.get('win_rate', 0.0)
            days = recent_stats.get('days', 1)
            color_ret = "red" if avg_ret > 0 else "green" if avg_ret < 0 else "black"
            
            review_html += f"""
            <h3>⚖️ 前期战绩打脸处刑台 (近 {days} 批次核算)</h3>
            <p>全局表现：平均波段收益 <b style="color:{color_ret}">{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: center;">
                <tr style="background-color: #f2f2f2;">
                    <th>推演日期</th><th>名称(代码)</th><th>潜伏价</th><th>目前价</th><th>真实损益</th><th>当时逻辑与反思</th>
                </tr>
            """
            for r in review_records:
                color = "red" if float(str(r['真实涨跌幅']).replace('%', '')) > 0 else "green"
                review_html += f"""
                <tr>
                    <td><b>{r['推演日']}</b></td>
                    <td>{r['名称']} ({r['代码']})</td>
                    <td>{r['买入价']}</td>
                    <td>{r['当前价']}</td>
                    <td style="color: {color}; font-weight: bold;">{r['真实涨跌幅']}</td>
                    <td style="font-size: 11px; color: #555; text-align: left;">{r['当时逻辑']}</td>
                </tr>
                """
            review_html += "</table><hr>"

        # 2. 大数据赛马榜 (智能顺延展示)
        tournament_html = f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #2980b9; margin-bottom: 20px;">
            <h3 style="margin-top: 0; color: #2980b9;">🏇 八路诸侯波段赛马榜 (近250交易日全景回测)</h3>
            <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: center;">
                <tr style="background-color: #ecf0f1;">
                    <th>战法名称</th><th>长线触发次数</th><th>5日后胜率</th><th>5日平均收益</th><th>5日内平均最大冲高</th>
                </tr>
        """
        
        for s_name, stats in tournament_stats.items():
            trades = stats['trades']
            win_rate = stats['wins'] / trades * 100 if trades > 0 else 0
            avg_ret = sum(stats['returns']) / trades if trades > 0 else 0
            avg_max = sum(stats['max_gains']) / trades if trades > 0 else 0
            
            # 智能顺延的UI展示逻辑
            if s_name == overall_best_strategy and s_name == actual_used_strategy:
                row_style = "background-color: #fff3cd; font-weight: bold; color: #d35400;"
                medal = "🏆 [全年霸主 & 今日实战]"
            elif s_name == overall_best_strategy:
                row_style = "background-color: #fdfbf7; color: #7f8c8d;"
                medal = "👑 [全年霸主 - 今日无标的]"
            elif s_name == actual_used_strategy:
                row_style = "background-color: #d1ecf1; font-weight: bold; color: #2980b9;"
                medal = "🎯 [系统顺延出击]"
            else:
                row_style = ""
                medal = ""
                
            color_ret = "red" if avg_ret > 0 else "green"
            color_max = "red" if avg_max > 0 else "black"
            
            tournament_html += f"""
                <tr style="{row_style}">
                    <td>{s_name} {medal}</td>
                    <td>{trades}次</td>
                    <td>{win_rate:.1f}%</td>
                    <td style="color: {color_ret};">{avg_ret:+.2f}%</td>
                    <td style="color: {color_max}; font-weight: bold;">{avg_max:+.2f}%</td>
                </tr>
            """
        tournament_html += "</table></div>"

        # 3. 选股结论
        top5_html = ""
        if ai_data and "top_5" in ai_data and len(ai_data["top_5"]) > 0:
            top5_html += f"""
            <h3>🧠 总舵主波段定调与打脸检讨</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 检讨与归因：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 波段铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 情绪推演：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🎯 实战出击选股池 (共 {target_count} 只，严格执行【{actual_used_strategy}】)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #2c3e50; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>量化策略标识</th><th>现价</th><th>波段操作计划</th><th>核心逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#e8f4f8; color:#2980b9; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">🥇 {s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🎯 冲高: {s.get('target_price', '')}<br>🛑 防守: {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = f"<p>🧊 极端行情！当前最优顺延策略【{actual_used_strategy}】最终也无达标个股，强制空仓休息！</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #c0392b; border-bottom: 2px solid #c0392b; padding-bottom: 10px;">📉 A股神级赛马雷达：动态顺延轮动 ({today_str})</h2>
            {review_html}
            {tournament_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：尾盘买入潜伏，持有观察 3-5 个交易日。期间一旦达到【平均最大冲高】利润，果断落袋为安，绝不恋战！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【八路智能顺延引擎】今日实战出击：{actual_used_strategy} - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "大数据波段系统"
        msg['From'] = formataddr((Header(sender_name, 'utf-8').encode(), sender))
        msg['To'] = ", ".join(receivers)
        msg.attach(MIMEText(html_content, 'html'))

        try:
            smtp_server = "smtp.qq.com" if "qq.com" in sender else "smtp.163.com" if "163.com" in sender else "smtp.gmail.com"
            port = 465 if smtp_server != "smtp.gmail.com" else 587
            server = smtplib.SMTP_SSL(smtp_server, port)
            server.login(sender, pwd)
            server.sendmail(sender, receivers, msg.as_string())
            server.quit()
            logger.info("✅ 八路诸侯赛马战报邮件发送成功！")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def save_todays_picks(self, top5_stocks, ai_reflection=""):
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_exists = os.path.exists(self.history_file)
        try:
            with open(self.history_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Date_T0', 'Code', 'Name', 'Price_T0', 'Date_T1', 'Price_T1', 'Return_Pct', 'AI_Reason'])
                for s in top5_stocks:
                    strategy_tag = f"[{s.get('strategy', '实战出击策略')}] "
                    ai_ref_short = ai_reflection.replace('\n', ' ')[:40]
                    # 融合具体买入逻辑和AI当时的总定调，方便日后打脸
                    reason_with_ref = f"{strategy_tag} {s['reason']} | [当时定调]: {ai_ref_short}..."
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', reason_with_ref])
        except: pass

    def run_screen(self):
        logger.info("========== 启动【5日波段潜伏·八路诸侯智能顺延引擎】 ==========")
        
        df = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records, recent_stats = self.process_review_and_history(df, lookback_days=5)
            
        logger.info("👉 执行活跃资金池初筛...")
        
        if df['circ_mv'].sum() == 0 and self.pro:
            try:
                cal = self.pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.now() - pd.Timedelta(days=10)).strftime('%Y%m%d'), end_date=datetime.now().strftime('%Y%m%d'))
                last_date = cal.iloc[-1]['cal_date']
                df_basic = self.pro.daily_basic(trade_date=last_date)
                if not df_basic.empty:
                    df_basic['code'] = df_basic['ts_code'].str[:6]
                    df_basic['circ_mv_tushare'] = df_basic['circ_mv'] * 10000
                    df = df.merge(df_basic[['code', 'circ_mv_tushare']], on='code', how='left')
                    df['circ_mv'] = np.where(df['circ_mv'] == 0, df['circ_mv_tushare'].fillna(0), df['circ_mv'])
            except: pass

        df = df[~df['name'].str.contains('ST|退|B')] 
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['close'] >= 2.0] 
        
        if df['circ_mv'].sum() > 0:
            df = df[(df['circ_mv'] >= 30_0000_0000) & (df['circ_mv'] <= 500_0000_0000)]
        df = df[df['amount'] >= 200000000]
        
        candidates = df.sort_values(by='amount', ascending=False).head(100)
        logger.info(f"👉 锁定 {len(candidates)} 只主战场标的，启动八大波段战法极速矩阵推演...")

        tournament_stats = {
            '战法A: 趋势低吸': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法B: 底部起爆': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法C: 强庄首阴': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法D: 均线粘合': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法E: 龙头断板': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法F: N字反包': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法G: 新高突破': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []},
            '战法H: 缩量双底': {'trades': 0, 'wins': 0, 'returns': [], 'max_gains': []}
        }
        
        today_signals = {} 
        total_c = len(candidates)
        lookback_days = 250
        
        consecutive_errors = 0
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if consecutive_errors >= 6:
                logger.error("🚨 连续6次获取K线失败！已触发防爬虫封锁！启动【终极熔断】，直接跳过剩余标的！")
                break
                
            if i % 20 == 0: logger.info(f"⏳ 12个月八大矩阵推演中... 进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                df_kline = self._get_daily_kline(code)
                if df_kline is None or len(df_kline) < 60: 
                    consecutive_errors += 1
                    time.sleep(1)
                    continue
                    
                consecutive_errors = 0
                
                tech_df = self.calculate_technical_indicators(df_kline)
                sig_df = self.evaluate_strategies(tech_df)
                
                actual_lookback = min(lookback_days, len(sig_df) - 60)
                if actual_lookback < 10: continue 
                
                test_df = sig_df.iloc[-(actual_lookback+5):-5].copy()
                
                test_df['Ret_5D'] = ((test_df['Close_T5'] - test_df['收盘']) / test_df['收盘'] - 0.003) * 100
                test_df['Max_Gain'] = ((test_df['High_5D'] - test_df['收盘']) / test_df['收盘']) * 100
                
                strategy_keys = [
                    ('战法A: 趋势低吸', 'Sig_A_Trend_Pullback'), 
                    ('战法B: 底部起爆', 'Sig_B_Bottom_Breakout'), 
                    ('战法C: 强庄首阴', 'Sig_C_Strong_Dip'), 
                    ('战法D: 均线粘合', 'Sig_D_MA_Squeeze'),
                    ('战法E: 龙头断板', 'Sig_E_Dragon_Relay'),
                    ('战法F: N字反包', 'Sig_F_N_Shape'),
                    ('战法G: 新高突破', 'Sig_G_ATH_Breakout'),
                    ('战法H: 缩量双底', 'Sig_H_Double_Bottom')
                ]
                
                for s_key, col_name in strategy_keys:
                    trades = test_df[test_df[col_name]]
                    if not trades.empty:
                        valid_rets = trades['Ret_5D'].dropna()
                        valid_maxs = trades['Max_Gain'].dropna()
                        if not valid_rets.empty:
                            tournament_stats[s_key]['trades'] += len(valid_rets)
                            tournament_stats[s_key]['returns'].extend(valid_rets.tolist())
                            tournament_stats[s_key]['max_gains'].extend(valid_maxs.tolist())
                            tournament_stats[s_key]['wins'] += (valid_rets > 0).sum()
                
                last = sig_df.iloc[-1]
                v_ratio = (last['成交量'] / last['Vol_MA5']) if last['Vol_MA5'] > 0 else 1.0
                
                today_signals[code] = {
                    'name': name, 'price': last['收盘'], 'pct': row['pct_chg'], 'amount': row['amount'], 'v_ratio': v_ratio,
                    'sig_A': last['Sig_A_Trend_Pullback'], 'sig_B': last['Sig_B_Bottom_Breakout'],
                    'sig_C': last['Sig_C_Strong_Dip'], 'sig_D': last['Sig_D_MA_Squeeze'],
                    'sig_E': last['Sig_E_Dragon_Relay'], 'sig_F': last['Sig_F_N_Shape'],
                    'sig_G': last['Sig_G_ATH_Breakout'], 'sig_H': last['Sig_H_Double_Bottom']
                }
                time.sleep(random.uniform(0.05, 0.1))
            except Exception: continue

        # =========================================================
        # 🏆 智能顺延选股机制：胜率排名 -> 依次寻找可用标的
        # =========================================================
        ranked_strategies = []
        for s_name, stats in tournament_stats.items():
            trades = stats['trades']
            if trades >= 10: 
                win_rate = stats['wins'] / trades
                avg_ret = sum(stats['returns']) / trades
                avg_max = sum(stats['max_gains']) / trades
                score = win_rate * avg_ret
                ranked_strategies.append({
                    'name': s_name, 'score': score, 'trades': trades,
                    'win_rate': win_rate, 'avg_ret': avg_ret, 'avg_max': avg_max
                })
        
        # 按得分从高到低排序
        ranked_strategies.sort(key=lambda x: x['score'], reverse=True)
        
        overall_best_strategy = ranked_strategies[0]['name'] if ranked_strategies else None
        actual_used_strategy = None
        best_reason = ""
        final_pool = []
        
        sig_map = {
            '战法A: 趋势低吸': 'sig_A', '战法B: 底部起爆': 'sig_B',
            '战法C: 强庄首阴': 'sig_C', '战法D: 均线粘合': 'sig_D',
            '战法E: 龙头断板': 'sig_E', '战法F: N字反包': 'sig_F',
            '战法G: 新高突破': 'sig_G', '战法H: 缩量双底': 'sig_H'
        }

        # 核心逻辑：从最高分的策略开始找，如果今天没股票触发，就自动找第二名、第三名...
        for st in ranked_strategies:
            s_name = st['name']
            
            # 如果分数已经是负数（全部在亏钱），放弃进攻，直接跳出循环走防守逻辑
            if st['score'] < 0:
                continue

            target_sig_key = sig_map[s_name]
            temp_pool = []
            
            for code, info in today_signals.items():
                if info[target_sig_key]:
                    temp_pool.append({
                        "代码": code, "名称": info['name'], "现价": info['price'],
                        "匹配策略": f"{s_name}", "今日涨幅": f"{info['pct']:.2f}%", 
                        "量比": f"{info['v_ratio']:.2f}", "成交额": f"{info['amount']/100000000:.1f}亿",
                        "sort_score": info['amount'] 
                    })
            
            # 如果这个高分策略选出股票了，它就是我们今天最终要用的策略！
            if temp_pool:
                actual_used_strategy = s_name
                final_pool = temp_pool
                
                if s_name == overall_best_strategy:
                    best_reason = f"一统江湖！全年霸主战法今日成功触发。近12个月全景回测触发{st['trades']}次，5天胜率{st['win_rate']*100:.1f}%，平均最大冲高{st['avg_max']:+.2f}%！"
                else:
                    best_reason = f"智能顺延出击！全年霸主【{overall_best_strategy}】今日无标的，系统向下切换至最优可行战法【{s_name}】：5天胜率{st['win_rate']*100:.1f}%，平均最大冲高{st['avg_max']:+.2f}%！"
                break
                
        # 如果所有赚钱的策略今天都选不出股票（或者全在亏钱），强制启用最后一道防线
        if not actual_used_strategy:
            logger.warning("🚨 极寒冰点或所有优势战法均无标的，强制开启深海防守。")
            actual_used_strategy = '战法H: 缩量双底'
            overall_best_strategy = overall_best_strategy or actual_used_strategy
            best_reason = "12个月历史大数据显示全部进攻策略吃面或今日轮空，强制切入最保守的极度缩量双底防守形态保命。"
            
            target_sig_key = sig_map[actual_used_strategy]
            for code, info in today_signals.items():
                if info[target_sig_key]:
                    final_pool.append({
                        "代码": code, "名称": info['name'], "现价": info['price'],
                        "匹配策略": f"🛡️ {actual_used_strategy}", "今日涨幅": f"{info['pct']:.2f}%", 
                        "量比": f"{info['v_ratio']:.2f}", "成交额": f"{info['amount']/100000000:.1f}亿",
                        "sort_score": info['amount'] 
                    })

        logger.info(f"🎯 今日锁定实战出击策略: 【{actual_used_strategy}】 ({best_reason})")

        if final_pool:
            final_pool = sorted(final_pool, key=lambda x: x['sort_score'], reverse=True)
            final_top_stocks = final_pool[:5]
        else:
            final_top_stocks = []
            
        self.target_count = len(final_top_stocks)
        
        ai_result = None
        if final_top_stocks:
            macro_news = self.fetch_macro_news()
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, actual_used_strategy, best_reason, review_summary)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"], ai_reflection=ai_result.get("ai_reflection", ""))
                self.save_ai_lesson(ai_result.get("new_lesson_learned", ""))
        
        self.send_email_report(ai_result, tournament_stats, overall_best_strategy, actual_used_strategy, self.target_count, review_records, recent_stats)
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
