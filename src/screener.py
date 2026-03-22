# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 动态EV期望 + 全局实盘强制核算 (霸体终极版)
===================================

核心重构:
1. 【解开护盾束缚】：放宽防量化收割条件，修复均线 NaN 屏蔽，满血恢复数千次双盲回测样本。
2. 【实盘胜率穿透】：只要历史买过1次，强制展示真实打脸胜率，消灭“样本不足”盲区！
3. 【老名称无损融合】：智能识别旧版 CSV 里的 [强庄首阴]、[缩量回踩低吸]，完美对齐新版八大门派。
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

socket.setdefaulttimeout(15.0)

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

    def fetch_top_sectors(self):
        try:
            df = ak.stock_board_industry_name_em()
            top_sectors = df.head(5)['板块名称'].tolist()
            return ", ".join(top_sectors)
        except: return "未知"

    def fetch_market_trend(self):
        try:
            sh_index = ak.stock_zh_index_daily_em(symbol="sh000001")
            if not sh_index.empty and len(sh_index) >= 20:
                sh_close = sh_index['close'].iloc[-1]
                sh_ma20 = sh_index['close'].tail(20).mean()
                if sh_close < sh_ma20:
                    return False, f"⚠️ 上证指({sh_close:.0f})已跌破MA20({sh_ma20:.0f})！大环境为【空头震荡】，强行压降进攻仓位！"
                else:
                    return True, f"✅ 上证指({sh_close:.0f})稳站MA20({sh_ma20:.0f})之上！大环境为【多头趋势】，题材可积极博弈！"
        except: pass
        return True, "大盘趋势未知，按中性对待。"

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
            lesson = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]:?\s*', '', lesson).strip()
            with open(self.lessons_file, 'a', encoding='utf-8') as f:
                date_str = datetime.now().strftime('%Y-%m-%d')
                f.write(f"[{date_str} 铁律]: {lesson}\n")
        except: pass

    def process_review_and_history(self, market_df, lookback_batches=5):
        today_date = datetime.now()
        today_str = today_date.strftime('%Y-%m-%d')
        
        global_stats = {"total_trades": 0, "win_rate": 0.0, "avg_ret": 0.0}
        recent_stats = {"avg_ret": 0.0, "win_rate": 0.0, "days": 0, "total_count": 0}
        review_records = []
        ai_feedback_data = {"best": [], "worst": []}
        strategy_real_performance = {}

        if not os.path.exists(self.history_file): 
            return "", review_records, recent_stats, global_stats, ai_feedback_data, strategy_real_performance

        try:
            df_hist = pd.read_csv(self.history_file)
            if df_hist.empty: return "", review_records, recent_stats, global_stats, ai_feedback_data, strategy_real_performance
            
            df_hist['Realized_Ret'] = np.nan
            logger.info(f"🔍 正在执行全局沙盘推演，核算历史所有金股真实盈亏...")
            
            today_prices = market_df.set_index('code')['close'].to_dict()
            
            for idx, row in df_hist.iterrows():
                code = str(row['Code']).zfill(6)
                t0_price = float(row['Price_T0'])
                
                if pd.isna(row.get('Date_T1')) or str(row.get('Date_T1')).strip() == '':
                    try:
                        t0_date = datetime.strptime(str(row['Date_T0']), '%Y-%m-%d')
                        days_held = (today_date - t0_date).days
                    except: days_held = 0
                    
                    if code in today_prices and t0_price > 0:
                        curr_p = float(today_prices[code])
                        ret_pct = ((curr_p - t0_price) / t0_price) * 100
                        
                        if days_held >= 7:
                            df_hist.at[idx, 'Date_T1'] = today_str
                            df_hist.at[idx, 'Price_T1'] = curr_p
                            df_hist.at[idx, 'Return_Pct'] = round(ret_pct, 2)
                            df_hist.at[idx, 'Realized_Ret'] = ret_pct
                        else:
                            df_hist.at[idx, 'Floating_Ret'] = round(ret_pct, 2)
                            df_hist.at[idx, 'Realized_Ret'] = ret_pct
                else:
                    if pd.notna(row.get('Return_Pct')):
                        df_hist.at[idx, 'Realized_Ret'] = float(row['Return_Pct'])
            
            df_hist.drop(columns=['Floating_Ret', 'Realized_Ret'], errors='ignore').to_csv(self.history_file, index=False)
            
            df_hist['Strategy_Name'] = df_hist['AI_Reason'].astype(str).str.extract(r'\[(.*?)\]')
            valid_df = df_hist.dropna(subset=['Realized_Ret']).copy()
            
            if not valid_df.empty:
                global_stats['total_trades'] = len(valid_df)
                global_stats['win_rate'] = (valid_df['Realized_Ret'] > 0).mean() * 100
                global_stats['avg_ret'] = valid_df['Realized_Ret'].mean()
                
                strat_group = valid_df.groupby('Strategy_Name')['Realized_Ret'].agg(['count', 'mean', lambda x: (x>0).mean()*100]).reset_index()
                strat_group.columns = ['Strategy', 'Count', 'Avg_Ret', 'Win_Rate']
                for _, s_row in strat_group.iterrows():
                    strategy_real_performance[str(s_row['Strategy'])] = {
                        'count': s_row['Count'], 'win_rate': s_row['Win_Rate'], 'avg_ret': s_row['Avg_Ret']
                    }
                
                best_3 = valid_df.nlargest(3, 'Realized_Ret')
                worst_3 = valid_df.nsmallest(3, 'Realized_Ret')
                ai_feedback_data['best'] = best_3[['Name', 'Strategy_Name', 'Realized_Ret']].to_dict('records')
                ai_feedback_data['worst'] = worst_3[['Name', 'Strategy_Name', 'Realized_Ret']].to_dict('records')
                
                recent_dates = sorted(valid_df['Date_T0'].unique())[-lookback_batches:]
                recent_records = valid_df[valid_df['Date_T0'].isin(recent_dates)]
                
                if not recent_records.empty:
                    recent_stats['total_count'] = len(recent_records)
                    recent_stats['win_rate'] = (recent_records['Realized_Ret'] > 0).mean() * 100
                    recent_stats['avg_ret'] = recent_records['Realized_Ret'].mean()
                    recent_stats['days'] = len(recent_dates)
                    
                    review_summary = f"【AI全局打脸与进化沙盘】\n"
                    review_summary += f"历史总推演 {global_stats['total_trades']} 标的，全局总胜率: {global_stats['win_rate']:.1f}%，总平均收益: {global_stats['avg_ret']:+.2f}%。\n"
                    review_summary += f"近期 {len(recent_dates)} 批次推演胜率: {recent_stats['win_rate']:.1f}%，收益: {recent_stats['avg_ret']:+.2f}%。\n"
                    
                    for date in recent_dates:
                        day_df = recent_records[recent_records['Date_T0'] == date]
                        for _, r in day_df.iterrows():
                            t0_p = float(r['Price_T0'])
                            ret_val = float(r['Realized_Ret'])
                            curr_p = r.get('Price_T1')
                            if pd.isna(curr_p) or str(curr_p).strip() == '':
                                curr_p = t0_p * (1 + ret_val / 100)
                                
                            review_records.append({
                                "推演日": r['Date_T0'], "代码": str(r['Code']).zfill(6), "名称": r['Name'], 
                                "买入价": round(t0_p, 2), "当前价": round(float(curr_p), 2), "真实涨跌幅": f"{ret_val:+.2f}%", 
                                "当时逻辑": str(r.get('AI_Reason', ''))[:60]
                            })
                            
        except Exception as e:
            logger.error(f"全局推演复盘异常: {e}")
            
        return review_summary, review_records, recent_stats, global_stats, ai_feedback_data, strategy_real_performance

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
        
        df['Ret_20d'] = df['收盘'].pct_change(20) * 100
        
        df['Std_20'] = df['收盘'].rolling(20).std()
        df['BB_Up'] = df['MA20'] + 2 * df['Std_20']
        
        df['prev_close'] = df['收盘'].shift(1)
        tr1 = df['最高'] - df['最低']
        tr2 = (df['最高'] - df['prev_close']).abs()
        tr3 = (df['最低'] - df['prev_close']).abs()
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['ATR'] = df['TR'].rolling(14, min_periods=1).mean()
        df['ATR_Pct'] = (df['ATR'] / df['收盘']) * 100
        
        df['CPV'] = (df['收盘'] - df['最低']) / (df['最高'] - df['最低'] + 0.0001)
        
        df['Body'] = abs(df['收盘'] - df['开盘'])
        df['Upper_Shadow'] = df['最高'] - df[['收盘', '开盘']].max(axis=1)
        df['Avg_Body_5d'] = df['Body'].rolling(5).mean() + 0.001
        df['Avg_Upper_5d'] = df['Upper_Shadow'].rolling(5).mean()

        df['Close_T5'] = df['收盘'].shift(-5)
        df['High_5D'] = df['最高'].shift(-1)[::-1].rolling(5, min_periods=1).max()[::-1]
        df['Low_5D'] = df['最低'].shift(-1)[::-1].rolling(5, min_periods=1).min()[::-1]
        
        df['Pct_Chg_Shift1'] = df['涨跌幅'].shift(1)
        df['Pct_Chg_Shift2'] = df['涨跌幅'].shift(2)
        df['Pct_Chg_Shift3'] = df['涨跌幅'].shift(3)
        df['Vol_Shift3'] = df['成交量'].shift(3)
        df['Open_Shift3'] = df['开盘'].shift(3)
        
        return df

    def evaluate_strategies(self, df):
        # 🚀 放宽基础健康过滤：允许弱势盘整，防止误杀导致0回测
        s_momentum_ok = df['Ret_20d'].fillna(0) > -15.0 
        
        # 🚀 放宽防量化护盾：允许上影线达到实体的 3 倍（A股常态洗盘）
        s_anti_harvest = df['Avg_Upper_5d'] < (df['Avg_Body_5d'] * 3.0)
        
        # 🚀 放宽布林带过热防守：突破上轨 5% 才算绝对泡沫
        s_not_overbought = df['收盘'] < (df['BB_Up'].fillna(float('inf')) * 1.05)
        
        global_shield = s_momentum_ok & s_anti_harvest & s_not_overbought
        
        sA_trend = df['MA20'] > df['MA60']
        sA_support = (abs(df['收盘'] - df['MA20']) / df['MA20']) <= 0.03
        sA_vol = df['成交量'] < df['Vol_MA5'] * 0.8
        sA_cpv = df['CPV'] > 0.2 # 只要不是极端的死在最低点即可
        df['Sig_A_Trend_Pullback'] = sA_trend & sA_support & sA_vol & sA_cpv & global_shield

        sB_base = df['收盘'].shift(1) < df['MA60'].shift(1)
        sB_break = df['收盘'] > df['MA60']
        sB_vol = df['成交量'] > df['Vol_MA5'] * 2.0
        sB_pct = df['涨跌幅'] > 4.0
        sB_cpv = df['CPV'] > 0.6 
        df['Sig_B_Bottom_Breakout'] = sB_base & sB_break & sB_vol & sB_pct & sB_cpv & global_shield

        sC_gene = df['Max_Pct_10d'] > 8.0
        sC_pct = (df['涨跌幅'] < 0) & (df['涨跌幅'] >= -6.0)
        sC_vol = df['成交量'] < df['Vol_MA5'] * 0.7
        sC_cpv = df['CPV'] > 0.2
        df['Sig_C_Strong_Dip'] = sC_gene & sC_pct & sC_vol & sC_cpv & global_shield

        ma_max = df[['MA5', 'MA10', 'MA20']].max(axis=1)
        ma_min = df[['MA5', 'MA10', 'MA20']].min(axis=1)
        sD_squeeze = (ma_max - ma_min) / ma_min < 0.03 
        sD_up = (df['收盘'] > ma_max) & (df['开盘'] < ma_min) & (df['涨跌幅'] > 3.0)
        df['Sig_D_MA_Squeeze'] = sD_squeeze & sD_up & global_shield
        
        sE_gene = df['Pct_Chg_Shift1'] > 9.0
        sE_pct = (df['涨跌幅'] > -5.0) & (df['涨跌幅'] < 4.0)
        sE_vol = df['成交量'] > df['Vol_MA5'] * 1.5
        sE_cpv = df['CPV'] > 0.4 
        df['Sig_E_Dragon_Relay'] = sE_gene & sE_pct & sE_vol & sE_cpv & global_shield
        
        sF_day3 = df['Pct_Chg_Shift3'] > 6.0
        sF_day21 = (df['Pct_Chg_Shift2'] < 2.0) & (df['Pct_Chg_Shift1'] < 2.0) & (df['收盘'].shift(1) > df['Open_Shift3'])
        sF_today = df['涨跌幅'] > 0
        sF_vol = df['成交量'] < df['Vol_Shift3']
        df['Sig_F_N_Shape'] = sF_day3 & sF_day21 & sF_today & sF_vol & global_shield
        
        sG_high = df['收盘'] >= df['High_120d_shift']
        sG_vol = df['成交量'] > df['Vol_MA5'] * 2.0
        sG_pct = df['涨跌幅'] > 4.0
        sG_cpv = df['CPV'] > 0.7 
        df['Sig_G_ATH_Breakout'] = sG_high & sG_vol & sG_pct & sG_cpv & global_shield
        
        sH_low = (df['收盘'] - df['Min_20d']) / df['Min_20d'] < 0.05
        sH_macd = df['MACD'] > df['MACD'].shift(5)
        sH_vol = df['成交量'] < df['Vol_MA5'] * 0.7
        df['Sig_H_Double_Bottom'] = sH_low & sH_macd & sH_vol

        return df

    def ai_select_top5(self, candidates, macro_news, actual_used_strategy, strategy_reason, review_summary, market_stats, top_sectors, is_market_safe, ai_feedback_data, global_stats):
        logger.info(f"🧠 正在唤醒 AI 执行今日实战出击策略并提取反馈基因...")
        if not self.config.gemini_api_key: return {"top_5": []}

        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | 策略:{c['匹配策略']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | K线重心:{c['重心CPV']}\n"

        worst_text = "\n".join([f"亏损 {x['Realized_Ret']:.2f}% (使用策略: {x['Strategy_Name']})" for x in ai_feedback_data.get('worst', [])])
        best_text = "\n".join([f"盈利 {x['Realized_Ret']:.2f}% (使用策略: {x['Strategy_Name']})" for x in ai_feedback_data.get('best', [])])

        prompt = f"""你是一位A股神级游资总舵主。正在执行极度冷酷的自我反思和量化选股！
根据【双盲期望值(EV)赛马】与【实盘胜率动态惩罚】，今日系统锁定出击的波段策略是：【{actual_used_strategy}】！
出击理由：{strategy_reason}。

### 🚨 你的历史实盘战绩红黑榜 (极其重要，逼迫你进化)：
全局总推演胜率: {global_stats.get('win_rate', 0):.1f}%，总平均收益: {global_stats.get('avg_ret', 0):+.2f}%。
💀【历史最惨教训 (你曾经选出的大雷)】: 
{worst_text or '暂无'}
🌟【历史最强盈利 (你曾经抓到的妖股)】: 
{best_text or '暂无'}

### 🌊 今日大盘环境：
两市总成交额 {market_stats.get('total_amount', 0):.0f} 亿元。
今日主线风口：{top_sectors}
大盘技术面：{"【安全】大盘在MA20之上，积极做多" if is_market_safe else "【危险】大盘已跌破MA20！宁可空仓绝不逆势凑数！"}

### 🧠 你的历史避坑记忆：
{past_lessons}

### 📊 实战备选池 (请用放大镜审视，最多选5只，宁缺毋滥！)：
{cand_text}

### 🎯 你的任务：
1. 结合“红黑榜历史教训”和“大盘环境”，在 `ai_reflection` 里进行极其深刻的反省（为什么最惨的票会亏？是不是追高了或者接飞刀了？）
2. 优选最多 5 只股票（如果大盘破位或觉得备选池都是垃圾，你可以只输出 1 只甚至 0 只！绝对不要凑数！）
3. 设定严格的 `stop_loss`。
4. 严禁在输出文本中自行编造或添加形如 [2026-xx-xx] 的日期。

请严格输出 JSON 格式：
```json
{{
    "ai_reflection": "深刻结合【红黑榜最惨教训】与大盘流动性进行的自我批评与反思...",
    "new_lesson_learned": "根据红黑榜总结出的实战避坑铁律(纯正文，严禁加日期)",
    "macro_view": "大盘情绪推演...",
    "top_5": [
        {{
            "code": "代码",
            "name": "名称",
            "strategy": "原样保留",
            "current_price": 现价,
            "reason": "入选逻辑（必须说明它为何避开了历史亏损雷区，并带有盈利基因）",
            "target_price": "波段目标价",
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

    def send_email_report(self, ai_data, tournament_stats, overall_best_strategy, actual_used_strategy, target_count, review_records, recent_stats, market_stats, top_sectors, is_market_safe, market_trend_desc, global_stats):
        logger.info("📧 正在生成最终幻想版赛马战报邮件...")
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        if not sender or not pwd: return

        today_str = datetime.now().strftime('%Y-%m-%d')
        total_vol = market_stats.get('total_amount', 0)
        limit_up = market_stats.get('limit_up', 0)
        limit_down = market_stats.get('limit_down', 0)
        trend_color = "#27ae60" if is_market_safe else "#c0392b"
        
        market_html = f"""
        <div style="background-color: #f1f2f6; padding: 10px; margin-bottom: 15px; border-radius: 5px; text-align: center; font-size: 14px;">
            🌡️ <b>今日全市场水温</b>：上涨 {market_stats.get('up',0)} 家 | 下跌 {market_stats.get('down',0)} 家 | 涨停 <span style="color:red;">{limit_up}</span> 家 | 跌停 <span style="color:green;">{limit_down}</span> 家<br>
            🌊 <b>真实总流动性</b>：<span style="color:#2980b9; font-weight:bold;">{total_vol:.0f} 亿元</span><br>
            🔥 <b>今日主线风口</b>：<span style="color:#d35400; font-weight:bold;">{top_sectors}</span><br>
            📉 <b>大盘择时状态</b>：<span style="color:{trend_color}; font-weight:bold;">{market_trend_desc}</span>
        </div>
        """

        global_ret = global_stats.get('avg_ret', 0.0)
        g_color = "red" if global_ret > 0 else "green"
        review_html = f"""
        <div style="border: 2px solid #34495e; border-radius: 5px; margin-bottom: 20px;">
            <div style="background-color: #34495e; color: white; padding: 8px 15px; font-weight: bold;">
                📊 全局实盘跟踪沙盘 (自雷达上线以来)
            </div>
            <div style="padding: 10px; display: flex; justify-content: space-around; background-color: #fafafa;">
                <div style="text-align: center;">总推演: <b style="font-size: 16px;">{global_stats.get('total_trades', 0)}</b> 支</div>
                <div style="text-align: center;">总胜率: <b style="font-size: 16px; color:#8e44ad;">{global_stats.get('win_rate', 0):.1f}%</b></div>
                <div style="text-align: center;">实盘总期望收益: <b style="font-size: 16px; color:{g_color};">{global_ret:+.2f}%</b></div>
            </div>
        </div>
        """

        if review_records:
            avg_ret = recent_stats.get('avg_ret', 0.0)
            win_rate = recent_stats.get('win_rate', 0.0)
            days = recent_stats.get('days', 1)
            color_ret = "red" if avg_ret > 0 else "green" if avg_ret < 0 else "black"
            
            review_html += f"""
            <h3>⚖️ 近期实况与打脸处刑台 (近 {days} 批次核算)</h3>
            <p>近期表现：平均波段收益 <b style="color:{color_ret}">{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: center;">
                <tr style="background-color: #f2f2f2;">
                    <th>推演日期</th><th>名称(代码)</th><th>潜伏价</th><th>目前价</th><th>实盘损益</th><th>当时逻辑与反思</th>
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

        tournament_html = f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #2980b9; margin-bottom: 20px;">
            <h3 style="margin-top: 0; color: #2980b9;">🏇 八大波段赛马榜 (期望值EV + 实盘胜率双重惩罚机制)</h3>
            <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: center;">
                <tr style="background-color: #ecf0f1;">
                    <th>战法名称</th><th>理论胜率(短/长)</th><th>实盘验证胜率</th><th>理论EV期望</th><th>惩罚后打分</th>
                </tr>
        """
        
        for s_name, stats in tournament_stats.items():
            win_rate = stats.get('win_rate', 0.0)
            win_rate_15d = stats.get('win_rate_15d', 0.0)
            real_win_rate = stats.get('real_win_rate', -1.0)
            ev = stats.get('ev', 0.0)
            final_score = stats.get('score', 0.0)
            is_banned = stats.get('is_banned', False)
            
            if is_banned:
                row_style = "background-color: #ecf0f1; color: #95a5a6; text-decoration: line-through;"
                medal = "⛔ [实盘拉跨熔断]"
            elif s_name == overall_best_strategy and s_name == actual_used_strategy:
                row_style = "background-color: #fff3cd; font-weight: bold; color: #d35400;"
                medal = "🏆 [霸主&实战]"
            elif s_name == overall_best_strategy:
                row_style = "background-color: #fdfbf7; color: #7f8c8d;"
                medal = "👑 [霸主-今日轮空]"
            elif s_name == actual_used_strategy:
                row_style = "background-color: #d1ecf1; font-weight: bold; color: #2980b9;"
                medal = "🎯 [顺延出击]"
            else:
                row_style = ""
                medal = ""
                
            color_ev = "red" if ev > 0 and not is_banned else "green" if ev <= 0 and not is_banned else "gray"
            
            real_win_str = f"{real_win_rate*100:.1f}%" if real_win_rate >= 0 else "样本不足"
            if real_win_rate >= 0 and real_win_rate < 0.35: real_win_str = f"<span style='color:green;font-weight:bold;'>{real_win_str} (严重失真)</span>"
            
            tournament_html += f"""
                <tr style="{row_style}">
                    <td>{s_name} {medal}</td>
                    <td>{win_rate_15d*100:.1f}% / {win_rate*100:.1f}%</td>
                    <td>{real_win_str}</td>
                    <td style="color: {color_ev}; font-weight: bold;">{ev:+.2f}</td>
                    <td style="font-weight: bold;">{final_score:.2f}</td>
                </tr>
            """
        tournament_html += "</table></div>"

        top5_html = ""
        if ai_data and "top_5" in ai_data and len(ai_data["top_5"]) > 0:
            target_kelly = tournament_stats.get(actual_used_strategy, {}).get('kelly_pct', 0.0)
            
            clean_reflection = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]:?\s*', '', str(ai_data.get('ai_reflection', '无')))
            clean_lesson = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]:?\s*', '', str(ai_data.get('new_lesson_learned', '无')))
            clean_macro = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]:?\s*', '', str(ai_data.get('macro_view', '无')))
            
            top5_html += f"""
            <h3>🧠 顶级游资 AI：绝地反思与风口研判</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>⚖️ 凯利系统指令：</b>基于EV期望与实盘打脸情况，今日单只个股下注仓位上限严格控制在 <b style="color:red; font-size:16px;">{target_kelly:.1f}%</b>！</p>
                <p><b>🔄 处刑后深刻归因：</b>{clean_reflection}</p>
                <p><b>🔴 血泪避坑铁律：</b><span style="color:red; font-weight:bold;">{clean_lesson}</span></p>
                <p><b>🌍 情绪推演：</b>{clean_macro}</p>
            </div>
            
            <h3>🎯 终极筛选实战出击池 (共 {target_count} 只，严格执行【{actual_used_strategy}】)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #2c3e50; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>策略标识</th><th>现价</th><th>波段防守计划</th><th>进化后的核心逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#e8f4f8; color:#2980b9; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">🥇 {s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🎯 止盈: {s.get('target_price', '')}<br>🛑 割肉: {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = f"<p>🧊 极端冰点！受限于【实盘连亏惩罚】或【大盘破位】，AI 判定当前备选池全为绞肉机，强制剥夺开仓权，空仓保命！</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #c0392b; border-bottom: 2px solid #c0392b; padding-bottom: 10px;">📉 A股终极量化：全景实盘追踪 + 动态降维惩罚 ({today_str})</h2>
            {market_html}
            {review_html}
            {tournament_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：所有推荐建立在 AI 血泪实盘回测之上！宁可踏空，绝不送钱！严格执行给定的割肉与止盈线！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【实盘进化版轮动】今日决战兵器：{actual_used_strategy} - {today_str}", 'utf-8')
        
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
            logger.info("✅ 终极幻想版赛马战报邮件发送成功！")
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
                    ai_ref_clean = re.sub(r'\[\d{4}-\d{2}-\d{2}.*?\]:?\s*', '', ai_reflection)
                    ai_ref_short = ai_ref_clean.replace('\n', ' ')[:40]
                    reason_with_ref = f"{strategy_tag} {s['reason']} | [AI当时定调]: {ai_ref_short}..."
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', reason_with_ref])
        except: pass

    def run_screen(self):
        logger.info("========== 启动【5日波段潜伏·全局实盘追踪终极形态】 ==========")
        
        df = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records, recent_stats, global_stats, ai_feedback_data, strategy_real_performance = self.process_review_and_history(df, lookback_batches=5)
            
        logger.info("👉 执行全市场流动性与风口感知...")
        top_sectors = self.fetch_top_sectors()
        total_amount_yi = df['amount'].sum() / 1_0000_0000
        
        limit_down_count = len(df[df['pct_chg'] <= -9.5])
        limit_up_count = len(df[df['pct_chg'] >= 9.5])
        up_count = len(df[df['pct_chg'] > 0])
        down_count = len(df[df['pct_chg'] < 0])
        market_stats = {'up': up_count, 'down': down_count, 'limit_up': limit_up_count, 'limit_down': limit_down_count, 'total_amount': total_amount_yi}
        
        is_market_safe, market_trend_desc = self.fetch_market_trend()
        
        is_market_crash = limit_down_count >= 50
        if is_market_crash:
            logger.error(f"🚨 极其恶劣！全市场跌停达 {limit_down_count} 家！触发股灾核按钮熔断！")

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
        logger.info(f"👉 锁定 {len(candidates)} 只主战场标的，启动防伪双向截断回测...")

        tournament_stats = {
            '战法A: 趋势低吸': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法B: 底部起爆': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法C: 强庄首阴': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法D: 均线粘合': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法E: 龙头断板': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法F: N字反包': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法G: 新高突破': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False},
            '战法H: 缩量双底': {'trades': 0, 'wins': 0, 'returns': [], 'trades_15d': 0, 'wins_15d': 0, 'returns_15d': [], 'is_banned': False}
        }
        
        today_signals = {} 
        total_c = len(candidates)
        lookback_days = 250
        consecutive_errors = 0
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if consecutive_errors >= 10:
                logger.error("🚨 连续10次获取K线失败！触发终极网络熔断，跳过剩余！")
                break
                
            if i % 20 == 0: logger.info(f"⏳ 真实执行(EV)矩阵推演中... 进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                df_kline = self._get_daily_kline(code)
                
                if df_kline is None or df_kline.empty: 
                    consecutive_errors += 1
                    time.sleep(1)
                    continue
                    
                consecutive_errors = 0 
                
                if len(df_kline) < 60:
                    continue 
                    
                tech_df = self.calculate_technical_indicators(df_kline)
                sig_df = self.evaluate_strategies(tech_df)
                
                actual_lookback = min(lookback_days, len(sig_df) - 60)
                if actual_lookback < 15: continue 
                
                test_df = sig_df.iloc[-(actual_lookback+5):-5].copy()
                
                test_df['Stop_Pct'] = (-1.5 * test_df['ATR_Pct']).clip(-15.0, -3.0)
                test_df['Target_Pct'] = (2.5 * test_df['ATR_Pct']).clip(5.0, 20.0)
                
                test_df['Low_5D_Pct'] = ((test_df['Low_5D'] - test_df['收盘']) / test_df['收盘']) * 100
                test_df['High_5D_Pct'] = ((test_df['High_5D'] - test_df['收盘']) / test_df['收盘']) * 100
                test_df['Raw_Ret_5D'] = ((test_df['Close_T5'] - test_df['收盘']) / test_df['收盘'] - 0.003) * 100
                
                test_df['Ret_5D'] = np.where(
                    test_df['Low_5D_Pct'] <= test_df['Stop_Pct'], 
                    test_df['Stop_Pct'] - 0.5, 
                    np.where(
                        test_df['High_5D_Pct'] >= test_df['Target_Pct'],
                        test_df['Target_Pct'] - 0.2, 
                        test_df['Raw_Ret_5D']
                    )
                )
                
                test_df_15 = test_df.tail(15)

                strategy_keys = [
                    ('战法A: 趋势低吸', 'Sig_A_Trend_Pullback'), ('战法B: 底部起爆', 'Sig_B_Bottom_Breakout'), 
                    ('战法C: 强庄首阴', 'Sig_C_Strong_Dip'), ('战法D: 均线粘合', 'Sig_D_MA_Squeeze'),
                    ('战法E: 龙头断板', 'Sig_E_Dragon_Relay'), ('战法F: N字反包', 'Sig_F_N_Shape'),
                    ('战法G: 新高突破', 'Sig_G_ATH_Breakout'), ('战法H: 缩量双底', 'Sig_H_Double_Bottom')
                ]
                
                for s_key, col_name in strategy_keys:
                    trades = test_df[test_df[col_name]]
                    trades_15 = test_df_15[test_df_15[col_name]]
                    
                    if not trades.empty:
                        valid_rets = trades['Ret_5D'].dropna()
                        if not valid_rets.empty:
                            tournament_stats[s_key]['trades'] += len(valid_rets)
                            tournament_stats[s_key]['returns'].extend(valid_rets.tolist())
                            tournament_stats[s_key]['wins'] += (valid_rets > 0).sum()
                    
                    if not trades_15.empty:
                        valid_rets_15 = trades_15['Ret_5D'].dropna()
                        if not valid_rets_15.empty:
                            tournament_stats[s_key]['trades_15d'] += len(valid_rets_15)
                            tournament_stats[s_key]['returns_15d'].extend(valid_rets_15.tolist())
                            tournament_stats[s_key]['wins_15d'] += (valid_rets_15 > 0).sum()
                
                last = sig_df.iloc[-1]
                v_ratio = (last['成交量'] / last['Vol_MA5']) if last['Vol_MA5'] > 0 else 1.0
                
                today_signals[code] = {
                    'name': name, 'price': last['收盘'], 'pct': row['pct_chg'], 'amount': row['amount'], 
                    'v_ratio': v_ratio, 'cpv': last['CPV'],
                    'sig_A': last['Sig_A_Trend_Pullback'], 'sig_B': last['Sig_B_Bottom_Breakout'],
                    'sig_C': last['Sig_C_Strong_Dip'], 'sig_D': last['Sig_D_MA_Squeeze'],
                    'sig_E': last['Sig_E_Dragon_Relay'], 'sig_F': last['Sig_F_N_Shape'],
                    'sig_G': last['Sig_G_ATH_Breakout'], 'sig_H': last['Sig_H_Double_Bottom']
                }
                time.sleep(random.uniform(0.05, 0.1))
            except Exception: continue

        # =========================================================
        # 🏆 核心：实盘表现倒逼理论评分 (消灭假数据)
        # =========================================================
        ranked_strategies = []
        for s_name, stats in tournament_stats.items():
            trades = stats['trades']
            trades_15d = stats['trades_15d']
            
            stats['real_win_rate'] = -1.0
            
            # 🚀 提出来的全局兼容实盘匹配逻辑
            s_key_short = s_name.split(':')[0].strip() 
            s_core_name = s_name.split(':')[1].strip() if ':' in s_name else s_name 
            
            total_real_count = 0
            total_real_wins = 0.0
            
            for r_sname, r_perf in strategy_real_performance.items():
                if s_key_short in str(r_sname) or s_core_name in str(r_sname) or ("首阴" in str(r_sname) and "首阴" in s_core_name) or ("低吸" in str(r_sname) and "低吸" in s_core_name):
                    total_real_count += r_perf['count']
                    total_real_wins += (r_perf['win_rate'] / 100.0) * r_perf['count']

            if total_real_count >= 1: 
                r_win_rate = total_real_wins / total_real_count
                stats['real_win_rate'] = r_win_rate

            if trades >= 10: 
                win_rate = stats['wins'] / trades
                avg_ret = sum(stats['returns']) / trades
                win_rate_15d = stats['wins_15d'] / trades_15d if trades_15d > 0 else 0
                
                avg_win_ret = sum([r for r in stats['returns'] if r > 0]) / stats['wins'] if stats['wins'] > 0 else 0.02
                avg_loss_ret = abs(sum([r for r in stats['returns'] if r <= 0]) / (trades - stats['wins'])) if (trades - stats['wins']) > 0 else 0.05
                
                expectancy = (win_rate * avg_win_ret) - ((1 - win_rate) * avg_loss_ret)
                stats['ev'] = expectancy
                
                real_win_rate_multiplier = 1.0 
                
                if stats['real_win_rate'] >= 0:
                    real_win_rate_multiplier = max(0.1, (stats['real_win_rate'] + 0.5)) 
                    if stats['real_win_rate'] < 0.35:
                        stats['is_banned'] = True
                        logger.warning(f"🚫 实盘熔断: 【{s_name}】 实盘给用户造成巨大亏损，真实胜率仅 {stats['real_win_rate']*100:.1f}%，永久拉黑！")
                
                if trades_15d >= 3 and win_rate_15d < 0.35:
                    stats['is_banned'] = True
                    logger.warning(f"🚫 短线熔断: 【{s_name}】 近期15天胜率仅为 {win_rate_15d*100:.1f}%，剥夺资格！")
                
                odds = avg_win_ret / avg_loss_ret if avg_loss_ret > 0 else 1.0
                kelly_fraction = win_rate - ((1 - win_rate) / odds) if avg_loss_ret > 0 else 0.99
                
                if not is_market_safe:
                    kelly_fraction = kelly_fraction * 0.5 
                kelly_pct = max(0, min(1.0, kelly_fraction)) * 100
                
                base_score = expectancy * 0.4 + (win_rate_15d * avg_win_ret - (1-win_rate_15d)*avg_loss_ret) * 0.6 if trades_15d > 0 else expectancy * 0.5
                final_score = base_score * real_win_rate_multiplier
                
                stats['win_rate'] = win_rate
                stats['avg_ret'] = avg_ret
                stats['win_rate_15d'] = win_rate_15d
                stats['kelly_pct'] = kelly_pct
                stats['score'] = final_score

                if not stats['is_banned']:
                    ranked_strategies.append({
                        'name': s_name, 'score': final_score, 'trades': trades,
                        'win_rate': win_rate, 'avg_ret': avg_ret, 'ev': expectancy
                    })
            else:
                stats['score'] = 0.0
                stats['ev'] = 0.0
                stats['win_rate'] = 0.0
                stats['avg_ret'] = 0.0
                stats['win_rate_15d'] = 0.0
                stats['kelly_pct'] = 0.0
        
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

        if is_market_crash:
            actual_used_strategy = '战法H: 缩量双底'
            overall_best_strategy = '市场熔断避险'
            best_reason = f"大盘暴跌超 {limit_down_count} 家跌停！启动防空警报，废除顺延，仅允许双底形态防御。"
            for code, info in today_signals.items():
                if info[sig_map[actual_used_strategy]]:
                    final_pool.append({
                        "代码": code, "名称": info['name'], "现价": info['price'],
                        "匹配策略": f"🛡️ {actual_used_strategy}", "今日涨幅": f"{info['pct']:.2f}%", 
                        "量比": f"{info['v_ratio']:.2f}", "成交额": f"{info['amount']/100000000:.1f}亿", "重心CPV": f"{info['cpv']:.2f}", "sort_score": info['amount'] 
                    })
        else:
            for st in ranked_strategies:
                s_name = st['name']
                if st['ev'] <= 0: continue 
                
                target_sig_key = sig_map[s_name]
                temp_pool = []
                
                for code, info in today_signals.items():
                    if info[target_sig_key]:
                        temp_pool.append({
                            "代码": code, "名称": info['name'], "现价": info['price'],
                            "匹配策略": f"{s_name}", "今日涨幅": f"{info['pct']:.2f}%", 
                            "量比": f"{info['v_ratio']:.2f}", "成交额": f"{info['amount']/100000000:.1f}亿", "重心CPV": f"{info['cpv']:.2f}",
                            "sort_score": info['amount'] 
                        })
                
                if temp_pool:
                    actual_used_strategy = s_name
                    final_pool = temp_pool
                    if s_name == overall_best_strategy:
                        best_reason = f"避开实盘与短线双重熔断雷区！长短双盲霸主出击，EV期望值达 {st['ev']:+.2f}！"
                    else:
                        best_reason = f"智能顺延！霸主轮空，切换至经受住实盘拷打且EV为正的【{s_name}】！"
                    break

        if not actual_used_strategy and not final_pool:
            actual_used_strategy = '强制空仓'
            best_reason = "所有优势波段策略均无标的，或实盘验证全部拉跨遭到系统剥夺权限，强行切断买入信号，绝不送钱！"

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
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, actual_used_strategy, best_reason, review_summary, market_stats, top_sectors, is_market_safe, ai_feedback_data, global_stats)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"], ai_reflection=ai_result.get("ai_reflection", ""))
                self.save_ai_lesson(ai_result.get("new_lesson_learned", ""))
        
        self.send_email_report(ai_result, tournament_stats, overall_best_strategy, actual_used_strategy, self.target_count, review_records, recent_stats, market_stats, top_sectors, is_market_safe, market_trend_desc, global_stats)
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
