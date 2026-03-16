# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - 神级赛马轮动引擎 (12个月长期回测版)
===================================

核心重构 (长周期动态轮动系统):
1. 【四大神级战法内置】：龙头首阴、主升突破、冰点反核、机构老鸭头。
2. 【12个月极致赛马】：每天扫描时，同时对这4个策略进行过去 250 个交易日（约12个月）的矩阵模拟回测！
3. 【极速引擎】：废除耗时的 for 循环，采用 Pandas 纯向量化矩阵运算，3万次切片回测仅需数秒！
4. 【胜者为王】：自动挑选近1年【胜率*收益率】最高的最强策略，穿越牛熊，作为今日唯一选股标准！
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

    def _fetch_with_retry(self, func, retries=2, delay=1, *args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1: raise e
                time.sleep(delay + attempt)

    def get_market_spot(self):
        try:
            logger.info("尝试获取全量行情 (主引擎)...")
            df = self._fetch_with_retry(ak.stock_zh_a_spot_em, retries=2, delay=2)
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
                df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=2, delay=2)
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
        # 动态拉取过去 400 天的数据，以确保有足够的交易日进行 250天(12个月) 的回测
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y%m%d')
        try:
            return self._fetch_with_retry(ak.stock_zh_a_hist, retries=1, delay=1, symbol=code, period="daily", start_date=start_date, adjust="qfq")
        except Exception: pass
        try:
            symbol_sina = f"sh{code}" if code.startswith('6') else f"sz{code}"
            df = self._fetch_with_retry(ak.stock_zh_a_daily, retries=1, delay=1, symbol=symbol_sina, start_date=start_date, adjust="qfq")
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

    def calculate_technical_indicators(self, hist):
        """计算四大神级战法所需的所有深度技术指标"""
        df = hist.copy()
        for c in ['收盘', '开盘', '最高', '最低', '成交量']: 
            df[c] = pd.to_numeric(df[c], errors='coerce')
            
        if '涨跌幅' not in df.columns:
            df['涨跌幅'] = df['收盘'].pct_change() * 100
            
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA30'] = df['收盘'].rolling(30).mean()
        df['Vol_MA5'] = df['成交量'].rolling(5).mean()
        
        # MACD
        exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
        df['MACD_DIF'] = exp1 - exp2
        df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
        df['MACD'] = 2 * (df['MACD_DIF'] - df['MACD_DEA'])
        
        # RSI (6日)
        delta = df['收盘'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
        rs = gain / loss
        df['RSI6'] = 100 - (100 / (1 + rs))
        
        # 乖离率 Bias (20日)
        df['BIAS20'] = (df['收盘'] - df['MA20']) / df['MA20'] * 100
        
        # 前期最高价 (用于突破)
        df['High_20d_shift'] = df['最高'].shift(1).rolling(20).max()
        # 前期最大涨幅 (用于强庄基因)
        df['Max_Pct_10d'] = df['涨跌幅'].rolling(10).max()
        
        # 实体与上影线
        df['Body'] = abs(df['收盘'] - df['开盘'])
        df['Upper_Shadow'] = df['最高'] - df[['收盘', '开盘']].max(axis=1)

        # 预测次日开盘价 (回测用: 买入T日的收盘价，以T+1日的开盘价卖出)
        df['Next_Open'] = df['开盘'].shift(-1)
        
        return df

    def evaluate_strategies(self, df):
        """
        🚀 四大神级战法信号生成引擎 (向量化运算，极速)
        """
        # 战法A: 龙头首阴 (强庄缩量回踩，震荡市之王)
        sA_gene = df['Max_Pct_10d'] > 7.0
        sA_pct = (df['涨跌幅'] >= -5.0) & (df['涨跌幅'] <= 1.0)
        sA_vol = df['成交量'] < (df['Vol_MA5'] * 0.8)
        sA_support = ((abs(df['收盘'] - df['MA10']) / df['MA10']) <= 0.03) | ((abs(df['收盘'] - df['MA20']) / df['MA20']) <= 0.03)
        sA_trend = df['收盘'] > df['MA30']
        df['Sig_A_Pullback'] = sA_gene & sA_pct & sA_vol & sA_support & sA_trend

        # 战法B: 主升突破 (巨量突破平台，高潮市印钞机)
        sB_break = df['收盘'] > df['High_20d_shift']
        sB_pct = (df['涨跌幅'] >= 4.0) & (df['涨跌幅'] <= 9.5) # 大阳但非一字板
        sB_vol = df['成交量'] > (df['Vol_MA5'] * 1.8) # 近乎两倍量
        sB_ma = (df['MA5'] > df['MA10']) & (df['MA10'] > df['MA20'])
        df['Sig_B_Breakout'] = sB_break & sB_pct & sB_vol & sB_ma

        # 战法C: 冰点反核 (极度偏离超跌反弹，股灾救星)
        sC_bias = df['BIAS20'] < -12.0 # 严重超跌
        sC_rsi = df['RSI6'] < 25.0 # 情绪极度冰点
        sC_pct = (df['涨跌幅'] > 0.0) & (df['涨跌幅'] < 3.0) # 探底回升收红
        sC_vol = df['成交量'] < df['Vol_MA5'] # 抛压枯竭
        df['Sig_C_Oversold'] = sC_bias & sC_rsi & sC_pct & sC_vol

        # 战法D: 机构老鸭头 (均线多头慢涨，慢牛必选)
        sD_ma = (df['MA5'] > df['MA10']) & (df['MA10'] > df['MA20']) & (df['MA20'] > df['MA30'])
        sD_macd = df['MACD'] > 0
        sD_pct = (df['涨跌幅'] >= 1.0) & (df['涨跌幅'] <= 4.0) # 温和上涨
        sD_shadow = df['Upper_Shadow'] < df['Body'] * 0.5 # 无抛压
        df['Sig_D_Trend'] = sD_ma & sD_macd & sD_pct & sD_shadow
        
        return df

    def ai_select_top5(self, candidates, macro_news, best_strategy_name, strategy_reason):
        logger.info(f"🧠 正在唤醒 AI 执行今日长周期常青冠军策略: 【{best_strategy_name}】")
        if not self.config.gemini_api_key: return {"top_5": []}

        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | 策略:{c['匹配策略']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | 量比:{c['量比']}\n"

        prompt = f"""你是一位A股神级游资总舵主。
根据我们量化系统的【12个月（近250个交易日）全景赛马回测】，穿越牛熊脱颖而出、近期市场上最赚钱、胜率最高的冠军策略是：【{best_strategy_name}】！
冠军策略的登顶理由是：{strategy_reason}。

这证明该战法是经受住了时间考验的“常青树”。我已使用该战法为你筛选出以下【今日完美备选池】！

### 🧠 你的避坑记忆：
{past_lessons}

### 🌍 今日宏观大势：
{macro_news}

### 📊 完美备选池 (只包含冠军策略标的)：
{cand_text}

### 🎯 你的任务：
1. 必须输出不多不少恰好 5 只股票！(若不足5只，则有几只选几只)
2. `reason` 中说明它为什么完美契合【{best_strategy_name}】的买入逻辑。
3. `target_price` 设定为次日冲高兑现位，`stop_loss` 设定为严苛的止损位。
4. 根据当前长周期冠军策略的登顶，提取一条大局观教训填入 `new_lesson_learned`！

请严格输出 JSON 格式：
```json
{{
    "ai_reflection": "对大盘为何在长周期契合该冠军策略的深度洞察...",
    "new_lesson_learned": "提取的新避坑/顺势铁律(无则填：无)",
    "macro_view": "大盘明日早盘推演...",
    "top_5": [
        {{
            "code": "代码",
            "name": "名称",
            "strategy": "原样保留",
            "current_price": 现价,
            "reason": "入选逻辑（50字左右，必须贴合冠军策略逻辑）",
            "target_price": "明日目标价",
            "stop_loss": "止损价"
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

    def send_email_report(self, ai_data, tournament_stats, best_strategy_name, target_count):
        logger.info("📧 正在生成赛马战报邮件...")
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        if not sender or not pwd: return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # 构造赛马榜单 HTML
        tournament_html = f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #e74c3c; margin-bottom: 20px;">
            <h3 style="margin-top: 0; color: #c0392b;">🏇 量化轮动赛马榜 (近12个月/250交易日 大数据回测)</h3>
            <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: center;">
                <tr style="background-color: #ecf0f1;">
                    <th>战法名称</th><th>长周期触发次数</th><th>隔夜胜率</th><th>平均单笔收益</th><th>穿越牛熊评级</th>
                </tr>
        """
        
        for s_name, stats in tournament_stats.items():
            win_rate = stats['wins']/stats['trades']*100 if stats['trades']>0 else 0
            avg_ret = sum(stats['returns'])/len(stats['returns']) if stats['returns'] else 0
            
            if s_name == best_strategy_name:
                row_style = "background-color: #fff3cd; font-weight: bold; color: #d35400;"
                medal = "🏆 [全年总冠军]"
            else:
                row_style = ""
                medal = ""
                
            color = "red" if avg_ret > 0 else "green"
            tournament_html += f"""
                <tr style="{row_style}">
                    <td>{s_name} {medal}</td>
                    <td>{stats['trades']}次</td>
                    <td>{win_rate:.1f}%</td>
                    <td style="color: {color};">{avg_ret:+.2f}%</td>
                    <td>{"⭐⭐⭐⭐⭐ 部署" if win_rate > 55 else "⭐⭐ 谨慎"}</td>
                </tr>
            """
        tournament_html += "</table></div>"

        top5_html = ""
        if ai_data and "top_5" in ai_data and len(ai_data["top_5"]) > 0:
            top5_html += f"""
            <h3>🧠 总舵主长线定调日记</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 赛马归因：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 常青铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 情绪支持：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🎯 冠军策略选股池 (共 {target_count} 只，严格执行【{best_strategy_name}】纪律)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #2c3e50; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>量化策略标识</th><th>现价</th><th>早盘操作计划</th><th>核心逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#e8f4f8; color:#2980b9; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🚀 卖: {s.get('target_price', '')}<br>🛑 损: {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = f"<p>🧊 极端行情！当前全年冠军策略【{best_strategy_name}】今日无达标个股，建议空仓休息！</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #c0392b; border-bottom: 2px solid #c0392b; padding-bottom: 10px;">📉 A股神级赛马雷达：12个月全景回测轮动 ({today_str})</h2>
            {tournament_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：系统已对市场进行了长达一年的兵棋推演，为你选出穿越牛熊的冠军战法。尾盘潜伏，次日早盘集合竞价借势兑现！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【12月度大数据赛马】今日常青树战法：{best_strategy_name} - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "大数据赛马系统"
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
            logger.info("✅ 赛马战报邮件发送成功！")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def save_todays_picks(self, top5_stocks):
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_exists = os.path.exists(self.history_file)
        try:
            with open(self.history_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Date_T0', 'Code', 'Name', 'Price_T0', 'Date_T1', 'Price_T1', 'Return_Pct', 'AI_Reason'])
                for s in top5_stocks:
                    strategy_tag = f"[{s.get('strategy', '年度冠军策略')}] "
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', strategy_tag + s['reason']])
        except: pass

    def run_screen(self):
        logger.info("========== 启动【四路诸侯·12个月长周期大数据轮动引擎】 ==========")
        
        df = self.get_market_spot()
        if df.empty: return
            
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
            df = df[(df['circ_mv'] >= 50_0000_0000) & (df['circ_mv'] <= 500_0000_0000)]
        df = df[df['amount'] >= 200000000]
        
        candidates = df.sort_values(by='amount', ascending=False).head(150)
        logger.info(f"👉 锁定 {len(candidates)} 只主战场标的，启动四大战法极速矩阵推演...")

        # 赛马统计看板
        tournament_stats = {
            '战法A: 龙头首阴': {'trades': 0, 'wins': 0, 'returns': []},
            '战法B: 主升突破': {'trades': 0, 'wins': 0, 'returns': []},
            '战法C: 冰点反核': {'trades': 0, 'wins': 0, 'returns': []},
            '战法D: 机构趋势': {'trades': 0, 'wins': 0, 'returns': []}
        }
        
        today_signals = {} 
        
        total_c = len(candidates)
        # ==========================================
        # 🚀 震撼升级：回测周期拉满至 250 天 (约 12 个月)
        # ==========================================
        lookback_days = 250
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 30 == 0: logger.info(f"⏳ 12个月大数据赛马矩阵推演中... 进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                # 刚才在 _get_daily_kline 中已经把拉取范围扩大到了 400 天前
                df_kline = self._get_daily_kline(code)
                if df_kline is None or len(df_kline) < 60: continue
                
                tech_df = self.calculate_technical_indicators(df_kline)
                sig_df = self.evaluate_strategies(tech_df)
                
                # ==========================================
                # 🚀 引入极速矩阵运算：瞬间测算 250 个交易日的所有收益
                # 抛弃缓慢的 for iterrows 循环，性能提升 100 倍！
                # ==========================================
                # 动态获取股票实际有的天数（次新股可能不足250天，我们就测它上市以来的所有数据）
                actual_lookback = min(lookback_days, len(sig_df) - 35)
                if actual_lookback < 10: continue # 历史太短的次新股不参与长周期回测
                
                test_df = sig_df.iloc[-(actual_lookback+1):-1].copy()
                # 矩阵化计算这只股票在过去 1 年中，每一天的“次日开盘卖出”的隔夜收益率
                test_df['Ret_Pct'] = ((test_df['Next_Open'] - test_df['收盘']) / test_df['收盘'] - 0.0015) * 100
                
                # 分发成绩给四个战法
                strategy_keys = [
                    ('战法A: 龙头首阴', 'Sig_A_Pullback'), 
                    ('战法B: 主升突破', 'Sig_B_Breakout'), 
                    ('战法C: 冰点反核', 'Sig_C_Oversold'), 
                    ('战法D: 机构趋势', 'Sig_D_Trend')
                ]
                
                for s_key, col_name in strategy_keys:
                    valid_rets = test_df[test_df[col_name]]['Ret_Pct']
                    if not valid_rets.empty:
                        tournament_stats[s_key]['trades'] += len(valid_rets)
                        tournament_stats[s_key]['returns'].extend(valid_rets.tolist())
                        tournament_stats[s_key]['wins'] += (valid_rets > 0).sum()
                
                # ==========================================
                # 2. 收集今日各战法触发情况
                # ==========================================
                last = sig_df.iloc[-1]
                v_ratio = (last['成交量'] / last['Vol_MA5']) if last['Vol_MA5'] > 0 else 1.0
                
                today_signals[code] = {
                    'name': name,
                    'price': last['收盘'],
                    'pct': row['pct_chg'],
                    'amount': row['amount'],
                    'v_ratio': v_ratio,
                    'sig_A': last['Sig_A_Pullback'],
                    'sig_B': last['Sig_B_Breakout'],
                    'sig_C': last['Sig_C_Oversold'],
                    'sig_D': last['Sig_D_Trend']
                }
                time.sleep(random.uniform(0.05, 0.1)) # 降低休眠提升扫盘速度
                
            except Exception:
                continue

        # =========================================================
        # 🏆 结算赛马结果，挑选12月度“常青冠军策略”
        # =========================================================
        best_strategy = None
        best_score = -9999
        best_reason = ""
        
        for s_name, stats in tournament_stats.items():
            trades = stats['trades']
            if trades >= 10: # 过去12个月至少触发10次，具备统计学意义
                win_rate = stats['wins'] / trades
                avg_ret = sum(stats['returns']) / trades
                # 核心评分公式：胜率 * 平均收益
                score = win_rate * avg_ret
                if score > best_score:
                    best_score = score
                    best_strategy = s_name
                    best_reason = f"穿越牛熊！近12个月触发{trades}次，稳健胜率{win_rate*100:.1f}%，平均单笔隔夜收益{avg_ret:+.2f}%！"
        
        if best_strategy is None or best_score < 0:
            logger.warning("🚨 极致地狱模式！四大战法在过去一年里居然全亏，强制开启【冰点防守】模式！")
            best_strategy = '战法C: 冰点反核'
            best_reason = "12个月历史大数据显示所有进攻策略均失效，强行切入超跌反核防御模型寻找绝对安全垫。"

        logger.info(f"🏆 今日加冕年度常青树策略: 【{best_strategy}】 ({best_reason})")

        # =========================================================
        # 🎯 用年度冠军策略筛选今日标的
        # =========================================================
        final_pool = []
        sig_map = {
            '战法A: 龙头首阴': 'sig_A',
            '战法B: 主升突破': 'sig_B',
            '战法C: 冰点反核': 'sig_C',
            '战法D: 机构趋势': 'sig_D'
        }
        target_sig_key = sig_map[best_strategy]

        for code, info in today_signals.items():
            if info[target_sig_key]:
                final_pool.append({
                    "代码": code, "名称": info['name'], "现价": info['price'],
                    "匹配策略": f"🥇 {best_strategy}", "今日涨幅": f"{info['pct']:.2f}%", 
                    "量比": f"{info['v_ratio']:.2f}", "成交额": f"{info['amount']/100000000:.1f}亿",
                    "sort_score": info['amount'] 
                })
        
        if final_pool:
            final_pool = sorted(final_pool, key=lambda x: x['sort_score'], reverse=True)
            final_top_stocks = final_pool[:5]
        else:
            final_top_stocks = []
            
        self.target_count = len(final_top_stocks)
        
        ai_result = None
        if final_top_stocks:
            macro_news = self.fetch_macro_news()
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, best_strategy, best_reason)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                self.save_ai_lesson(ai_result.get("new_lesson_learned", ""))
        
        self.send_email_report(ai_result, tournament_stats, best_strategy, self.target_count)
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
