# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (内嵌 10日动态回测 终极版)
===================================

核心重构：
1. 【内嵌动态回测】：在每天选股的同时，利用已拉取的K线顺手回测过去10天的实战胜率，绝不增加API负担！
2. 【策略锁定】：强庄缩量回踩低吸（左侧买入，次日开盘砸盘卖出）。
3. 【数据防封】：双引擎（Tushare + 东财 + 新浪）切换，过滤超级大盘股。
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
from datetime import datetime
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
            except Exception as e:
                pass

    def _fetch_with_retry(self, func, retries=2, delay=1, *args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1: raise e
                time.sleep(delay + attempt)

    def get_market_spot(self):
        try:
            logger.info("尝试获取 [东方财富] 全量行情 (主引擎)...")
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
            return df, "EastMoney"
        except Exception as e:
            logger.warning("东方财富接口受限，🔄 无缝切换至【新浪财经】...")
            
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=2, delay=2)
            col_map = {'symbol': '代码', 'name': '名称', 'changepercent': '涨跌幅', 'amount': '成交额', 'trade': '最新价', 'open': '今开', 'settlement': '昨收'}
            for eng, chn in col_map.items():
                if chn not in df.columns and eng in df.columns:
                    df[chn] = df[eng]
                    
            df['code'] = df['代码'].str.replace(r'^[a-zA-Z]+', '', regex=True)
            df['name'] = df['名称']
            df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
            df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
            df['close'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
            df['open'] = pd.to_numeric(df.get('今开', df['close']), errors='coerce').fillna(0)
            df['prev_close'] = pd.to_numeric(df.get('昨收', df['close']), errors='coerce').fillna(0)
            df['market_cap'] = 0  
            df['circ_mv'] = 0  
            return df, "SinaFinance"
        except Exception:
            return pd.DataFrame(), "NONE"

    def _get_daily_kline(self, code, start_date="20230101"):
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
            query = urllib.parse.quote("中国 A股 政策 降息 央行 经济")
            url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=4) as res:
                root = ET.fromstring(res.read())
                lines = [f"- {it.find('title').text}" for it in root.findall('.//item')[:5]]
                if lines: news_text = "\n".join(lines)
        except Exception: pass
        return news_text

    def load_ai_lessons(self):
        if not os.path.exists(self.lessons_file): return "暂无历史避坑教训。"
        try:
            with open(self.lessons_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            lessons = [line.strip() for line in lines if line.strip()]
            return "\n".join(lessons[-10:]) if lessons else "暂无历史避坑教训。"
        except: return "读取历史教训失败。"

    def save_ai_lesson(self, lesson):
        if not lesson or len(lesson) < 5 or "无" in lesson.strip() or "未" in lesson.strip(): return
        try:
            with open(self.lessons_file, 'a', encoding='utf-8') as f:
                date_str = datetime.now().strftime('%Y-%m-%d')
                f.write(f"[{date_str} 铁律]: {lesson}\n")
        except: pass

    def calculate_technical_indicators(self, hist):
        df = hist.copy()
        numeric_cols = ['收盘', '开盘', '最高', '最低', '成交量']
        for c in numeric_cols: df[c] = pd.to_numeric(df[c], errors='coerce')
        
        if '涨跌幅' not in df.columns:
            df['涨跌幅'] = df['收盘'].pct_change() * 100
        
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA30'] = df['收盘'].rolling(30).mean()
        df['Vol_MA5'] = df['成交量'].rolling(5).mean()
        df['Max_Pct_10d'] = df['涨跌幅'].rolling(10).max()
        return df

    def _run_embedded_backtest(self, tech_df, lookback_days=10):
        """
        🚀 核心外挂：内嵌动态回测！
        利用已经下载好的K线数据，强行推演过去 10 天如果按此策略买入，次日开盘卖出的真实收益。
        """
        if len(tech_df) < 30: return []
        
        df = tech_df.copy()
        df['Next_Open'] = df['开盘'].shift(-1) # 获取次日开盘价用于卖出
        
        # 截取过去10天（不包含今天，因为今天还没收盘没法验证次日）
        test_df = df.iloc[-(lookback_days+1):-1] 
        
        returns = []
        for _, row in test_df.iterrows():
            # 严格套用现在的策略条件
            gene = row['Max_Pct_10d'] > 7.0
            pct = -5.0 <= row['涨跌幅'] <= 1.5
            vol = row['成交量'] < (row['Vol_MA5'] * 0.8)
            ma10_support = abs(row['收盘'] - row['MA10']) / row['MA10'] <= 0.03
            ma20_support = abs(row['收盘'] - row['MA20']) / row['MA20'] <= 0.03
            support = ma10_support or ma20_support
            trend = row['收盘'] > row['MA30']
            
            # 如果某天满足了买入条件
            if gene and pct and vol and support and trend:
                # 算隔夜收益: (次日开盘价 - 今日收盘价) / 今日收盘价 - 千分之1.5手续费
                ret = (row['Next_Open'] - row['收盘']) / row['收盘'] - 0.0015
                returns.append(ret * 100)
                
        return returns

    def ai_select_top5(self, candidates, macro_news, backtest_summary):
        logger.info("🧠 正在唤醒 AI 分析缩量回踩池...")
        
        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | 现价:{c['现价']} | 缩量比:{c['缩量比']} | 支撑点:{c['均线支撑']} | 异动基因:{c['强庄基因']}\n"

        prompt = f"""你是一位擅长【龙头首阴、缩量回踩生命线战法】的顶级A股游资。
这套战法的奥义是：【今日尾盘低吸潜伏，次日早盘集合竞价趁高开直接砸盘获利！】绝不参与盘中的下跌！

### 🧠 你的低吸避坑记忆：
{past_lessons}

### 📉 【核心预警】当前策略近10日量化回测表现：
{backtest_summary}
(请务必参考此胜率：若近期胜率极低，说明当前大盘不支持低吸，务必在报告中警示风险！)

### 🌍 今日大势：
{macro_news}

### 📊 今日完美备选池：
{cand_text}

### 🎯 你的任务：
1. 必须输出不多不少恰好 5 只股票！挑选主力洗盘最明显、次日早盘反抽概率最大的。
2. `reason` 中说明买入逻辑。
3. `target_price` 设定为次日早盘冲高点，`stop_loss` 设定为跌破支撑均线止损。
4. 若回测数据显示大亏，提取一条教训（50字以内）填入 `new_lesson_learned`！

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "对近期策略回测数据和大势的反思...",
    "new_lesson_learned": "提取的新避坑铁律(无亏损填：无)",
    "macro_view": "判断市场情绪是否支持次日早盘反抽...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "缩量回踩低吸",
            "current_price": 当前价格,
            "reason": "入选逻辑（围绕极致缩量和强支撑展开，50字左右）",
            "target_price": "明日早盘竞价兑现位（具体数字）",
            "stop_loss": "跌破均线的极简止损位（具体数字）"
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
        except Exception:
            return None

    def send_email_report(self, ai_data, backtest_summary, target_count):
        logger.info("📧 正在生成并发送回踩战报...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        if not sender or not pwd: return

        today_str = datetime.now().strftime('%Y-%m-%d')

        top5_html = ""
        if ai_data and "top_5" in ai_data and len(ai_data["top_5"]) > 0:
            top5_html += f"""
            <h3>🧠 主力洗盘监控日记</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 避坑铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 情绪支持：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 极致缩量回踩标的 (共 {target_count} 只，尾盘潜伏，次日竞价兑现)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #2c3e50; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>洗盘形态</th><th>现价</th><th>早盘竞价操作计划</th><th>核心逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#e8f4f8; color:#2980b9; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🚀 冲高卖: {s.get('target_price', '')}<br>🛑 破位损: {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = "<p>🧊 今日无符合极致缩量回踩的个股，主力未露出破绽，空仓休息。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #c0392b; border-bottom: 2px solid #c0392b; padding-bottom: 10px;">📉 A股洗盘雷达：缩量回踩低吸战报 ({today_str})</h2>
            
            <div style="background-color: #f1f2f6; padding: 15px; border-left: 5px solid #2980b9; margin-bottom: 20px;">
                <h3 style="margin-top: 0; color: #2980b9;">📊 策略内嵌 10 日动态回测 (次日开盘无脑砸盘法)</h3>
                <p style="font-size: 14px; margin: 0;">{backtest_summary.replace('\n', '<br>')}</p>
            </div>
            
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：强庄股大跌缩量时低吸，次日早盘集合竞价借高开直接砸盘走人！不参与盘中走势！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【主力洗盘雷达】强庄缩量低吸名单 - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "量化低吸系统"
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
            logger.info("✅ 低吸战报邮件发送成功！")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        logger.info("========== 启动【强庄缩量回踩低吸战法 + 内嵌回测】量化雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        logger.info("👉 执行左侧低吸过滤：全市场极速初筛...")
        
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
        df = df[(df['pct_chg'] >= -5.0) & (df['pct_chg'] <= 1.5)]
        
        if df['circ_mv'].sum() > 0:
            df = df[(df['circ_mv'] >= 30_0000_0000) & (df['circ_mv'] <= 300_0000_0000)]
        df = df[df['amount'] >= 100000000]
        
        candidates = df.sort_values(by='amount', ascending=False).head(100) # 取前100只活跃股测算
        logger.info(f"👉 初筛出 {len(candidates)} 只回调企稳标的！启动K线强算与动态回测...")

        scored_pool = []
        total_c = len(candidates)
        
        # 🚀 收集10天回测数据的池子
        all_backtest_returns = []
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 20 == 0: logger.info(f"⏳ 洗盘扫描与回测中... 进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                df_kline = self._get_daily_kline(code)
                if df_kline is None or len(df_kline) < 30: continue
                
                tech_df = self.calculate_technical_indicators(df_kline)
                
                # ==========================================
                # 🚀 执行内嵌回测 (不产生额外API请求)
                # ==========================================
                bt_returns = self._run_embedded_backtest(tech_df, lookback_days=10)
                all_backtest_returns.extend(bt_returns)
                
                # ==========================================
                # 🔪 洗盘战法核心三大定律判断 (计算今日的买点)
                # ==========================================
                last = tech_df.iloc[-1]
                vol_today = float(last['成交量'])
                vol_ma5 = float(last['Vol_MA5'])
                close_p = float(last['收盘'])
                ma10 = float(last['MA10'])
                ma20 = float(last['MA20'])
                ma30 = float(last['MA30'])
                
                if vol_today == 0 or vol_ma5 == 0: continue
                
                max_pct_10d = tech_df['涨跌幅'].tail(10).max()
                has_gene = max_pct_10d > 7.0
                vol_ratio = vol_today / vol_ma5
                is_shrinking = vol_ratio < 0.80 
                near_ma10 = abs(close_p - ma10) / ma10 <= 0.03
                near_ma20 = abs(close_p - ma20) / ma20 <= 0.03
                has_support = near_ma10 or near_ma20
                trend_ok = close_p > ma30
                
                if not (has_gene and is_shrinking and has_support and trend_ok):
                    continue
                
                score = 50
                strategy_tag = "缩量回踩"
                support_desc = "精准回踩均线"
                gene_desc = f"近10日最大涨幅{max_pct_10d:.1f}%"
                
                if vol_ratio <= 0.60:
                    score += 30 
                    strategy_tag = "💡 极品地量回踩"
                else: score += 15
                    
                if near_ma20: score += 20
                elif near_ma10: score += 10
                    
                if max_pct_10d > 9.5: score += 15 
                if close_p >= float(last['开盘']): score += 10
                        
                scored_pool.append({
                    "代码": code, "名称": name, "现价": close_p,
                    "匹配策略": strategy_tag, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                    "缩量比": f"{vol_ratio*100:.1f}%", "均线支撑": support_desc,
                    "强庄基因": gene_desc, "综合得分": score 
                })
                time.sleep(random.uniform(0.1, 0.2))
                
            except Exception:
                continue
                
        # =========================================================
        # 📊 整理 10 日动态回测报告
        # =========================================================
        backtest_summary = "暂无充足的回测样本。"
        if len(all_backtest_returns) > 0:
            wins = [r for r in all_backtest_returns if r > 0]
            win_rate = len(wins) / len(all_backtest_returns) * 100
            avg_ret = sum(all_backtest_returns) / len(all_backtest_returns)
            eval_color = "🟢 极度恶劣！请管住手空仓！" if win_rate < 40 else "🔴 行情绝佳！闭眼提款！" if win_rate > 60 else "🟡 震荡分化，请严格控制仓位。"
            
            backtest_summary = f"过去 10 个交易日内，大盘活跃标的中满足【强庄缩量低吸】形态共 {len(all_backtest_returns)} 次。\n"
            backtest_summary += f"若按『尾盘买入，次日开盘价无脑砸盘』的铁律操作，**胜率为 {win_rate:.1f}%，平均单笔隔夜收益为 {avg_ret:+.2f}%**。\n"
            backtest_summary += f"结论：当前市场情绪 {eval_color}"
            logger.info(f"📊 {backtest_summary}")

        # =========================================================
        # 👑 胜者为王：截取前 5 名
        # =========================================================
        final_top_stocks = []
        if scored_pool:
            scored_pool = sorted(scored_pool, key=lambda x: x['综合得分'], reverse=True)
            final_top_stocks = scored_pool[:5]
        
        self.target_count = len(final_top_stocks)
        
        ai_result = None
        if final_top_stocks:
            macro_news = self.fetch_macro_news()
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, backtest_summary)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                lesson = ai_result.get("new_lesson_learned", "")
                self.save_ai_lesson(lesson)
        
        self.send_email_report(ai_result, backtest_summary, self.target_count)
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
