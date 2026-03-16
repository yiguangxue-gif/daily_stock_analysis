# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (强庄缩量回踩低吸 终极版)
===================================

核心重构 (颠覆性策略：从右侧追涨改为左侧低吸)：
1. 目标池：30亿-300亿活跃中盘股。
2. 强庄基因：近10日内必须有过单日涨幅 > 7% 的异动，证明有主力活跃。
3. 恐慌买入：今日涨幅在 -5% 到 +1.5% 之间，坚决不追高，专买阴线或十字星。
4. 极致缩量：今日成交量必须小于5日均量的 80% (极致洗盘，主力未出货)。
5. 均线精准狙击：股价必须回踩到 MA10 或 MA20 附近 (+/- 3%范围内)。
"""

import os
import warnings
# 屏蔽所有过期的警告噪音
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
                logger.warning(f"⚠️ Tushare 初始化失败: {e}")

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
            logger.warning("东方财富全市场接口受限，🔄 无缝切换至【新浪财经】...")
            
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=2, delay=2)
            logger.info("✅ 新浪财经基础数据拉取完成。")
            
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
        except Exception as e:
            logger.error(f"❌ 双引擎全军覆没: {e}")
            return pd.DataFrame(), "NONE"

    def _get_daily_kline(self, code, start_date="20230101"):
        """双引擎日线拉取"""
        try:
            return self._fetch_with_retry(ak.stock_zh_a_hist, retries=1, delay=1, symbol=code, period="daily", start_date=start_date, adjust="qfq")
        except Exception:
            pass

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
        except:
            pass
            
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
        except Exception:
            pass
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
                f.write(f"[{date_str} 低吸铁律]: {lesson}\n")
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
                logger.info(f"🔍 正在核算 {len(unreviewed)} 只历史标的的真实盈亏...")
                for idx, row in unreviewed.iterrows():
                    code = str(row['Code']).zfill(6)
                    match = market_df[market_df['code'] == code]
                    if not match.empty:
                        t1_price = float(match.iloc[0]['close'])
                        t0_price = float(row['Price_T0'])
                        if t0_price > 0:
                            # 左侧低吸策略，计算反弹利润
                            ret_pct = ((t1_price - t0_price) / t0_price) * 100
                            df_hist.at[idx, 'Date_T1'] = today_str
                            df_hist.at[idx, 'Price_T1'] = t1_price
                            df_hist.at[idx, 'Return_Pct'] = round(ret_pct, 2)
                            
                            review_records.append({
                                "代码": code, "名称": row['Name'], "昨买价": t0_price, 
                                "今收价": t1_price, "真实涨跌幅": f"{ret_pct:+.2f}%", "AI逻辑": str(row.get('AI_Reason', ''))[:30]
                            })
                df_hist.to_csv(self.history_file, index=False)
                
            reviewed_df = df_hist.dropna(subset=['Return_Pct']).copy()
            if not reviewed_df.empty:
                recent_dates = sorted(reviewed_df['Date_T0'].unique())[-lookback_days:]
                recent_records = reviewed_df[reviewed_df['Date_T0'].isin(recent_dates)]
                
                total_return = recent_records['Return_Pct'].sum()
                win_count = (recent_records['Return_Pct'] > 0).sum()
                total_count = len(recent_records)
                
                avg_ret = total_return / total_count if total_count > 0 else 0
                win_rate = (win_count / total_count) * 100 if total_count > 0 else 0
                
                recent_stats = {
                    "avg_ret": avg_ret, "win_rate": win_rate, 
                    "days": len(recent_dates), "total_count": total_count
                }
                
                review_summary = f"【AI低吸模型 - 近 {len(recent_dates)} 日实盘复盘】\n"
                review_summary += f"潜伏共 {total_count} 只标的，平均单笔收益: {avg_ret:+.2f}%，胜率: {win_rate:.1f}%。\n"
                
                for date in recent_dates:
                    day_df = recent_records[recent_records['Date_T0'] == date]
                    review_summary += f"\n👉 [{date} 批次]:\n"
                    for _, r in day_df.iterrows():
                        review_summary += f"  - {r['Name']}({str(r['Code']).zfill(6)}) | 涨跌: {r['Return_Pct']:+.2f}%\n"
                        
        except Exception as e:
            logger.error(f"复盘核算异常: {e}")
            
        return review_summary, review_records, recent_stats

    def save_todays_picks(self, top5_stocks):
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_exists = os.path.exists(self.history_file)
        try:
            with open(self.history_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Date_T0', 'Code', 'Name', 'Price_T0', 'Date_T1', 'Price_T1', 'Return_Pct', 'AI_Reason'])
                for s in top5_stocks:
                    strategy_tag = f"[{s.get('strategy', '左侧低吸')}] "
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', strategy_tag + s['reason']])
        except Exception as e:
            logger.error(f"保存今日金股失败: {e}")

    def calculate_technical_indicators(self, hist):
        df = hist.copy()
        numeric_cols = ['收盘', '开盘', '最高', '最低', '成交量']
        for c in numeric_cols: df[c] = pd.to_numeric(df[c], errors='coerce')
        
        # 计算真实涨跌幅 (防新浪接口缺失)
        if '涨跌幅' not in df.columns:
            df['涨跌幅'] = df['收盘'].pct_change() * 100
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA30'] = df['收盘'].rolling(30).mean()
        
        df['Vol_MA5'] = df['成交量'].rolling(5).mean()
        
        return df

    def _generate_fallback_ai_data(self, candidates):
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c.get('匹配策略', '🛡️ 量化底仓直出'),
                "current_price": c['现价'],
                "reason": f"满足【强庄缩量回踩】法则。量比: {c.get('缩量比', 'N/A')}，支撑位: {c.get('均线支撑', 'N/A')}。",
                "target_price": "次日或后天脉冲反包出局",
                "stop_loss": "破位核心均线止损"
            })
        return {
            "ai_reflection": "【预警】AI受限。以下标的为量化引擎100%严格按【缩量回踩核心均线】低吸战法强算输出！",
            "new_lesson_learned": "无",
            "macro_view": "按游资首阴回踩模式，潜伏左侧，卖在右侧。",
            "top_5": top_5
        }

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 分析缩量回踩池...")
        if not self.config.gemini_api_key:
            return self._generate_fallback_ai_data(candidates)

        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 现价:{c['现价']} | 缩量比:{c['缩量比']} | 支撑点:{c['均线支撑']} | 异动基因:{c['强庄基因']}\n"

        prompt = f"""你是一位擅长【龙头首阴、缩量回踩生命线战法】的顶级A股游资。
我为你筛选出了以下备选股票池，它们全部符合“近期异动爆拉过、今日缩量回调、正好踩在MA10或MA20支撑位”的黄金低吸形态！

### 🧠 你的低吸避坑记忆：
{past_lessons}

### 📉 近期实盘打脸复盘：
{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 缩量回踩完美备选池 (萎缩越厉害、支撑越准越优)：
{cand_text}

### 🎯 你的任务：
1. 必须输出不多不少恰好 5 只股票！从中挑选主力洗盘最明显、反包概率最大的标的。
2. `reason` 中说明买入逻辑（如：缩量极限洗盘，精准回踩MA10，近期有涨停基因预期反包）。
3. `target_price` 设定为前高或大阳线高点，`stop_loss` 设定为跌破支撑均线3%无条件止损。
4. 若昨日或近期复盘连亏，提取一条血的教训（50字以内）填入 `new_lesson_learned`！

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "对当前低吸环境和昨日打脸的复盘反思...",
    "new_lesson_learned": "提取的新避坑铁律(无亏损填：无)",
    "macro_view": "判断市场情绪是否支持次日反包修复...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留匹配策略",
            "current_price": 当前价格,
            "reason": "入选逻辑（围绕极致缩量和强支撑反包展开，50字左右）",
            "target_price": "明日反弹预期目标位（具体数字）",
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
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = model.generate_content(
                        prompt, 
                        generation_config={"temperature": 0.4, "max_output_tokens": 4096},
                        request_options={"timeout": 120} 
                    )
                    text = response.text
                    m = re.search(r'(\{.*\})', text, re.DOTALL)
                    json_str = m.group(1) if m else text
                    
                    result_data = json.loads(repair_json(json_str))
                    if not result_data.get("top_5") or len(result_data["top_5"]) == 0:
                        return self._generate_fallback_ai_data(candidates)
                        
                    return result_data
                except Exception as inner_e:
                    if attempt < max_retries - 1: time.sleep(3)
                    else: return self._generate_fallback_ai_data(candidates)
        except Exception:
            return self._generate_fallback_ai_data(candidates)

    def send_email_report(self, ai_data, review_records, recent_stats, target_count):
        logger.info("📧 正在生成并发送回踩战报...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd: return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        review_html = ""
        if review_records:
            avg_ret = recent_stats.get('avg_ret', 0.0)
            win_rate = recent_stats.get('win_rate', 0.0)
            days = recent_stats.get('days', 1)
            color_ret = "red" if avg_ret > 0 else "green" if avg_ret < 0 else "black"
            
            review_html += f"""
            <h3>⚖️ 近 {days} 日低吸战法实盘处刑台</h3>
            <p>全局表现：平均单笔收益 <b style="color:{color_ret}">{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>名称(代码)</th><th>低吸成本</th><th>当前估值</th><th>真实盈亏</th><th>潜伏逻辑</th>
                </tr>
            """
            for r in review_records:
                color = "red" if float(str(r['真实涨跌幅']).replace('%', '')) > 0 else "green"
                review_html += f"""
                <tr>
                    <td>{r['名称']} ({r['代码']})</td>
                    <td>{r['昨买价']}</td>
                    <td>{r['今收价']}</td>
                    <td style="color: {color}; font-weight: bold;">{r['真实涨跌幅']}</td>
                    <td style="font-size: 12px; color: #555;">{r['AI逻辑']}</td>
                </tr>
                """
            review_html += "</table><hr>"

        top5_html = ""
        if ai_data and "top_5" in ai_data and len(ai_data["top_5"]) > 0:
            top5_html += f"""
            <h3>🧠 主力洗盘监控日记</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 避坑铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 情绪支持：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 极致缩量回踩标的 (共 {target_count} 只，左侧潜伏，静待反包)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #2c3e50; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>洗盘形态</th><th>现价</th><th>反包操作计划</th><th>核心逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#e8f4f8; color:#2980b9; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🚀 目标: {s.get('target_price', '')}<br>🛑 防守: {s.get('stop_loss', '')}</td>
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
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：强庄股大跌缩量时低吸，跌破核心均线无条件止损，反包拉升果断止盈！绝不追涨！</p>
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
        logger.info("========== 启动【强庄缩量回踩低吸战法】量化雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records, recent_stats = self.process_review_and_history(df, lookback_days=5)
        
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
        
        # 🎯 核心改变：绝不追红盘！只找绿盘或者平盘的洗盘股！
        # 今日涨跌幅必须在 -5.0% 到 +1.5% 之间 (允许微红，但绝不买大涨的)
        df = df[(df['pct_chg'] >= -5.0) & (df['pct_chg'] <= 1.5)]
        
        # 活跃中盘游资票：30亿 ~ 300亿 (放宽一点，囊括更多活跃题材)
        if df['circ_mv'].sum() > 0:
            df = df[(df['circ_mv'] >= 30_0000_0000) & (df['circ_mv'] <= 300_0000_0000)]
        
        # 必须有成交量基础
        df = df[df['amount'] >= 100000000]
        
        # 按换手率或成交额排序取前 150 名，这里主力资金博弈最激烈
        candidates = df.sort_values(by='amount', ascending=False).head(150)
        logger.info(f"👉 初筛出 {len(candidates)} 只回调企稳标的！启动K线强算，寻找极致缩量回踩...")

        scored_pool = []
        total_c = len(candidates)
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 20 == 0: logger.info(f"⏳ 洗盘扫描中... 进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                df_kline = self._get_daily_kline(code)
                if df_kline is None or len(df_kline) < 30: continue
                
                tech_df = self.calculate_technical_indicators(df_kline)
                
                # 提取最近数据
                last = tech_df.iloc[-1]
                vol_today = float(last['成交量'])
                vol_ma5 = float(last['Vol_MA5'])
                close_p = float(last['收盘'])
                ma10 = float(last['MA10'])
                ma20 = float(last['MA20'])
                ma30 = float(last['MA30'])
                
                if vol_today == 0 or vol_ma5 == 0: continue
                
                # ==========================================
                # 🔪 洗盘战法核心三大定律判断
                # ==========================================
                
                # 1. 强庄基因：过去10天内，必须出现过 > 7% 的大阳线或涨停板
                max_pct_10d = tech_df['涨跌幅'].tail(10).max()
                has_gene = max_pct_10d > 7.0
                
                # 2. 极致缩量：今天的成交量必须明显低于过去5日均量 (主力不出货)
                vol_ratio = vol_today / vol_ma5
                is_shrinking = vol_ratio < 0.80 # 萎缩到 80% 以下才及格
                
                # 3. 核心支撑位精准狙击：当前价格必须贴近 MA10 或 MA20 (+/- 3%)
                near_ma10 = abs(close_p - ma10) / ma10 <= 0.03
                near_ma20 = abs(close_p - ma20) / ma20 <= 0.03
                has_support = near_ma10 or near_ma20
                
                # 4. 中期趋势不能破底
                trend_ok = close_p > ma30
                
                # 如果四大核心有一个不满足，立刻淘汰！宁缺毋滥！
                if not (has_gene and is_shrinking and has_support and trend_ok):
                    continue
                
                # 开始打分，找出形态最完美的
                score = 50
                strategy_tag = "缩量回踩"
                support_desc = "未知"
                gene_desc = f"近10日最大涨幅{max_pct_10d:.1f}%"
                
                if vol_ratio <= 0.60:
                    score += 30 # 缩量到 60% 以下，极品地量！
                    strategy_tag = "💡 极品地量回踩"
                else:
                    score += 15
                    
                if near_ma10:
                    score += 10
                    support_desc = f"精准回踩 MA10 ({ma10:.2f})"
                elif near_ma20:
                    score += 20 # 回踩生命线加分更多
                    support_desc = f"精准回踩生命线 MA20 ({ma20:.2f})"
                    
                if max_pct_10d > 9.5:
                    score += 15 # 近期有过涨停板，反包概率极大
                    gene_desc = "近期存在涨停板基因"
                    
                # 收盘最好是小阳线或者十字星 (抗跌)
                if close_p >= float(last['开盘']):
                    score += 10
                        
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
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, review_summary)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                lesson = ai_result.get("new_lesson_learned", "")
                self.save_ai_lesson(lesson)
        
        self.send_email_report(ai_result, review_records, recent_stats, self.target_count)
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
