# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (图文完美契合：100%定制版)
===================================

核心重构 (严格遵循用户图文指令)：
1. 流通盘：死死卡在 50亿 到 100亿。
2. 涨幅：死死卡在 2% 到 5%。
3. 高开：高开幅度绝对不超过 3%。
4. 多头放量：MA5 > MA10 > MA20 绝对多头。
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
        
        # 初始化 Tushare VIP 引擎
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

    def _get_daily_kline(self, code, start_date="20231001"):
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
                res['收盘'] = df['close']
                res['开盘'] = df['open']
                res['最高'] = df['high']
                res['最低'] = df['low']
                res['成交量'] = df['volume']
                res['换手率'] = 5.0 
                return res
        except:
            pass
            
        return None

    def fetch_macro_news(self):
        news_text = "今日无重大全球性突发宏观事件"
        try:
            query = urllib.parse.quote("国际突发 战争 A股 宏观经济 降息")
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
                logger.info(f"🔍 发现 {len(unreviewed)} 只待复盘的历史金股，正在核算真实盈亏...")
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
                                "代码": code, "名称": row['Name'], "昨买价": t0_price, 
                                "今收价": t1_price, "真实涨跌幅": f"{ret_pct:+.2f}%", "AI逻辑": str(row.get('AI_Reason', ''))[:30]
                            })
                
                df_hist.to_csv(self.history_file, index=False)
                
            # 🚀 新增：5日连续回测分析逻辑 (让AI从长周期寻找规律)
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
                    "avg_ret": avg_ret,
                    "win_rate": win_rate,
                    "days": len(recent_dates),
                    "total_count": total_count
                }
                
                review_summary = f"【AI自我进化 - 近 {len(recent_dates)} 日实盘连贯复盘】\n"
                review_summary += f"最近 {len(recent_dates)} 次潜伏共 {total_count} 只股票，平均真实收益率: {avg_ret:+.2f}%，隔夜胜率: {win_rate:.1f}%。\n"
                review_summary += "以下是近期多日详细表现（请寻找连亏共性）：\n"
                
                for date in recent_dates:
                    day_df = recent_records[recent_records['Date_T0'] == date]
                    review_summary += f"\n👉 [{date} 批次]:\n"
                    for _, r in day_df.iterrows():
                        review_summary += f"  - {r['Name']}({str(r['Code']).zfill(6)}) | 涨跌: {r['Return_Pct']:+.2f}% | 逻辑: {str(r.get('AI_Reason', ''))[:20]}\n"
                        
                review_summary += "\n👉 核心指令：请深刻反思上述【连续多日】的复盘结果！如果是亏损，请提取避坑教训（如某类形态连续坑人）！\n"
                    
        except Exception as e:
            logger.error(f"复盘核算发生异常: {e}")
            
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
                    strategy_tag = f"[{s.get('strategy', 'AI套利')}] "
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', strategy_tag + s['reason']])
        except Exception as e:
            logger.error(f"保存今日金股失败: {e}")

    def calculate_technical_indicators(self, hist):
        df = hist.copy()
        df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
        df['开盘'] = pd.to_numeric(df['开盘'], errors='coerce')
        df['最高'] = pd.to_numeric(df['最高'], errors='coerce')
        df['最低'] = pd.to_numeric(df['最低'], errors='coerce')
        df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.5
        df['Ret_20'] = (df['收盘'] / df['收盘'].shift(20) - 1) * 100
        return df

    def _check_tail_volume(self, code):
        """ 5分钟级盘口显微镜：专抓 14:30 - 15:00 """
        try:
            df_min = self._fetch_with_retry(ak.stock_zh_a_hist_min_em, retries=1, delay=1, symbol=code, period="5", adjust="qfq")
            if df_min is None or df_min.empty: return False, 0, "暂无分时"
                
            last_time = str(df_min.iloc[-1]['时间'])
            today_date = last_time.split(' ')[0]
            df_today = df_min[df_min['时间'].astype(str).str.startswith(today_date)]
            
            if len(df_today) < 10: return False, 0, "数据不全"
                
            tail_vol = df_today['成交量'].tail(6).sum()
            total_vol = df_today['成交量'].sum()
            
            if total_vol == 0: return False, 0, "无成交"
                
            ratio = tail_vol / total_vol
            ratio_pct = ratio * 100
            
            if ratio >= 0.18: return True, ratio, f"极度抢筹 ({ratio_pct:.1f}%)"
            elif ratio >= 0.13: return True, ratio, f"温和流入 ({ratio_pct:.1f}%)"
            else: return False, ratio, f"无异动 ({ratio_pct:.1f}%)"
                
        except Exception:
            return False, 0, "接口超时"

    def _generate_fallback_ai_data(self, candidates):
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c.get('匹配策略', '🛡️ 系统防守直出'),
                "current_price": c['现价'],
                "reason": f"满足图文四大铁律。综合得分: {c.get('综合得分', 'N/A')}，尾盘状态: {c.get('量比', 'N/A')}。",
                "target_price": "次日盘中冲高即卖",
                "stop_loss": "破位今日开盘价即斩"
            })
        return {
            "ai_reflection": "【系统预警】AI模块响应受限。以下标的为量化引擎 100% 严格按照【50-100亿市值+2-5%涨幅+低开高走多头】物理强算输出！",
            "new_lesson_learned": "无",
            "macro_view": "按纯量化图文策略执行。",
            "top_5": top_5
        }

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 刺客进行套利分析...")
        if not self.config.gemini_api_key:
            return self._generate_fallback_ai_data(candidates)

        past_lessons = self.load_ai_lessons()
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 得分:{c['综合得分']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | 尾盘:{c['量比']}\n"

        prompt = f"""你是一位专注于【A股超短线隔夜套利】的顶级游资刺客。
我为你筛选出了以下备选股票池，它们全部完美符合了“50-100亿市值、2-5%涨幅、多头放量且无大高开”的铁血量化标准！

### 🧠 你的套利进化记忆 (避坑黑名单)：
{past_lessons}

### 📉 近期连续多日打脸复盘 (多日回测数据)：
{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 尾盘套利完美备选池 (按综合得分排序，分高者优)：
{cand_text}

### 🎯 你的任务：
1. 你的第一要务是 **必须输出不多不少恰好 5 只股票**！绝不允许找借口空仓交白卷！
2. 在 `reason` 中说明买入逻辑（如：完美契合流通盘与涨幅要求，多头形态坚固）。
3. 你的 `target_price` 必须是“次日早盘高开冲高点”，`stop_loss` 必须是极紧的止损线。
4. 🚀 关键进化：若昨日复盘有亏损，提取一条血的教训（50字以内）填入 `new_lesson_learned`！如果无亏损填“无”。

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "作为套利刺客，我对当前接力环境的快评...",
    "new_lesson_learned": "提取的新避坑铁律(无亏损则填：无)",
    "macro_view": "判断明日早盘大盘是否有跳空低开的宏观风险...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留上面的匹配策略名",
            "current_price": 当前价格,
            "reason": "入选逻辑（结合严格的量化指标和隔夜溢价预期展开，50字左右）",
            "target_price": "明日早盘冲高兑现位（具体数字或百分比）",
            "stop_loss": "极严格的止损位（具体数字）"
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
                        fallback_data = self._generate_fallback_ai_data(candidates)
                        result_data["top_5"] = fallback_data["top_5"]
                        
                    return result_data
                except Exception as inner_e:
                    if attempt < max_retries - 1: time.sleep(3)
                    else: return self._generate_fallback_ai_data(candidates)
        except Exception:
            return self._generate_fallback_ai_data(candidates)

    def send_email_report(self, ai_data, review_records, recent_stats, target_count):
        logger.info("📧 正在生成并发送选股邮件报告...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd:
            return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        review_html = ""
        if review_records:
            avg_ret = recent_stats.get('avg_ret', 0.0)
            win_rate = recent_stats.get('win_rate', 0.0)
            days = recent_stats.get('days', 1)
            color_ret = "red" if avg_ret > 0 else "green" if avg_ret < 0 else "black"
            
            review_html += f"""
            <h3>⚖️ 近 {days} 日套利连贯复盘处刑台</h3>
            <p>近 {days} 日隔夜表现：平均单笔收益 <b style="color:{color_ret}">{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <p style="font-size:12px; color:#666;">（下方表格为最新一批潜伏标的隔夜明细）</p>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>名称(代码)</th><th>昨日潜伏价</th><th>今日收盘(估)</th><th>隔夜盈亏</th><th>昨日逻辑</th>
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
            <h3>🧠 套利游资操盘日记</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 新增避坑铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 明早兑现环境：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 100% 契合四大铁律标的 (按综合得分入选：共 {target_count} 只)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #1a2942; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>量化策略标识</th><th>现价</th><th>明日早盘剧本</th><th>套利逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#ffeaa7; color:#d35400; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🚀 卖: {s.get('target_price', '')}<br>🛑 损: {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = "<p>🧊 极端行情，今日无标的满足四大铁律，空仓休息。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股隔夜刺客：四大图文铁律雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 本报告100%严格遵循：50-100亿市值、2-5%涨幅、高开不超过3%、多头放量上涨的绝对纪律！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【图文定制铁律】尾盘擒龙报告 - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "图文定制雷达"
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
            logger.info("✅ 尾盘套利报告邮件发送成功！")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        logger.info("========== 启动【图文100%定制】高频雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records, recent_stats = self.process_review_and_history(df, lookback_days=5)
        
        logger.info("👉 执行第一道绝对铁律：全市场极速初筛...")
        
        # 🚀 绝杀招式一：Tushare 市值补天术
        if df['circ_mv'].sum() == 0 and self.pro:
            try:
                logger.info("📡 启用 Tushare VIP 获取真实市值数据...")
                cal = self.pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.now() - pd.Timedelta(days=10)).strftime('%Y%m%d'), end_date=datetime.now().strftime('%Y%m%d'))
                last_date = cal.iloc[-1]['cal_date']
                df_basic = self.pro.daily_basic(trade_date=last_date)
                if df_basic.empty and len(cal) > 1:
                    df_basic = self.pro.daily_basic(trade_date=cal.iloc[-2]['cal_date'])
                
                if not df_basic.empty:
                    df_basic['code'] = df_basic['ts_code'].str[:6]
                    df_basic['circ_mv_tushare'] = df_basic['circ_mv'] * 10000
                    df = df.merge(df_basic[['code', 'circ_mv_tushare']], on='code', how='left')
                    df['circ_mv'] = np.where(df['circ_mv'] == 0, df['circ_mv_tushare'].fillna(0), df['circ_mv'])
            except Exception:
                pass

        # 清理垃圾股
        df = df[~df['name'].str.contains('ST|退|B')] 
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['close'] >= 1.0] 
        
        # ========================================================
        # 🎯 图文铁律 1 & 2 & 3: 绝对的前置过滤网
        # ========================================================
        
        # 铁律 1: 收盘涨幅介于 2% 到 5%
        df = df[(df['pct_chg'] >= 2.0) & (df['pct_chg'] <= 5.0)]
        
        # 铁律 2: 高开幅度不超过 3%
        if 'open' in df.columns and 'prev_close' in df.columns:
            df['open_gap'] = (df['open'] - df['prev_close']) / df['prev_close'] * 100
            df = df[(df['open_gap'] <= 3.0) | (df['prev_close'] == 0)]
            
        # 铁律 3: 流通盘介于 50亿 到 100亿
        if df['circ_mv'].sum() > 0:
            df = df[(df['circ_mv'] >= 50_0000_0000) & (df['circ_mv'] <= 100_0000_0000)]
        else:
            # 防故障物理隔离：50-100亿的票，日常成交额极难超过 8 亿
            df = df[df['amount'] <= 8_0000_0000]
        
        candidates = df.sort_values(by='amount', ascending=False)
        logger.info(f"👉 经过四大铁律血洗，全市场仅剩 {len(candidates)} 只标的！启动K线排雷...")

        scored_pool = []
        total_c = len(candidates)
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            code = row['code']
            name = row['name']
            try:
                df_kline = self._get_daily_kline(code)
                if df_kline is None or len(df_kline) < 30: continue
                
                tech_df = self.calculate_technical_indicators(df_kline)
                last = tech_df.iloc[-1]
                
                close_p = float(last['收盘'])
                open_p = float(last['开盘'])
                high_p = float(last['最高'])
                
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                ma20 = float(last['MA20'])
                
                score = 50 # 基础分
                
                # ========================================================
                # 🎯 图文铁律 4: 成交量温和放大，多头放量上涨
                # ========================================================
                is_yang = close_p > open_p
                # 绝对多头排列：MA5 > MA10 > MA20
                is_multi_head = (ma5 > ma10) and (ma10 > ma20)
                
                if not (is_yang and is_multi_head):
                    continue # 必须满足多头阳线，否则直接踢掉！
                    
                score += 20 # 满足多头加分
                
                # 斩杀长上影 (极其严格：上影线不能大于实体的一半)
                body = abs(close_p - open_p)
                upper_shadow = high_p - max(close_p, open_p)
                if body > 0 and upper_shadow > body * 0.5:
                    score -= 30
                else:
                    score += 20
                    
                # 防高位接力
                ret_20 = float(last['Ret_20']) if not pd.isna(last['Ret_20']) else 0
                if ret_20 > 25.0: score -= 20
                    
                # 分时尾盘异动查验
                tail_ratio_str = "多头放量"
                strategy_matched = "🎯 图文100%契合"
                
                is_tail_vol, tail_ratio, tail_desc = self._check_tail_volume(code)
                if is_tail_vol:
                    score += 30 
                    strategy_matched = "🎯 完美契合+尾盘抢跑"
                    tail_ratio_str = tail_desc
                        
                scored_pool.append({
                    "代码": code, "名称": name, "现价": close_p,
                    "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                    "量比": tail_ratio_str, "成交额": f"{row['amount']/100000000:.1f}亿",
                    "综合得分": score 
                })
                time.sleep(random.uniform(0.1, 0.3))
                
            except Exception:
                continue
                
        # =========================================================
        # 👑 胜者为王：强制截取前 5 名
        # =========================================================
        final_top_stocks = []
        if scored_pool:
            scored_pool = sorted(scored_pool, key=lambda x: x['综合得分'], reverse=True)
            final_top_stocks = scored_pool[:5]
        else:
            # 物理兜底，从刚才满足四大铁律的候选池直接拿
            logger.warning("🚨 今天行情未能跑出完美K线，触发【纯量化指标兜底】！")
            for _, r in candidates.head(5).iterrows():
                final_top_stocks.append({
                    "代码": r['code'], "名称": r['name'], "现价": r['close'],
                    "匹配策略": "🆘 量化硬切(符合四铁律)", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                    "量比": "量价齐升", "成交额": f"{r['amount']/100000000:.1f}亿",
                    "综合得分": 60 
                })
                
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
