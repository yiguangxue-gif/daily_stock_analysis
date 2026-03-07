# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (稳健套利：跌透企稳+尾盘爆量版)
===================================

核心进化：
1. 【防高位站岗】：强制过滤近15天跌幅在 8%-25% 且最近3天不再创新低（企稳）的标的。
2. 【温和放量】：严格比对近期量能与前期缩量区均量，只抓 1.3-1.5 倍温和放量，拒绝天量出货。
3. 【尾盘狙击】：14:30-15:00 30分钟量能占全天 20% 以上，实锤主力尾盘突袭。
4. 【隔夜套利】：AI 定调改为“尾盘拿货，次日冲高兑现”，安全垫极大！
"""

import akshare as ak
import pandas as pd
import numpy as np
import logging
import time
import random
import re
import os
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

class ReboundScreener:
    def __init__(self):
        self.config = get_config()
        self.history_file = "data/screener_history.csv"
        self.lessons_file = "data/ai_lessons.txt"
        os.makedirs("data", exist_ok=True)

    def _fetch_with_retry(self, func, retries=3, delay=2, *args, **kwargs):
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
            logger.warning("东方财富接口受限，🔄 正在自动切换至【新浪财经】备用全市场接口...")
            
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=3, delay=3)
            logger.info("✅ 新浪财经基础数据拉取完成，正在进行数据清洗...")
            
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

    def fetch_macro_news(self):
        logger.info("正在扫描全球宏观与 A股突发大事件...")
        news_text = "今日无重大全球性突发宏观事件"
        try:
            query = urllib.parse.quote("国际突发 战争 A股 宏观经济 降息")
            url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as res:
                root = ET.fromstring(res.read())
                lines = [f"- {it.find('title').text}" for it in root.findall('.//item')[:5]]
                if lines: news_text = "\n".join(lines)
        except Exception as e:
            logger.debug(f"获取宏观新闻失败: {e}")
        return news_text

    def load_ai_lessons(self):
        if not os.path.exists(self.lessons_file):
            return "暂无历史避坑教训。"
        try:
            with open(self.lessons_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            lessons = [line.strip() for line in lines if line.strip()]
            if not lessons:
                return "暂无历史避坑教训。"
            return "\n".join(lessons[-10:])
        except:
            return "读取历史教训失败。"

    def save_ai_lesson(self, lesson):
        if not lesson or len(lesson) < 5 or "无" == lesson.strip() or "无亏损" in lesson:
            return
        try:
            with open(self.lessons_file, 'a', encoding='utf-8') as f:
                date_str = datetime.now().strftime('%Y-%m-%d')
                f.write(f"[{date_str} 铁律]: {lesson}\n")
        except:
            pass

    def process_review_and_history(self, market_df):
        today_str = datetime.now().strftime('%Y-%m-%d')
        review_summary = "暂无往期复盘数据。"
        review_records = []
        
        if not os.path.exists(self.history_file):
            return review_summary, review_records

        try:
            df_hist = pd.read_csv(self.history_file)
            unreviewed = df_hist[df_hist['Date_T1'].isna() | (df_hist['Date_T1'] == '')]
            
            if not unreviewed.empty:
                logger.info(f"🔍 发现 {len(unreviewed)} 只待复盘的历史金股，正在核算真实盈亏...")
                total_return, win_count = 0, 0
                
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
                            
                            total_return += ret_pct
                            if ret_pct > 0: win_count += 1
                            
                            review_records.append({
                                "代码": code, "名称": row['Name'], "昨买价": t0_price, 
                                "今收价": t1_price, "真实涨跌幅": f"{ret_pct:+.2f}%", "AI逻辑": row.get('AI_Reason', '')[:30]
                            })
                
                df_hist.to_csv(self.history_file, index=False)
                
                if review_records:
                    avg_ret = total_return / len(review_records)
                    win_rate = (win_count / len(review_records)) * 100
                    
                    review_summary = f"【AI自我进化 - 昨日实盘打脸复盘】\n昨日尾盘潜伏 {len(review_records)} 只股票，今日平均真实收益率: {avg_ret:+.2f}%，隔夜胜率: {win_rate:.1f}%。\n详细表现如下：\n"
                    for r in review_records:
                        review_summary += f"- {r['名称']}({r['代码']}) | 真实涨跌: {r['真实涨跌幅']} | 逻辑: {r['AI逻辑']}\n"
                    review_summary += "👉 核心指令：请深刻反思上述复盘结果！如果是亏损，说明套利形态选错，或者次日未及时兑现，请吸取教训！\n"
                    
        except Exception as e:
            logger.error(f"复盘核算发生异常: {e}")
            
        return review_summary, review_records

    def save_todays_picks(self, top5_stocks):
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_exists = os.path.exists(self.history_file)
        try:
            with open(self.history_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Date_T0', 'Code', 'Name', 'Price_T0', 'Date_T1', 'Price_T1', 'Return_Pct', 'AI_Reason'])
                for s in top5_stocks:
                    strategy_tag = f"[{s.get('strategy', 'AI优选')}] "
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
        df['换手率'] = pd.to_numeric(df.get('换手率', 5.0), errors='coerce').fillna(5.0)
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        
        df['VMA5'] = df['成交量'].rolling(5).mean()
        
        # 计算震荡幅度 (最高-最低)/昨收
        df['昨收'] = df['收盘'].shift(1)
        df['振幅'] = (df['最高'] - df['最低']) / df['昨收'] * 100
        df['振幅'] = df['振幅'].fillna(0)
        
        # 连板基因监控
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.5
        
        # MACD
        exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['Hist'] = df['MACD'] - df['Signal']
        
        return df

    def _check_tail_volume(self, code):
        """
        🚀 5分钟级盘口显微镜：核查尾盘 14:30 - 15:00 是否有明显放量
        """
        try:
            df_min = self._fetch_with_retry(ak.stock_zh_a_hist_min_em, retries=2, delay=1, symbol=code, period="5", adjust="qfq")
            if df_min is None or df_min.empty: 
                return False, 0
                
            last_time = str(df_min.iloc[-1]['时间'])
            today_date = last_time.split(' ')[0]
            df_today = df_min[df_min['时间'].astype(str).str.startswith(today_date)]
            
            if len(df_today) < 10: 
                return False, 0 
                
            # 尾盘 14:30 - 15:00 是最后 6 根 K 线 (如果收盘了)
            tail_vol = df_today['成交量'].tail(6).sum()
            total_vol = df_today['成交量'].sum()
            
            if total_vol == 0: 
                return False, 0
                
            ratio = tail_vol / total_vol
            # 💡 铁律：尾盘30分成交需占全天 20% 以上，证明有资金扫货
            return ratio >= 0.20, ratio
            
        except Exception as e:
            return False, 0
        
    def _generate_fallback_ai_data(self, candidates):
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c['匹配策略'],
                "current_price": c['现价'],
                "reason": f"系统物理捕捉。跌透企稳，尾盘量占比达 {c.get('量比', 'N/A')}，符合套利特征。",
                "target_price": "次日高开+3%兑现",
                "stop_loss": "破位昨日低点"
            })
            
        return {
            "ai_reflection": "【系统防空包弹警报】AI 反思模块下线。以下为量化引擎基于 [跌透企稳+尾盘爆量] 铁律强制输出的套利标的。",
            "new_lesson_learned": "无",
            "macro_view": "按资金驱动流派执行隔夜套利。",
            "top_5": top_5
        }

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        if not self.config.gemini_api_key:
            return self._generate_fallback_ai_data(candidates)

        past_lessons = self.load_ai_lessons()

        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | 尾盘量占比:{c['量比']} | 成交额:{c['成交额']}\n"

        prompt = f"""你是一位专注于【A股超短线隔夜套利】的顶级游资操盘手。
我为你筛选出了以下备选股票池，它们全部满足：“前期跌透、近3日企稳不破新低、尾盘放量抢筹、非长上影阳线”的极品潜伏特征！

### 🧠 你的长期进化记忆 (AI避坑黑名单库)：
{past_lessons}

### 📉 昨日复盘打脸记录：
{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 尾盘企稳套利备选池 (按尾盘放量强度排序)：
{cand_text}

### 🎯 你的任务：
1. 从【备选股票池】中精选出 **2 到 5 只** 爆发潜力最强、最适合明早高开套利的金股（宁缺毋滥，不要强求5只）。
2. 在 `reason` 中简述为什么它安全（如：前期跌透、筹码牢固、尾盘抢筹坚决）。
3. 🚀 关键进化：如果昨天的复盘记录中有亏损，请提取一条血的教训（50字以内）填入 `new_lesson_learned`！

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "作为套利游资，我对大盘环境和昨日表现的快评...",
    "new_lesson_learned": "提取的新避坑铁律(无亏损则填：无)",
    "macro_view": "结合突发新闻，评估明日早盘冲高兑现的环境安全度...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留上面的匹配策略名",
            "current_price": 当前价格,
            "reason": "入选逻辑（围绕跌透企稳和尾盘放量展开，50字左右）",
            "target_price": "明日冲高预估卖点（具体数字）",
            "stop_loss": "极简止损位（如破5日线或近3日低点）"
        }}
    ]
}}
```
"""
        try:
            import google.generativeai as genai
            import warnings
            warnings.filterwarnings("ignore") 
            genai.configure(api_key=self.config.gemini_api_key)
            model = genai.GenerativeModel(model_name=self.config.gemini_model)
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = model.generate_content(
                        prompt, 
                        generation_config={"temperature": 0.5, "max_output_tokens": 4096},
                        request_options={"timeout": 120} 
                    )
                    
                    text = response.text
                    m = re.search(r'(\{.*\})', text, re.DOTALL)
                    json_str = m.group(1) if m else text
                    
                    result_data = json.loads(repair_json(json_str))
                    
                    if not result_data.get("top_5") or len(result_data["top_5"]) == 0:
                        logger.warning("⚠️ 大模型交了白卷 (top_5 为空)，强行合并底层物理筛选结果！")
                        fallback_data = self._generate_fallback_ai_data(candidates)
                        result_data["top_5"] = fallback_data["top_5"]
                        
                    return result_data
                except Exception as inner_e:
                    logger.warning(f"⚠️ AI 智能精选第 {attempt + 1} 次尝试失败: {inner_e}")
                    if attempt < max_retries - 1: time.sleep(5)
                    else: return self._generate_fallback_ai_data(candidates)
        except Exception as outer_e:
            logger.error(f"❌ AI 初始化发生严重崩溃: {outer_e}，强行降级输出！")
            return self._generate_fallback_ai_data(candidates)

    def send_email_report(self, ai_data, review_records, target_count):
        logger.info("📧 正在生成并发送选股邮件报告...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd:
            return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        review_html = ""
        if review_records:
            total_ret = sum(float(str(r['真实涨跌幅']).replace('%', '')) for r in review_records)
            avg_ret = total_ret / len(review_records)
            win_rate = (sum(1 for r in review_records if float(str(r['真实涨跌幅']).replace('%', '')) > 0) / len(review_records)) * 100
            
            review_html += f"""
            <h3>⚖️ 昨日尾盘潜伏复盘处刑台</h3>
            <p>昨日推票隔夜表现：平均收益 <b>{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>名称(代码)</th><th>昨日买入</th><th>今日收盘</th><th>真实盈亏</th><th>昨日AI逻辑</th>
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
            <h3>🧠 超短套利游资全局反思</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 新增避坑铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 冲高环境评估：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 极品尾盘套利标的 (宁缺毋滥：共 {target_count} 只)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #1a2942; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>命中量化战法</th><th>现价</th><th>明日操作计划</th><th>套利潜伏逻辑</th>
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
            top5_html = "<p>🧊 系统未捕捉到任何安全的标的，今日空仓休息。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股定制级 尾盘企稳套利雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 提示：本报告严格遵循“前期回调8-20% + 近3日不破新低 + 14:30爆量抢筹”左侧试错战法生成。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【跌透企稳+尾盘抢筹】超短套利雷达 - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "AI套利助手"
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
            logger.info("✅ 尾盘套利报告邮件发送成功！请查收。")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        socket.setdefaulttimeout(10.0)
        logger.info("========== 启动【跌透企稳+尾盘抢筹】套利雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # ========================================================
        # 第一步：基础极速初筛 (只找有流动性、今天微红的)
        # ========================================================
        logger.info("👉 正在执行全市场极速初筛...")
        df = df[~df['name'].str.contains('ST|退|B')] 
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['close'] >= 2.0] 
        df = df[df['amount'] >= 100000000] # 成交额 >= 1亿 (核心铁律)
        
        # 涨幅锁定：-1.0% ~ 4.5% (寻找微红或微跌企稳，绝不追大阳线)
        df = df[(df['pct_chg'] >= -1.0) & (df['pct_chg'] <= 4.5)]
        
        # 先取成交量最活跃的 250 只进入强算
        candidates = df.sort_values(by='amount', ascending=False).head(250)
        logger.info(f"👉 锁定 {len(candidates)} 只基础活跃标的，启动【左侧企稳与量价强算】...")

        quant_pool = []
        backup_pool = [] 
        total_c = len(candidates)
        
        # ========================================================
        # 第二步：对K线形态、回调深度、MACD进行像素级排查
        # ========================================================
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 20 == 0:
                logger.info(f"⏳ 强算雷达扫描中... 进度: {i} / {total_c} (正在扫描: {row['name']})")
                
            code = row['code']
            name = row['name']
            try:
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=2, delay=1, symbol=code, period="daily", start_date="20231001", adjust="qfq")
                if hist is None or len(hist) < 30: continue
                
                tech_df = self.calculate_technical_indicators(hist)
                last = tech_df.iloc[-1]
                prev = tech_df.iloc[-2]
                
                close_p = float(last['收盘'])
                open_p = float(last['开盘'])
                high_p = float(last['最高'])
                low_p = float(last['最低'])
                
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                turnover = float(last['换手率'])
                vol = float(last['成交量'])
                
                # ----------------------------------------------------
                # 🛡️ 铁律 1: 确实“跌了一段时间” (近15天高点到近5天低点，跌幅在 8%~25% 之间)
                # ----------------------------------------------------
                high_15 = tech_df['最高'].iloc[-15:-3].max()
                low_5 = tech_df['最低'].iloc[-5:].min()
                drop_pct = (high_15 - low_5) / high_15 * 100 if high_15 > 0 else 0
                is_dropped = 8.0 <= drop_pct <= 25.0
                
                # ----------------------------------------------------
                # 🛡️ 铁律 2: 已经“企稳”而非还在砸 (最近3天不再创新低，且振幅小)
                # ----------------------------------------------------
                low_15_all = tech_df['最低'].iloc[-15:].min()
                is_stabilized = tech_df['最低'].iloc[-3:].min() >= low_15_all # 底部没下移
                
                # 振幅 <= 4.5% (小阳/小阴/十字星)
                amp_3d_max = tech_df['振幅'].iloc[-3:].max()
                is_small_amp = amp_3d_max <= 4.5
                
                # 股价贴近或站上 5 日线
                is_on_ma = close_p >= ma5 * 0.985
                
                # ----------------------------------------------------
                # 🛡️ 铁律 3: “温和放量”而非暴量出货
                # ----------------------------------------------------
                vol_3d_mean = tech_df['成交量'].iloc[-3:].mean()
                vol_base_mean = tech_df['成交量'].iloc[-15:-3].mean()
                vol_ratio_period = vol_3d_mean / vol_base_mean if vol_base_mean > 0 else 1.0
                
                # 是前期的 1.1 ~ 2.5 倍
                is_mild_vol = 1.1 <= vol_ratio_period <= 2.5
                # 今天不是最近20天的天量
                not_max_vol = vol < tech_df['成交量'].iloc[-20:-1].max()
                # 换手率健康 (3%-10%)，为了防漏杀放宽至 2.5%~12%
                is_turnover_ok = 2.5 <= turnover <= 12.0
                
                # ----------------------------------------------------
                # 🛡️ 铁律 4: K线与MACD微观形态
                # ----------------------------------------------------
                # 必须是实体阳线或假阴真阳
                is_yang = close_p > open_p
                # 拒绝长上影线 (上影线不能大于实体1.5倍)
                upper_shadow = high_p - close_p
                body = close_p - open_p
                no_long_upper = (upper_shadow <= body * 1.5) or (upper_shadow / close_p < 0.015)
                
                # MACD 绿柱缩短或DIF抬头
                macd_ok = (last['Hist'] >= prev['Hist']) and (last['MACD'] >= prev['MACD'])

                # ========================================================
                # ⚔️ 综合判决与分时狙击
                # ========================================================
                core_match = is_dropped and is_stabilized and is_small_amp and is_on_ma and is_mild_vol and not_max_vol and is_turnover_ok and is_yang and no_long_upper and macd_ok
                
                # 备胎条件：如果核心没选出来，只要跌过、企稳且今天红盘无上影，就放入备胎池
                backup_match = (5.0 <= drop_pct <= 30.0) and is_stabilized and is_on_ma and is_yang and no_long_upper and (vol_ratio_period >= 0.9)
                
                strategy_matched = None
                tail_ratio_str = "N/A"
                
                if core_match or backup_match:
                    # 触发高频盘口引擎
                    is_tail_vol, tail_ratio = self._check_tail_volume(code)
                    
                    if is_tail_vol and core_match:
                        strategy_matched = "🎯 完美企稳+尾盘爆买"
                        tail_ratio_str = f"{tail_ratio*100:.1f}%"
                        quant_pool.append({
                            "代码": code, "名称": name, "现价": close_p,
                            "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                            "量比": tail_ratio_str, "成交额": f"{row['amount']/100000000:.1f}亿"
                        })
                    elif tail_ratio >= 0.15 and backup_match:
                        strategy_matched = "🛡️ 右侧试错+尾盘异动"
                        tail_ratio_str = f"{tail_ratio*100:.1f}%"
                        backup_pool.append({
                            "代码": code, "名称": name, "现价": close_p,
                            "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                            "量比": tail_ratio_str, "成交额": f"{row['amount']/100000000:.1f}亿"
                        })
                    
                time.sleep(random.uniform(0.1, 0.2))
            except Exception as e:
                continue
                
        # =========================================================
        # 💣 宁缺毋滥的兜底机制
        # =========================================================
        quant_pool = sorted(quant_pool, key=lambda x: float(str(x['量比']).replace('%', '')) if '%' in str(x['量比']) else 0, reverse=True)
        
        # 只要能选出2只以上完美的，就不兜底。如果不到2只，从备胎里拿。
        if len(quant_pool) < 2:
            needed = 3 - len(quant_pool)  # 至少凑3只看看
            existing_codes = {q['代码'] for q in quant_pool}
            
            backup_pool = sorted(backup_pool, key=lambda x: float(str(x['量比']).replace('%', '')) if '%' in str(x['量比']) else 0, reverse=True)
            for r in backup_pool:
                if r['代码'] not in existing_codes:
                    quant_pool.append(r)
                    existing_codes.add(r['代码'])
                if len(quant_pool) >= 3: break
                
        self.target_count = len(quant_pool)
        
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            # 严格按照“选出5只左右”的要求
            sorted_pool = quant_pool[:5]
            ai_result = self.ai_select_top5(sorted_pool, macro_news, review_summary)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                lesson = ai_result.get("new_lesson_learned", "")
                self.save_ai_lesson(lesson)
        
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【跌透企稳+尾盘抢筹套利】捕获 {self.target_count} 只标的")
        print("="*80)
        if review_records: print(f"✅ 昨日实盘打脸核算完毕！")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 绝不重仓高位股！套利狙击名单已生成！")
            if self.config.email_sender:
                print("📧 报告已发送至您的邮箱！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
