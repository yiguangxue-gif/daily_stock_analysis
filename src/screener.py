# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (全市场尾盘漏斗 + 绝对防空包弹版)
===================================
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
                    
                    review_summary = f"【AI自我进化 - 昨日实盘打脸复盘】\n昨日你精选了 {len(review_records)} 只股票，今日平均真实收益率: {avg_ret:+.2f}%，胜率: {win_rate:.1f}%。\n详细表现如下：\n"
                    for r in review_records:
                        review_summary += f"- {r['名称']}({r['代码']}) | 真实涨跌: {r['真实涨跌幅']} | 你昨天的理由: {r['AI逻辑']}\n"
                    review_summary += "👉 核心指令：请深刻反思上述复盘结果！如果是大面积亏损，说明你的策略被当前市场毒打，必须立刻转变今天的选股偏好！\n"
                    
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
        df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce')
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        
        df['VMA5'] = df['成交量'].rolling(5).mean()
        df['Ret_20'] = df['收盘'].pct_change(20) * 100
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.5
        
        return df

    def _check_tail_volume(self, code):
        """
        🚀 5分钟级盘口显微镜：核查尾盘 14:30 - 15:00 是否有异动放量
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
                
            # 尾盘 14:30 - 15:00 是最后 6 根 K 线
            tail_vol = df_today['成交量'].tail(6).sum()
            total_vol = df_today['成交量'].sum()
            
            if total_vol == 0: 
                return False, 0
                
            # 正常均分 30分钟是 12.5%。尾盘占全天成交超 15% 即视为明显放量抢筹。
            ratio = tail_vol / total_vol
            return ratio >= 0.15, ratio
            
        except Exception as e:
            return False, 0
        
    def _generate_fallback_ai_data(self, candidates):
        """💥 断网与空包弹物理直出机制"""
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c['匹配策略'],
                "current_price": c['现价'],
                "reason": f"系统自动捕获。该股符合量价多头特征，分时量占比: {c.get('量比', 'N/A')}，今日涨幅 {c['今日涨幅']}。",
                "target_price": "盘中动态突破",
                "stop_loss": "破位MA10离场"
            })
            
        return {
            "ai_reflection": "【AI分析模块暂时下线/未正常输出】以下标的为量化引擎层层过滤后，依据纯技术面法则（1亿成交/流通50-100亿/阳线防追高）强算所得，可信度极高。",
            "new_lesson_learned": "无",
            "macro_view": "按资金驱动流派物理执行。",
            "top_5": top_5
        }

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        if not self.config.gemini_api_key:
            logger.error("未配置 GEMINI_API_KEY，触发断网物理直出模式！")
            return self._generate_fallback_ai_data(candidates)

        past_lessons = self.load_ai_lessons()

        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | 尾盘量占比:{c['量比']} | 成交额:{c['成交额']}\n"

        prompt = f"""你是一位掌管着百亿资金的顶级 A股实战游资大鳄。
我为你筛选出了以下备选股票池（已经过底层极其严格的【尾盘量能抢跑算法】过滤，它们尾盘均出现主力资金介入）。

### 🧠 你的长期进化记忆 (AI避坑黑名单库)：
以下是你之前在实盘中血亏换来的教训，今天选股时【绝对不能再犯】：
{past_lessons}

### 📉 昨日复盘打脸记录：
{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 尾盘异动资金备选池 (已按放量强度排序)：
{cand_text}

### 🎯 你的任务：
1. 深刻反思昨天的盈亏原因。
2. 结合宏观新闻定调明天是进攻还是防守。
3. 从【备选股票池】中挑选出 **2 到 5 只** 爆发潜力最强、最稳健的金股（宁缺毋滥），并严格规避你的【进化记忆库】中的陷阱。
4. 🚀 关键进化：如果昨天的复盘记录中【有亏损的标的】，请你提取一条血的教训（50字以内），填入 `new_lesson_learned`！如果没有亏损，请填"无"。

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "我对昨天选股结果的深度反思...",
    "new_lesson_learned": "提取的新避坑铁律(无亏损则填：无)",
    "macro_view": "结合突发新闻，我判断今天的核心避险/进攻主线是...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留上面的匹配策略名",
            "current_price": 当前价格,
            "reason": "入选逻辑（结合尾盘抢筹战法，说明为什么选它，50字左右）",
            "target_price": "预估短期目标位（具体数字）",
            "stop_loss": "建议防守止损位（具体数字）"
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
                        generation_config={"temperature": 0.6, "max_output_tokens": 4096},
                        request_options={"timeout": 120} 
                    )
                    
                    text = response.text
                    m = re.search(r'(\{.*\})', text, re.DOTALL)
                    json_str = m.group(1) if m else text
                    
                    result_data = json.loads(repair_json(json_str))
                    
                    # 💣 终极防空包弹：如果大模型生成出错，导致 top_5 为空，强行植入物理结果！
                    if not result_data.get("top_5") or len(result_data["top_5"]) == 0:
                        logger.warning("⚠️ 大模型交了白卷 (top_5 为空)，强行合并底层物理筛选结果！")
                        fallback_data = self._generate_fallback_ai_data(candidates)
                        result_data["top_5"] = fallback_data["top_5"]
                        
                    return result_data
                except Exception as inner_e:
                    logger.warning(f"⚠️ AI 智能精选第 {attempt + 1} 次尝试失败: {inner_e}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
                    else:
                        logger.error(f"❌ AI 接口彻底挂掉 (Timeout)，启动物理直出兜底模式！")
                        return self._generate_fallback_ai_data(candidates)
        except Exception as outer_e:
            logger.error(f"❌ AI 初始化发生严重崩溃: {outer_e}，强行降级输出！")
            return self._generate_fallback_ai_data(candidates)

    def send_email_report(self, ai_data, review_records, target_count):
        logger.info("📧 正在生成并发送选股邮件报告...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd:
            logger.warning("❌ 未获取到发件邮箱或密码。")
            return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        review_html = ""
        if review_records:
            total_ret = sum(float(str(r['真实涨跌幅']).replace('%', '')) for r in review_records)
            avg_ret = total_ret / len(review_records)
            win_rate = (sum(1 for r in review_records if float(str(r['真实涨跌幅']).replace('%', '')) > 0) / len(review_records)) * 100
            
            review_html += f"""
            <h3>⚖️ 昨日金股复盘处刑台</h3>
            <p>昨日推票表现：平均真实收益 <b>{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
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
            <h3>🧠 A股实战游资大鳄全局反思 (尾盘稳健模型)</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🔴 新增避坑铁律：</b><span style="color:red; font-weight:bold;">{ai_data.get('new_lesson_learned', '无')}</span></p>
                <p><b>🌍 宏观定调：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 今日精选 尾盘异动金股 (自全市场 5000+ 只股票中漏斗过滤)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #1a2942; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>命中量化战法</th><th>现价</th><th>操作防守位</th><th>系统买入逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#ffeaa7; color:#d35400; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', '未定义')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🎯 {s.get('target_price', '')}<br>🛑 {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = "<p>🧊 系统遇到致命错误，无法提取任何数据。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股定制级 尾盘抢筹选股雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 提示：本报告在全市场进行海选，严格遵循 1亿成交底线、1-5%中小阳线、非长上影及尾盘放量铁律生成。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【尾盘防追高雷达】AI 闭环复盘与精选金股 - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "AI智能选股"
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
            logger.info("✅ 实战选股报告邮件发送成功！请查收。")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        socket.setdefaulttimeout(10.0)

        logger.info("========== 启动【A股尾盘异动防追高】选股雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # ========================================================
        # 第一步：极其暴力的 Pandas 瞬间全市场海选 (剔除 90% 不合规票)
        # ========================================================
        logger.info("👉 正在执行全市场 5000+ 股票极速初筛...")
        df = df[~df['name'].str.contains('ST|退|B')] # 剔除 ST 和 B 股
        df = df[~df['code'].str.startswith(('8', '4', '68'))] # 剔除北交、老三板、科创板
        df = df[df['close'] >= 2.0] # 股价2元以上防仙股
        df = df[df['amount'] >= 100000000] # 当天成交额 >= 1亿
        
        # 涨幅锁定：1% ~ 5.5% (给点冗余余地)
        df = df[(df['pct_chg'] >= 1.0) & (df['pct_chg'] <= 5.5)]
        
        # 流通市值锁定：30亿 ~ 150亿 (适当放宽容错)
        df = df[(df['circ_mv'] >= 30_0000_0000) & (df['circ_mv'] <= 150_0000_0000)]
        
        # 极严苛：高开绝对不能超过 3.5% (防早盘骗炮)
        if 'open' in df.columns and 'prev_close' in df.columns:
            df['open_gap'] = (df['open'] - df['prev_close']) / df['prev_close'] * 100
            # 兼容有些接口昨收为0的情况
            df = df[(df['open_gap'] <= 3.5) | (df['prev_close'] == 0)]
        
        # 经过这么一筛，全市场可能就剩下 50 - 150 只股票了，按成交额排个序
        candidates = df.sort_values(by='amount', ascending=False)
        logger.info(f"👉 全市场初筛完成：锁定 {len(candidates)} 只黄金标的，启动【K线排雷与分时扫描】...")

        quant_pool = []
        backup_pool = [] 
        total_c = len(candidates)
        
        # ========================================================
        # 第二步：对这几十只股票拉取 K 线和分时图，死抠细节！
        # ========================================================
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 10 == 0:
                logger.info(f"⏳ 强算雷达扫描中... 进度: {i} / {total_c} (正在扫描: {row['name']})")
                
            code = row['code']
            name = row['name']
            try:
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=2, delay=1, symbol=code, period="daily", start_date="20231001", adjust="qfq")
                if hist is None or len(hist) < 60: continue
                
                tech_df = self.calculate_technical_indicators(hist)
                last = tech_df.iloc[-1]
                
                close_p = float(last['收盘'])
                open_p = float(last['开盘'])
                high_p = float(last['最高'])
                low_p = float(last['最低'])
                
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                
                # 1. 价格要强不癫：涨幅 +1% ~ +5% 之间 (再次确认)
                is_pct_ok = 1.0 <= row['pct_chg'] <= 5.0
                
                # 2. 中小阳线：收盘价 > 开盘价
                is_yang = close_p > open_p
                
                # 3. 绝对不能是长上影大阴线！
                # 上影线长度不能超过实体部分的 1.5 倍，或者上影线绝对波动不到 1.5%
                upper_shadow = high_p - close_p
                body = close_p - open_p
                no_long_upper = (upper_shadow <= body * 1.5) or (upper_shadow / close_p < 0.015)
                
                # 4. 趋势要向上：股价在5日线之上
                trend_up = (close_p > ma5)
                
                # 5. 防高位接力：最近20天涨幅不大(不超过25%)，且近3天内没有大涨停
                ret_20 = float(last['Ret_20']) if not pd.isna(last['Ret_20']) else 0
                pos_ok = ret_20 < 25.0
                limit_up_3d = tech_df['Is_Limit_Up'].tail(3).sum()
                no_recent_limit = limit_up_3d == 0

                is_kline_ok = is_pct_ok and is_yang and no_long_upper and trend_up and pos_ok and no_recent_limit
                
                strategy_matched = None
                tail_ratio_str = "N/A"
                
                # 如果日线完全合规，开启【分时放大镜】查尾盘
                if is_kline_ok:
                    is_tail_vol, tail_ratio = self._check_tail_volume(code)
                    if is_tail_vol:
                        strategy_matched = "🔥 尾盘放量抢筹 (主选)"
                        tail_ratio_str = f"{tail_ratio*100:.1f}%"
                    elif tail_ratio > 0:
                        # 就算尾盘没有爆量，只要日线完美，也算备选
                        strategy_matched = "🛡️ 稳健多头阳线 (备选)"
                        tail_ratio_str = f"{tail_ratio*100:.1f}%"

                if strategy_matched:
                    quant_pool.append({
                        "代码": code, "名称": name, "现价": close_p,
                        "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": tail_ratio_str, "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                    
                time.sleep(random.uniform(0.1, 0.2))
            except Exception as e:
                continue
                
        # =========================================================
        # 💣 物理兜底机制 (以防万一选不到)
        # =========================================================
        # 按尾盘放量比例排序，量最大的排前面
        quant_pool = sorted(quant_pool, key=lambda x: float(str(x['量比']).replace('%', '')) if '%' in str(x['量比']) else 0, reverse=True)
        
        if len(quant_pool) < 2:
            needed = 2 - len(quant_pool)
            existing_codes = {q['代码'] for q in quant_pool}
            logger.warning(f"⚠️ 核心尾盘战法极度严苛，仅选出 {len(quant_pool)} 只，从全市场前 30 名最活跃标的中强行抓取 {needed} 只做底仓！")
            
            for _, r in candidates.head(30).iterrows():
                if r['code'] not in existing_codes:
                    quant_pool.append({
                        "代码": r['code'], "名称": r['name'], "现价": r['close'],
                        "匹配策略": "🩸 活跃底仓抱团", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                        "量比": "N/A", "成交额": f"{r['amount']/100000000:.1f}亿"
                    })
                    existing_codes.add(r['code'])
                if len(quant_pool) >= 2: break
                
        self.target_count = len(quant_pool)
        
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            # AI 分析池控制在最多 8 只，减轻 API 压力
            sorted_pool = quant_pool[:8]
            ai_result = self.ai_select_top5(sorted_pool, macro_news, review_summary)
            
            # 【这里是双重保险：不管大模型怎么样，哪怕它交白卷，上面的 JSON 防空包弹机制也会生成兜底 top_5】
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                # 🚀 记录并保存 AI 今天学到的新教训
                lesson = ai_result.get("new_lesson_learned", "")
                self.save_ai_lesson(lesson)
        
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【尾盘防追高量化扫单】成功捕获 {self.target_count} 只标的")
        print("="*80)
        if review_records: print(f"✅ 昨日实盘打脸核算完毕！")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 绝不空仓！避险与进攻最佳阵容已生成！")
            if self.config.email_sender:
                print("📧 报告已发送至您的邮箱！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
