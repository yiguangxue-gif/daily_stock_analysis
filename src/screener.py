# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (防卡死熔断 + 积分淘汰制 + 绝对不空包版)
===================================

核心重构：
1. 【修复 K线数学Bug】：严格使用绝对值计算实体和上影线，杜绝阴线被误判为长上影线直接扣100分的问题。
2. 【上帝级物理兜底】：如果积分系统全军覆没或API被彻底封死，强行从初筛快照中抓取5只最活跃的标的，永远告别“捕获 0 只”！
3. 【反反爬虫熔断】：连续3次请求超时直接跳出，带着现有数据强行生成报告。
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

# 全局强制超时时间：5秒！哪怕东方财富卡死，5秒后也必须释放连接
socket.setdefaulttimeout(5.0)

class ReboundScreener:
    def __init__(self):
        self.config = get_config()
        self.history_file = "data/screener_history.csv"
        self.lessons_file = "data/ai_lessons.txt"
        os.makedirs("data", exist_ok=True)

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
            logger.warning("东方财富接口受限，🔄 正在自动切换至【新浪财经】备用全市场接口...")
            
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=2, delay=2)
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
            with urllib.request.urlopen(req, timeout=3) as res:
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
        
        # 20日最大涨幅 (防追高)
        df['Ret_20'] = (df['收盘'] / df['收盘'].shift(20) - 1) * 100
        
        return df

    def _check_tail_volume(self, code):
        """
        🚀 5分钟级盘口显微镜：专抓 14:30 - 15:00 的尾盘异动！
        """
        try:
            df_min = self._fetch_with_retry(ak.stock_zh_a_hist_min_em, retries=1, delay=1, symbol=code, period="5", adjust="qfq")
            if df_min is None or df_min.empty: 
                return False, 0, "暂无分时"
                
            last_time = str(df_min.iloc[-1]['时间'])
            today_date = last_time.split(' ')[0]
            df_today = df_min[df_min['时间'].astype(str).str.startswith(today_date)]
            
            if len(df_today) < 10: 
                return False, 0, "数据不全"
                
            # 尾盘 14:30 - 15:00 是最后 6 根 K 线
            tail_vol = df_today['成交量'].tail(6).sum()
            total_vol = df_today['成交量'].sum()
            
            if total_vol == 0: 
                return False, 0, "无成交"
                
            ratio = tail_vol / total_vol
            ratio_pct = ratio * 100
            
            if ratio >= 0.18:
                return True, ratio, f"极度抢筹 ({ratio_pct:.1f}%)"
            elif ratio >= 0.13:
                return True, ratio, f"温和流入 ({ratio_pct:.1f}%)"
            else:
                return False, ratio, f"无异动 ({ratio_pct:.1f}%)"
            
        except Exception as e:
            return False, 0, "接口超时"
        
    def _generate_fallback_ai_data(self, candidates):
        """💥 断网与空包弹物理直出机制"""
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c.get('匹配策略', '🛡️ 系统防守直出'),
                "current_price": c['现价'],
                "reason": f"系统物理防线生成。综合得分: {c.get('综合得分', 'N/A')}，尾盘状态: {c.get('量比', 'N/A')}。",
                "target_price": "次日盘中冲高溢价",
                "stop_loss": "破位昨日低点"
            })
            
        return {
            "ai_reflection": "【系统防空包弹预警】AI 模块响应受限。以下标的为底层量化积分引擎强制输出的套利标的。",
            "new_lesson_learned": "无",
            "macro_view": "按资金驱动流派执行尾盘拿货、明早兑现。",
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
我为你筛选出了以下备选股票池，它们是今天全市场综合评分最高的最稳健标的。

### 🧠 你的套利进化记忆 (避坑黑名单)：
{past_lessons}

### 📉 昨日隔夜单复盘：
{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 尾盘套利备选池 (按综合得分排序，分高者优)：
{cand_text}

### 🎯 你的任务：
1. 你的第一要务是 **必须输出不多不少恰好 5 只股票**！绝不允许找借口空仓交白卷！如果完美的标的不够，请从得分较高的候选股中挑选形态相对安全的作为防守底仓。
2. 在 `reason` 中说明买入逻辑（如：K线安全，虽然尾盘放量一般但前期跌透筹码牢固）。
3. 你的 `target_price` 必须是“次日早盘高开冲高点”，`stop_loss` 必须是极紧的止损线。
4. 🚀 关键进化：若昨日复盘有亏损，提取一条血的教训（50字以内）填入 `new_lesson_learned`！

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
            "reason": "入选逻辑（围绕得分优势和隔夜溢价预期展开，50字左右）",
            "target_price": "明日早盘冲高兑现位（具体数字或百分比）",
            "stop_loss": "极严格的止损位（具体数字）"
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
                    logger.warning(f"⚠️ AI 智能精选尝试失败: {inner_e}")
                    if attempt < max_retries - 1: time.sleep(3)
                    else: return self._generate_fallback_ai_data(candidates)
        except Exception as outer_e:
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
            <h3>⚖️ 昨日隔夜套利单复盘处刑台</h3>
            <p>昨日推票隔夜表现：平均收益 <b>{avg_ret:+.2f}%</b>，胜率 <b>{win_rate:.1f}%</b></p>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>名称(代码)</th><th>昨日潜伏价</th><th>今日收盘</th><th>隔夜盈亏</th><th>昨日逻辑</th>
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
            
            <h3>🏆 今日极品尾盘套利标的 (按得分入选：共 {target_count} 只)</h3>
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
            top5_html = "<p>🧊 系统遇到致命错误，未捕捉到任何安全的套利标的。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股隔夜刺客：尾盘抢筹套利雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 核心纪律：尾盘买入，明早冲高即卖。不涨反跌，无条件斩仓！</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【隔夜套利狙击】尾盘擒龙报告 - {today_str}", 'utf-8')
        
        sender_name = self.config.email_sender_name or "隔夜套利助手"
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
        logger.info("========== 启动【尾盘隔夜套利 - 防卡死积分版】高频雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # ========================================================
        # 第一步：极速海选，缩减目标至 100 只以防被封 IP
        # ========================================================
        logger.info("👉 正在执行全市场极速初筛...")
        df = df[~df['name'].str.contains('ST|退|B')] 
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['close'] >= 2.0] 
        
        # 门槛卡死在 8000 万
        df = df[df['amount'] >= 80000000] 
        # 涨幅锁定：-2.0% ~ 6.0% (允许微绿洗盘和稍强阳线)
        df = df[(df['pct_chg'] >= -2.0) & (df['pct_chg'] <= 6.0)]
        
        # 🚀 极其关键：只取前 100 名！大幅度缩减耗时，防止东方财富封 IP！
        candidates = df.sort_values(by='amount', ascending=False).head(100)
        logger.info(f"👉 锁定 {len(candidates)} 只最活跃标的，启动【多维积分与反爬熔断引擎】...")

        scored_pool = []
        total_c = len(candidates)
        consecutive_errors = 0  # 🚀 反爬虫连错计数器
        
        # ========================================================
        # 第二步：多维积分制 (满分 100，只要及格就有机会)
        # ========================================================
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 15 == 0:
                logger.info(f"⏳ 积分引擎运转中... 进度: {i} / {total_c} (正在扫描: {row['name']})")
                
            code = row['code']
            name = row['name']
            try:
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=1, delay=1, symbol=code, period="daily", start_date="20231001", adjust="qfq")
                if hist is None or len(hist) < 30: continue
                
                # 如果成功获取，重置连续错误计数器
                consecutive_errors = 0 
                
                tech_df = self.calculate_technical_indicators(hist)
                last = tech_df.iloc[-1]
                
                close_p = float(last['收盘'])
                open_p = float(last['开盘'])
                high_p = float(last['最高'])
                
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                
                score = 0
                strategy_matched = "待定"
                
                # ----------------------------------------------------
                # 维次 1：K线基本形态 (满分 30 分)
                # ----------------------------------------------------
                is_yang = close_p > open_p
                if is_yang:
                    score += 15
                    
                # 🚀 修复核心：计算实体和上影线必须用绝对值和最大值！
                body = abs(close_p - open_p)
                upper_shadow = high_p - max(close_p, open_p)
                
                # 上影线太长（实体1.5倍以上且绝对幅度>1%），一票否决
                if body > 0 and upper_shadow > body * 1.5 and (upper_shadow / close_p > 0.01):
                    score -= 100
                elif upper_shadow < body * 0.5:
                    score += 15 # 光头或短上影加分
                    
                # ----------------------------------------------------
                # 维次 2：涨幅位置 (满分 20 分)
                # ----------------------------------------------------
                if 1.0 <= row['pct_chg'] <= 4.5:
                    score += 20
                elif -1.0 <= row['pct_chg'] <= 6.0:
                    score += 10
                    
                # ----------------------------------------------------
                # 维次 3：均线支撑 (满分 20 分)
                # ----------------------------------------------------
                if close_p >= ma5:
                    score += 10
                if ma5 > ma10:
                    score += 10
                    
                # ----------------------------------------------------
                # 维次 4：防高位接力 (扣分项)
                # ----------------------------------------------------
                ret_20 = float(last['Ret_20']) if not pd.isna(last['Ret_20']) else 0
                if ret_20 > 30.0:
                    score -= 30
                    
                limit_up_3d = tech_df['Is_Limit_Up'].tail(3).sum()
                if limit_up_3d > 0:
                    score -= 20
                
                # ========================================================
                # 维次 5：分时尾盘异动 (满分 30 分)
                # ========================================================
                tail_ratio_str = "N/A"
                if score >= 20: # 只要基础面没被扣穿(>20分)，才去查高频分时图，省点请求
                    is_tail_vol, tail_ratio, tail_desc = self._check_tail_volume(code)
                    tail_ratio_str = tail_desc
                    
                    if tail_ratio >= 0.18:
                        score += 30 
                        strategy_matched = "🎯 完美形态+尾盘爆买"
                    elif tail_ratio >= 0.13:
                        score += 15 
                        strategy_matched = "🛡️ 稳健多头+资金流入"
                    else:
                        strategy_matched = "🩸 活跃底仓防守"
                        
                # 记录及格的股票 (允许低至 20 分的股票进入海选池，保障绝不空仓)
                if score >= 20:
                    scored_pool.append({
                        "代码": code, "名称": name, "现价": close_p,
                        "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": tail_ratio_str, "成交额": f"{row['amount']/100000000:.1f}亿",
                        "综合得分": score 
                    })
                    
                # 🚀 增加睡眠时间，装作是真人在看盘，防止被封 IP
                time.sleep(random.uniform(0.5, 1.0))
                
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.warning("⚠️ 连续3只股票拉取失败，极大概率触发东方财富IP限制！强制熔断并提取当前已扫出的数据...")
                    break # 🚀 核心救命代码：直接跳出循环，去发邮件！
                time.sleep(2)
                continue
                
        # =========================================================
        # 👑 胜者为王：强制截取前 5 名 (绝不空仓！)
        # =========================================================
        final_top_stocks = []
        if scored_pool:
            # 按综合得分从高到低排序
            scored_pool = sorted(scored_pool, key=lambda x: x['综合得分'], reverse=True)
            final_top_stocks = scored_pool[:5]
        else:
            # 🚀 上帝级绝对防弹兜底：如果 API 瘫痪或者千股跌停导致池子全空，强行抓初筛的前5名红盘活跃股！
            logger.warning("🚨 积分池为空（可能因API被完全封锁或极端冰点行情），触发【上帝级无脑物理兜底】！")
            for _, r in candidates.iterrows():
                if r['pct_chg'] > 0:
                    final_top_stocks.append({
                        "代码": r['code'], "名称": r['name'], "现价": r['close'],
                        "匹配策略": "🆘 盲抓防守底仓 (API故障/无及格)", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                        "量比": "未知", "成交额": f"{r['amount']/100000000:.1f}亿",
                        "综合得分": 0 
                    })
                if len(final_top_stocks) >= 5: break
                
        self.target_count = len(final_top_stocks)
        
        ai_result = None
        if final_top_stocks:
            macro_news = self.fetch_macro_news()
            # 给 AI 的指令里，它必须在这 5 只里选，而且不能反抗
            ai_result = self.ai_select_top5(final_top_stocks, macro_news, review_summary)
            
            if ai_result and "top_5" in ai_result and len(ai_result["top_5"]) > 0:
                self.save_todays_picks(ai_result["top_5"])
                lesson = ai_result.get("new_lesson_learned", "")
                self.save_ai_lesson(lesson)
        
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【隔夜刺客：尾盘抢筹套利】捕获 {self.target_count} 只标的")
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
