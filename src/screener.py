# -*- coding: utf-8 -*-
"""
===================================
A股专业量化选股雷达 - AI 自进化闭环 (五大私募级策略矩阵)
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
            df['market_cap'] = pd.to_numeric(df['总市值'], errors='coerce').fillna(0)
            df['close'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
            return df, "EastMoney"
        except Exception as e:
            logger.warning("东方财富接口受限，🔄 正在自动切换至【新浪财经】备用全市场接口...")
            
        try:
            df = self._fetch_with_retry(ak.stock_zh_a_spot, retries=3, delay=3)
            logger.info("✅ 新浪财经基础数据拉取完成，正在进行数据清洗...")
            
            # 兼容不同版本的 Akshare 字段名，做极客级兜底
            col_map = {'symbol': '代码', 'name': '名称', 'changepercent': '涨跌幅', 'amount': '成交额', 'trade': '最新价'}
            for eng, chn in col_map.items():
                if chn not in df.columns and eng in df.columns:
                    df[chn] = df[eng]
                    
            df['code'] = df['代码'].str.replace(r'^[a-zA-Z]+', '', regex=True)
            df['name'] = df['名称']
            df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
            df['amount'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
            df['close'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
            df['market_cap'] = 0  
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
                    review_summary += "👉 核心指令：请深刻反思上述复盘结果！如果是大面积亏损，说明你的策略被当前市场毒打，必须根据今天的新闻立刻转变今天的选股偏好！\n"
                    
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
        """计算专业的量化技术指标因子"""
        df = hist.copy()
        
        # 均线系统
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA60'] = df['收盘'].rolling(60).mean()
        df['MA120'] = df['收盘'].rolling(120).mean()
        
        # 量能系统
        df['VMA5'] = df['成交量'].rolling(5).mean()
        df['VMA60'] = df['成交量'].rolling(60).mean()
        
        # 波动率与极值
        df['Highest_20'] = df['最高'].rolling(20).max()
        df['Lowest_60'] = df['最低'].rolling(60).min()
        df['Highest_120'] = df['最高'].rolling(120).max()
        
        # MACD
        exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['Hist'] = df['MACD'] - df['Signal']
        
        # RSI 14
        delta = df['收盘'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # 涨停基因探测 (近似判断：涨幅>9.5%)
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.5
        df['Limit_Up_Count_20'] = df['Is_Limit_Up'].rolling(20).sum()
        
        return df

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        if not self.config.gemini_api_key:
            logger.error("未配置 GEMINI_API_KEY，无法执行 AI 智能精选！")
            return None

        # 优化候选池展示格式，提供给 AI 更多因子维度
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 现价:{c['现价']:.2f} | 涨幅:{c['今日涨幅']} | 量比:{c['量比']} | RSI:{c.get('RSI', 'N/A')}\n"

        prompt = f"""你是一位掌管着百亿资金的顶级 A股量化私募基金经理。
我通过底层的【五大专业私募策略矩阵】（龙回头/VCP突破/机构长牛/左侧底/右侧起爆），从全市场 5000 只股票中，严格筛选出了 {len(candidates)} 只技术面产生极强共振的备选标的。
现在，你需要结合【昨日实盘复盘记录】和【今日全球宏观头条】，挑选出**最具爆发潜力或最安全的 5 只金股**！

{review_summary}

### 🌍 今日全球宏观与突发大事件：
{macro_news}

### 📊 专业策略备选池 (已通过严苛因子校验)：
{cand_text}

### 🎯 你的任务：
1. 深刻反思（Self-Reflection）昨天的盈亏原因。
2. 结合【宏观新闻】定调今天的操作主线（防御还是进攻？什么板块？）。
3. 结合各个股票命中的【策略战法】，挑选出刚好 5 只金股。

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "我对昨天选股结果的深度反思，以及今天做出的策略调整...",
    "macro_view": "结合突发新闻，我判断今天的核心避险/进攻主线是...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留上面列表中的匹配策略名",
            "current_price": 当前价格,
            "reason": "入选核心逻辑（结合宏观、量化策略和题材，50字左右）",
            "target_price": "预估短期目标位（具体数字）",
            "stop_loss": "建议防守止损位（具体数字）"
        }}
    ]
}}
```
"""
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.config.gemini_api_key)
            model = genai.GenerativeModel(model_name=self.config.gemini_model)
            
            response = model.generate_content(
                prompt, 
                generation_config={"temperature": 0.5, "max_output_tokens": 4096},
                request_options={"timeout": 60}
            )
            
            text = response.text
            m = re.search(r'(\{.*\})', text, re.DOTALL)
            json_str = m.group(1) if m else text
            result_data = json.loads(repair_json(json_str))
            return result_data
        except Exception as e:
            logger.error(f"AI 智能精选失败: {e}")
            return None

    def send_email_report(self, ai_data, review_records, target_count):
        logger.info("📧 正在生成并发送选股邮件报告...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd:
            logger.warning("❌ 未获取到发件邮箱或密码。请检查 GitHub Secrets 和 screener.yml 是否配置正确！")
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
        if ai_data and "top_5" in ai_data:
            top5_html += f"""
            <h3>🧠 AI 私募经理全局视野与复盘</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🌍 宏观定调：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 今日 TOP 5 专业绝杀金股 (自 {target_count} 只五大模型备选池精选)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #1a2942; color: #ffffff;">
                    <th>代码</th><th>名称</th><th>命中量化战法</th><th>现价</th><th>操作防守位</th><th>AI 机构买入逻辑</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td><span style="background:#ffeaa7; color:#d35400; padding:4px 6px; border-radius:4px; font-weight:bold; font-size: 12px;">{s.get('strategy', 'AI优选')}</span></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="font-size: 13px;">🎯 {s.get('target_price', '')}<br>🛑 {s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = "<p>🧊 今日市场极其恶劣，五大私募战法全部空仓防御，严禁伸手！</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股私募级 AI自进化选股雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 提示：本报告由五大量化因子模型结合 LLM 宏观强化学习生成。股市有风险，实盘需谨慎。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        # 强制 UTF-8 编码，彻底解决 QQ 邮箱 550 报错
        msg['Subject'] = Header(f"【私募级量化】AI 闭环复盘与选股雷达 - {today_str}", 'utf-8')
        
        # 使用 formataddr 和 Header 规范发件人格式
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
            logger.info("✅ 专业选股报告邮件发送成功！请查收。")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        logger.info("========== 启动【五大私募模型】多策略选股雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # 1. 过滤垃圾股，保障基础流动性
        df = df[~df['name'].str.contains('ST|退')]
        df = df[~df['code'].str.startswith(('8', '4', '68'))] # 聚焦主板/创业板
        df = df[df['amount'] >= 100000000] # 成交额 > 1亿 确保游资和机构能进出
        
        # 将昂贵的 Pandas 因子计算范围缩小到 150 只，防限流防卡死
        candidates = df.sort_values(by='amount', ascending=False).head(150)
        logger.info(f"👉 初筛完成：锁定全市场 {len(candidates)} 只高活跃标的，即将启动 K 线因子强算...")
        logger.info(f"👉 预计耗时 2-3 分钟，请耐心等待...")

        quant_pool = []
        backup_pool = [] # 强行兜底池，保证必有股票输出
        total_c = len(candidates)
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            # 【高频进度播报】：从20次改为10次，缓解等待焦虑
            if i % 10 == 0:
                logger.info(f"⏳ 量化推算中... 当前进度: {i} / {total_c}")
                
            code = row['code']
            name = row['name']
            try:
                # 将 start_date 缩短至 20230601，减少下载负担但依然足够计算 MA120
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=2, delay=1, symbol=code, period="daily", start_date="20230601", adjust="qfq")
                if hist is None or len(hist) < 130: continue
                
                # 获取全套专业指标
                tech_df = self.calculate_technical_indicators(hist)
                last = tech_df.iloc[-1]
                prev = tech_df.iloc[-2]
                
                vr = last['成交量'] / last['VMA5'] if last['VMA5'] > 0 else 1.0
                drop_60d = (last['收盘'] - last['Lowest_60']) / last['Lowest_60'] * 100 # 相对60日最低点反弹幅度
                drop_from_high = (last['Highest_20'] - last['收盘']) / last['Highest_20'] * 100 # 20日回撤幅度
                
                strategy_matched = None
                
                # 【专业战法 1：🐉 顶级游资·龙回头】
                if last['Limit_Up_Count_20'] >= 2 and 15 < drop_from_high < 30 and vr < 1.0 and row['pct_chg'] > -2:
                    if abs(last['收盘'] - last['MA10'])/last['MA10'] < 0.03 or abs(last['收盘'] - last['MA20'])/last['MA20'] < 0.03:
                        strategy_matched = "🐉 顶级游资·龙回头"
                        
                # 【专业战法 2：🏆 欧奈尔·VCP突破】
                elif (last['Highest_120'] - last['收盘'])/last['Highest_120'] < 0.15 and prev['成交量'] < prev['VMA60'] * 0.7:
                    if vr > 2.0 and row['pct_chg'] > 4.0 and last['收盘'] > last['MA5']:
                        strategy_matched = "🏆 欧奈尔·VCP起爆"
                        
                # 【专业战法 3：📈 机构抱团·趋势长牛】
                elif last['MA20'] > last['MA60'] > last['MA120'] and last['收盘'] > last['MA20']:
                    if 50 < last['RSI'] < 75 and 0 < row['pct_chg'] < 5 and vr < 1.8:
                        strategy_matched = "📈 机构抱团·趋势长牛"

                # 【专业战法 4：🩸 左侧绝杀·恐慌底】
                elif drop_from_high > 25 and last['RSI'] < 30 and last['Hist'] > prev['Hist'] and row['pct_chg'] >= 0:
                    strategy_matched = "🩸 左侧绝杀·恐慌底"
                    
                # 【专业战法 5：🔥 右侧点火·均线共振】
                elif prev['MA5'] <= prev['MA10'] and last['MA5'] > last['MA10'] and vr > 1.8 and last['Hist'] > 0 and prev['Hist'] <= 0:
                    strategy_matched = "🔥 右侧点火·均线共振"
                    
                # 【新增战法 6：⚡ 游资青睐·活跃异动】(防空仓放水版)
                elif vr > 1.2 and row['pct_chg'] > 1.5 and last['收盘'] > last['MA5']:
                    strategy_matched = "⚡ 游资青睐·活跃异动"

                if strategy_matched:
                    quant_pool.append({
                        "代码": code, "名称": name, "现价": last['收盘'],
                        "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": f"{vr:.2f}", "RSI": f"{last['RSI']:.1f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                else:
                    # 【强制兜底预备】：如果全都没选中，但今天没跌且有量，丢进备用池！
                    if row['pct_chg'] > 0 and vr > 1.0:
                        backup_pool.append({
                            "代码": code, "名称": name, "现价": last['收盘'],
                            "匹配策略": "🛡️ 弱市兜底·资金活口", "今日涨幅": f"{row['pct_chg']:.2f}%", 
                            "量比": f"{vr:.2f}", "RSI": f"{last['RSI']:.1f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                        })
                        
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                continue
                
        # 【终极强制兜底指令】：不管大盘多烂，强行凑齐给 AI 看！
        if len(quant_pool) < 5 and backup_pool:
            needed = 5 - len(quant_pool)
            # 按今日涨幅从大到小排序，把涨得最好的活口塞进去
            backup_pool = sorted(backup_pool, key=lambda x: float(x['今日涨幅'].strip('%')), reverse=True)
            quant_pool.extend(backup_pool[:needed])
            logger.warning(f"⚠️ 触发强行兜底机制，从死水行情中硬抠出 {min(needed, len(backup_pool))} 只资金活口！")
            
        self.target_count = len(quant_pool)
        
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            # 从合格池中按策略进行优先级排序，让AI优选 (优先给AI推送龙回头和VCP)
            sorted_pool = sorted(quant_pool, key=lambda x: ("龙回头" in x['匹配策略'], "VCP" in x['匹配策略'], "兜底" not in x['匹配策略']), reverse=True)[:30]
            ai_result = self.ai_select_top5(sorted_pool, macro_news, review_summary)
            if ai_result and "top_5" in ai_result:
                self.save_todays_picks(ai_result["top_5"])
        
        # 即使空仓也发邮件，确保知道运行结果
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【私募五大模型+强制兜底】捕获 {self.target_count} 只异动标的")
        print("="*80)
        if review_records: print(f"✅ 昨日实盘打脸核算完毕，已喂给 AI 进化模型！")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 AI 结合宏观大势，已从量化池中精选出最强 TOP 5！")
            if self.config.email_sender:
                print("📧 深度私募级报告已发送至您的邮箱！")
        else:
            print("🧊 极端死水或崩盘行情，五大专业模型强制熔断空仓，绝不送钱！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
