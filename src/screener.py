# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环版
===================================

核心能力：
1. 【量化初筛】：双引擎全市场扫描，过滤出底层有异动的超跌反弹标的。
2. 【昨日复盘】：自动核算上一个交易日选出股票的真实盈亏。
3. 【AI 自我反思】：将复盘结果喂给大模型，让 AI 根据真实打脸结果总结经验，优化策略。
4. 【宏观优选】：结合今日全球宏观突发新闻，从初筛池中精选出胜率最高的 TOP 5 金股。
5. 【自动触达】：将复盘结果与今日金股打包，一键发送至用户邮箱。
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
from datetime import datetime
from json_repair import repair_json

from src.config import get_config

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🚀 %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class ReboundScreener:
    def __init__(self):
        self.config = get_config()
        self.history_file = "data/screener_history.csv"
        os.makedirs("data", exist_ok=True)

    def _fetch_with_retry(self, func, retries=3, delay=2, *args, **kwargs):
        """强力网络重试装甲"""
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(delay + attempt)

    def get_market_spot(self):
        """双引擎全市场数据抓取"""
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
        """抓取全球宏观突发新闻"""
        logger.info("正在扫描全球宏观与 A股突发大事件...")
        news_text = "今日无重大全球性突发宏观事件"
        try:
            query = urllib.parse.quote("国际突发 战争 A股 宏观经济 降息")
            url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as res:
                root = ET.fromstring(res.read())
                lines = [f"- {it.find('title').text}" for it in root.findall('.//item')[:5]]
                if lines: 
                    news_text = "\n".join(lines)
        except Exception as e:
            logger.debug(f"获取宏观新闻失败: {e}")
        return news_text

    def process_review_and_history(self, market_df):
        """复盘昨日选股，核算真实盈亏，并准备给 AI 喂料"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        review_summary = "暂无往期复盘数据。"
        review_records = []
        
        if not os.path.exists(self.history_file):
            return review_summary, review_records

        try:
            df_hist = pd.read_csv(self.history_file)
            # 找到 Date_T1 为空的行（即之前选出但还未复盘的股票）
            unreviewed = df_hist[df_hist['Date_T1'].isna() | (df_hist['Date_T1'] == '')]
            
            if not unreviewed.empty:
                logger.info(f"🔍 发现 {len(unreviewed)} 只待复盘的历史金股，正在核算真实盈亏...")
                total_return = 0
                win_count = 0
                
                for idx, row in unreviewed.iterrows():
                    code = str(row['Code']).zfill(6)
                    # 从今天的全市场行情中找到这只股的今天收盘价
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
                
                # 保存复盘结果
                df_hist.to_csv(self.history_file, index=False)
                
                if review_records:
                    avg_ret = total_return / len(review_records)
                    win_rate = (win_count / len(review_records)) * 100
                    
                    # 生成给 AI 反思的文本
                    review_summary = f"【AI自我进化 - 昨日实盘打脸复盘】\n昨日你精选了 {len(review_records)} 只股票，今日平均真实收益率: {avg_ret:+.2f}%，胜率: {win_rate:.1f}%。\n详细表现如下：\n"
                    for r in review_records:
                        review_summary += f"- {r['名称']}({r['代码']}) | 真实涨跌: {r['真实涨跌幅']} | 你昨天的理由: {r['AI逻辑']}\n"
                    review_summary += "👉 核心指令：请深刻反思上述复盘结果！如果是正收益，总结经验；如果大面积亏损，说明你的策略被当前市场毒打，必须立即转变今天的选股思路（比如从大盘股切到微盘股，或从科技切到防御）！\n"
                    
        except Exception as e:
            logger.error(f"复盘核算发生异常: {e}")
            
        return review_summary, review_records

    def save_todays_picks(self, top5_stocks):
        """保存今天的精选用于明天的复盘"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_exists = os.path.exists(self.history_file)
        
        try:
            with open(self.history_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Date_T0', 'Code', 'Name', 'Price_T0', 'Date_T1', 'Price_T1', 'Return_Pct', 'AI_Reason'])
                
                for s in top5_stocks:
                    # 写入今日的买入价（以此作为 T0 基准）
                    writer.writerow([today_str, str(s['code']).zfill(6), s['name'], s['current_price'], '', '', '', s['reason']])
        except Exception as e:
            logger.error(f"保存今日金股失败: {e}")

    def ai_select_top5(self, candidates, macro_news, review_summary):
        """调用大模型，执行 AI 自进化闭环与 TOP 5 精选"""
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        
        if not self.config.gemini_api_key:
            logger.error("未配置 GEMINI_API_KEY，无法执行 AI 智能精选！")
            return []

        # 将候选股票池格式化成精简文本
        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}] {c['名称']} | 现价:{c['现价']:.2f} | 涨幅:{c['今日涨幅']} | 60日深跌:{c['60日跌幅']} | 量比:{c['量比']}\n"

        prompt = f"""你是一位掌管着百亿资金的顶级 A股量化基金经理。
我刚刚通过量化底层算法，从全市场 5000 只股票中，为你初步筛选出了 {len(candidates)} 只“底部放量、MACD/KDJ拐头”的备选股票池。
现在，你需要结合【昨日实盘复盘打脸记录】和【今日全球宏观头条】，从这个股票池中，挑选出**你认为胜率最高、最值得买入的 5 只金股**！

{review_summary}

### 🌍 今日全球宏观与突发大事件：
{macro_news}

### 📊 备选股票池 (底层量化已确认技术面企稳)：
{cand_text}

### 🎯 你的任务：
1. 先根据【昨日复盘】写一段深刻的反思（Self-Reflection）。
2. 结合【宏观新闻】定调今天的操作方向。
3. 严格从【备选股票池】中挑选出刚好 5 只爆发潜力最强的股票（代码和名称必须完全对应）。

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "我对昨天选股结果的深度反思，以及今天我因此做出的策略调整...",
    "macro_view": "结合突发新闻，我判断今天的核心避险/进攻主线是...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "current_price": 当前价格,
            "reason": "入选核心逻辑（结合宏观、题材和复盘经验，50字左右）",
            "target_price": "预估短期阻力位（具体数字）",
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
        """生成并发送 HTML 邮件报告"""
        logger.info("📧 正在生成并发送选股邮件报告...")
        
        sender = self.config.email_sender
        pwd = self.config.email_password
        receivers = self.config.email_receivers or [sender]
        
        if not sender or not pwd:
            logger.warning("未配置发件邮箱或密码，跳过邮件发送。")
            return

        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # 构建昨日复盘 HTML
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

        # 构建今日 AI Top 5 HTML
        top5_html = ""
        if ai_data and "top_5" in ai_data:
            top5_html += f"""
            <h3>🧠 AI 基金经理自我进化与宏观定调</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 复盘反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🌍 宏观视角：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 今日 TOP 5 绝杀金股 (从 {target_count} 只底层异动股中精选)</h3>
            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #e6f7ff;">
                    <th>代码</th><th>名称</th><th>现价</th><th>目标价</th><th>止损价</th><th>AI 入选逻辑 (综合宏观与复盘)</th>
                </tr>
            """
            for s in ai_data.get("top_5", []):
                top5_html += f"""
                <tr>
                    <td><b>{s.get('code', '')}</b></td>
                    <td><b>{s.get('name', '')}</b></td>
                    <td>{s.get('current_price', '')}</td>
                    <td style="color: red;">{s.get('target_price', '')}</td>
                    <td style="color: green;">{s.get('stop_loss', '')}</td>
                    <td style="font-size: 13px;">{s.get('reason', '')}</td>
                </tr>
                """
            top5_html += "</table>"
        else:
            top5_html = "<p>🧊 今日极端行情，量化雷达未选出合格股票，AI 强烈建议空仓管住手！</p>"

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50;">🚀 A股 AI自进化选股雷达报告 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999;">💡 提示：本报告由量化初筛 + AI宏观反思聚合生成。可将看好代码填入 Google 表格进行极深度的盘后体检。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"【AI自进化选股】大盘复盘与 TOP 5 金股推荐 - {today_str}"
        msg['From'] = f"{self.config.email_sender_name} <{sender}>"
        msg['To'] = ", ".join(receivers)
        msg.attach(MIMEText(html_content, 'html'))

        try:
            smtp_server = "smtp.qq.com" if "qq.com" in sender else "smtp.163.com" if "163.com" in sender else "smtp.gmail.com"
            port = 465 if smtp_server != "smtp.gmail.com" else 587
            
            server = smtplib.SMTP_SSL(smtp_server, port)
            server.login(sender, pwd)
            server.sendmail(sender, receivers, msg.as_string())
            server.quit()
            logger.info("✅ 选股复盘邮件发送成功！请查收。")
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")

    def run_screen(self):
        logger.info("========== 启动【AI自进化】量化选股雷达 ==========")
        
        # 1. 获取全市场并核算昨日真实盈亏
        df, source = self.get_market_spot()
        if df.empty:
            return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # 2. 量化初筛
        df = df[~df['name'].str.contains('ST|退')]
        df = df[~df['code'].str.startswith(('8', '4'))] 
        df = df[df['pct_chg'] > 0.0]
        df = df[df['amount'] >= 10000000]
        
        if source == "EastMoney":
            df = df[(df['market_cap'] >= 10 * 100000000) & (df['market_cap'] <= 800 * 100000000)]
            candidates = df.head(100) 
        else:
            candidates = df.sort_values(by='pct_chg', ascending=False).head(150)

        logger.info(f"粗筛完成：锁定 {len(candidates)} 只底层异动标的。进入 K 线强算阶段...")

        quant_pool = []
        for idx, row in candidates.iterrows():
            code = row['code']
            name = row['name']
            try:
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=2, delay=1, symbol=code, period="daily", start_date="20231001", adjust="qfq")
                if hist is None or len(hist) < 65: continue
                
                sp = hist['收盘']
                sv = hist['成交量']
                
                drop_60d = (sp.iloc[-1] - sp.iloc[-60]) / sp.iloc[-60] * 100
                if drop_60d > -5.0: continue 
                    
                avg_vol_5 = sv.iloc[-6:-1].mean()
                vr = sv.iloc[-1] / avg_vol_5 if avg_vol_5 > 0 else 1.0
                if vr < 0.8: continue 
                
                low_min9 = hist['最低'].rolling(9, min_periods=1).min()
                high_max9 = hist['最高'].rolling(9, min_periods=1).max()
                denom = (high_max9 - low_min9).replace(0, 1e-9)
                rsv = (sp - low_min9) / denom * 100
                k = rsv.ewm(com=2, adjust=False).mean()
                d = k.ewm(com=2, adjust=False).mean()
                j = 3 * k - 2 * d
                
                if j.iloc[-4:-1].min() < 50 and j.iloc[-1] > j.iloc[-2]:
                    exp1, exp2 = sp.ewm(span=12).mean(), sp.ewm(span=26).mean()
                    macd = exp1 - exp2
                    hist_bar = macd - macd.ewm(span=9).mean()
                    if hist_bar.iloc[-1] > hist_bar.iloc[-2]:
                        quant_pool.append({
                            "代码": code, "名称": name, "现价": sp.iloc[-1],
                            "今日涨幅": f"{row['pct_chg']:.2f}%", "60日跌幅": f"{drop_60d:.2f}%",
                            "量比": f"{vr:.2f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                        })
                time.sleep(random.uniform(0.1, 0.3))
            except: continue
                
        self.target_count = len(quant_pool)
        
        # 3. 宏观新闻抓取 & AI 精选 Top 5
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            ai_result = self.ai_select_top5(quant_pool, macro_news, review_summary)
            
            # 保存 Top 5 以备明天复盘
            if ai_result and "top_5" in ai_result:
                self.save_todays_picks(ai_result["top_5"])
        
        # 4. 发送邮件
        self.send_email_report(ai_result, review_records, self.target_count)
        
        # 控制台打印简化结果
        print("\n" + "="*80)
        print("          🏆 A股【AI自进化】今日选股与复盘已发送至邮箱")
        print("="*80)
        if review_records: print(f"✅ 昨日选股真实核算完毕！共核算 {len(review_records)} 只股票。")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 AI 结合宏观大势，已从 {self.target_count} 只初筛池中为你精选出 TOP 5。")
        else:
            print("🧊 今日市场极度恶劣，AI 强烈要求空仓。")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
