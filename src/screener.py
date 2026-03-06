# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (A股本土实战战法 + 绝对强制兜底版)
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
        # 强制转换为浮点数，防清洗出错
        df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
        df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA60'] = df['收盘'].rolling(60).mean()
        
        df['VMA5'] = df['成交量'].rolling(5).mean()
        
        # A股特色：判断近期是否有涨停基因 (>9.0% 就算)
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.0
        df['Limit_Up_Count_20'] = df['Is_Limit_Up'].rolling(20).sum()
        
        return df

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        if not self.config.gemini_api_key:
            logger.error("未配置 GEMINI_API_KEY，无法执行 AI 智能精选！")
            return None

        cand_text = ""
        for c in candidates:
            cand_text += f"[{c['代码']}]{c['名称']} | {c['匹配策略']} | 现价:{c['现价']} | 涨幅:{c['今日涨幅']} | 量比:{c['量比']} | 成交额:{c['成交额']}\n"

        prompt = f"""你是一位掌管着百亿资金的顶级 A股实战游资大鳄。
我为你筛选出了以下备选股票池（已经过底层异动算法过滤）。
你需要结合【昨日实盘复盘记录】和【今日宏观头条】，挑选出**最适合明日出手的 5 只金股**！哪怕市场再差，你也必须选满 5 只！

{review_summary}

### 🌍 今日大势：
{macro_news}

### 📊 异动备选池 (已排序)：
{cand_text}

### 🎯 你的任务：
1. 深刻反思昨天的盈亏原因。
2. 结合宏观新闻定调明天是进攻还是防守。
3. 从【备选股票池】中**强制挑选出刚好 5 只金股**，少一只都不行！(代码和名称必须是列表里有的)

请严格输出以下 JSON 格式：
```json
{{
    "ai_reflection": "我对昨天选股结果的深度反思，以及今天做出的策略调整...",
    "macro_view": "结合突发新闻，我判断今天的核心避险/进攻主线是...",
    "top_5": [
        {{
            "code": "股票代码",
            "name": "股票名称",
            "strategy": "原样保留上面的匹配策略名",
            "current_price": 当前价格,
            "reason": "入选核心逻辑（为什么选它，50字左右）",
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
                generation_config={"temperature": 0.6, "max_output_tokens": 4096},
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
            <h3>🧠 A股实战游资大鳄全局反思</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🌍 宏观定调：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 今日 TOP 5 实战金股 (自 {target_count} 只主力异动池精选)</h3>
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
            top5_html = "<p>🧊 AI 生成数据失败，未能解析出股票。</p>"

        html_content = f"""
        <html>
        <body style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股定制级 AI自进化选股雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 提示：本报告由 A股游资核心战法 + 绝对兜底机制生成，保证每日送达。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【A股实战游资】AI 闭环复盘与 TOP 5 金股 - {today_str}", 'utf-8')
        
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

        logger.info("========== 启动【A股实战游资】多策略选股雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # 1. 宽泛初筛：只剔除垃圾股，保障成交额 > 1亿
        df = df[~df['name'].str.contains('ST|退')]
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['amount'] >= 100000000]
        
        candidates = df.sort_values(by='amount', ascending=False).head(150)
        logger.info(f"👉 初筛完成：锁定全市场 {len(candidates)} 只高活跃标的，即将启动 K 线因子强算...")

        quant_pool = []
        total_c = len(candidates)
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 10 == 0:
                logger.info(f"⏳ 量化推算中... 当前进度: {i} / {total_c} (正在推算: {row['name']})")
                
            code = row['code']
            name = row['name']
            try:
                hist = self._fetch_with_retry(ak.stock_zh_a_hist, retries=2, delay=1, symbol=code, period="daily", start_date="20231001", adjust="qfq")
                if hist is None or len(hist) < 60: continue
                
                tech_df = self.calculate_technical_indicators(hist)
                last = tech_df.iloc[-1]
                prev = tech_df.iloc[-2]
                
                close_p = float(last['收盘'])
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                ma20 = float(last['MA20'])
                ma60 = float(last['MA60'])
                
                vma5 = float(last['VMA5'])
                vol = float(last['成交量'])
                vr = vol / vma5 if vma5 > 0 else 1.0
                
                strategy_matched = None
                
                # 【A股战法 1：🔥 游资·主升突破】(资金主攻方向)
                # 条件：多头排列，今天放量(>1.5)，近期有涨停基因，今日涨幅>2%
                if close_price > ma5 > ma20 and vr > 1.5 and last['Limit_Up_Count_20'] >= 1 and row['pct_chg'] > 2:
                    strategy_matched = "🔥 游资·主升突破"
                    
                # 【A股战法 2：🐉 游资·龙回头】(高辨识度妖股回踩)
                # 条件：近期有涨停，今天缩量(量比<0.8)，回踩到 20 日线附近企稳
                elif last['Limit_Up_Count_20'] >= 1 and vr < 0.8 and abs(close_p - ma20)/ma20 < 0.04:
                    strategy_matched = "🐉 游资·龙回头"
                    
                # 【A股战法 3：🌊 机构·底部异动】(抄底第一根大阳线)
                # 条件：前期在MA60下方运行，今日突然放倍量(>2.0)且收红
                elif prev['收盘'] < float(prev['MA60']) and vr > 2.0 and row['pct_chg'] > 0:
                    strategy_matched = "🌊 机构·底部异动"

                # 【A股战法 4：📈 机构·稳健长牛】(慢牛白马股)
                # 条件：完美的均线多头(MA20>MA60)，今天没跌，且没有缩量太狠
                elif ma20 > ma60 and close_p > ma20 and row['pct_chg'] > -1 and vr > 0.8:
                    strategy_matched = "📈 机构·稳健长牛"

                if strategy_matched:
                    quant_pool.append({
                        "代码": code, "名称": name, "现价": close_p,
                        "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": f"{vr:.2f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                    
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                # 记录但不中断，继续下一只
                continue
                
        # =========================================================
        # 💣 物理级强制兜底机制：绕过所有K线计算，强行从活跃池里抓壮丁！
        # =========================================================
        if len(quant_pool) < 5:
            needed = 5 - len(quant_pool)
            existing_codes = {q['代码'] for q in quant_pool}
            logger.warning(f"⚠️ 四大实战策略仅选出 {len(quant_pool)} 只股票，触发【物理级兜底】，强行抓取 {needed} 只资金活口！")
            
            # 第一波兜底：找今天收红盘、成交额最大的票
            safe_candidates = candidates[candidates['pct_chg'] > 0].sort_values(by='amount', ascending=False)
            for _, r in safe_candidates.iterrows():
                if r['code'] not in existing_codes:
                    quant_pool.append({
                        "代码": r['code'], "名称": r['name'], "现价": r['close'],
                        "匹配策略": "🛡️ 资金抱团·核心活口", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                        "量比": "N/A", "成交额": f"{r['amount']/100000000:.1f}亿"
                    })
                    existing_codes.add(r['code'])
                if len(quant_pool) >= 5: break
                
            # 第二波终极兜底：如果连红盘的都没有5只(千股跌停)，直接按成交额硬拿前5名！
            if len(quant_pool) < 5:
                for _, r in candidates.sort_values(by='amount', ascending=False).iterrows():
                    if r['code'] not in existing_codes:
                        quant_pool.append({
                            "代码": r['code'], "名称": r['name'], "现价": r['close'],
                            "匹配策略": "🩸 极致冰点·恐慌避风港", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                            "量比": "N/A", "成交额": f"{r['amount']/100000000:.1f}亿"
                        })
                        existing_codes.add(r['code'])
                    if len(quant_pool) >= 5: break
            
        self.target_count = len(quant_pool)
        
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            # 排序传给AI，优先展示游资战法选出来的
            sorted_pool = sorted(quant_pool, key=lambda x: "活口" not in x['匹配策略'] and "冰点" not in x['匹配策略'], reverse=True)[:30]
            ai_result = self.ai_select_top5(sorted_pool, macro_news, review_summary)
            if ai_result and "top_5" in ai_result:
                self.save_todays_picks(ai_result["top_5"])
        
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【实战游资+强制兜底】成功捕获 {self.target_count} 只标的")
        print("="*80)
        if review_records: print(f"✅ 昨日实盘打脸核算完毕！")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 绝不空仓！AI 已为你选出今日最强 TOP 5 阵容！")
            if self.config.email_sender:
                print("📧 报告已发送至您的邮箱！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
