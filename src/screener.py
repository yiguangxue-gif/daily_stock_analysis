# -*- coding: utf-8 -*-
"""
===================================
A股游资量化选股雷达 - AI 自进化闭环 (图片三战法定制 + 绝对防弹兜底版)
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
            df['market_cap'] = pd.to_numeric(df.get('总市值', 0), errors='coerce').fillna(0)
            # 【新增提取流通市值】
            df['circ_mv'] = pd.to_numeric(df.get('流通市值', df['market_cap']), errors='coerce').fillna(0)
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
        df['开盘'] = pd.to_numeric(df['开盘'], errors='coerce')
        df['最低'] = pd.to_numeric(df['最低'], errors='coerce')
        df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
        
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        
        df['VMA5'] = df['成交量'].rolling(5).mean()
        
        # MACD
        exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
        exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['Hist'] = df['MACD'] - df['Signal']
        
        # 连板基因 (涨幅大于9.5%)
        df['Is_Limit_Up'] = (df['收盘'].pct_change() * 100) > 9.5
        df['Limit_Up_Count_10'] = df['Is_Limit_Up'].rolling(10).sum()
        
        return df
        
    def _generate_fallback_ai_data(self, candidates):
        """💥 断网物理直出机制：当 AI API 彻底连不上时，伪造数据格式强行发件"""
        top_5 = []
        for c in candidates[:5]:
            top_5.append({
                "code": c['代码'],
                "name": c['名称'],
                "strategy": c['匹配策略'],
                "current_price": c['现价'],
                "reason": f"AI服务超时，系统按【{c['匹配策略']}】战法物理直出。今日涨幅: {c['今日涨幅']}",
                "target_price": "动态止盈",
                "stop_loss": "破位离场"
            })
            
        return {
            "ai_reflection": "【系统断网警报】⚠️ Google Gemini API 响应超时，AI 反思模块下线。以下股票为底层量化模型强制输出。",
            "macro_view": "【系统断网警报】⚠️ 无法生成宏观解读，按纯技术面选股。",
            "top_5": top_5
        }

    def ai_select_top5(self, candidates, macro_news, review_summary):
        logger.info("🧠 正在唤醒 AI 基金经理进行自进化和深度筛选...")
        if not self.config.gemini_api_key:
            logger.error("未配置 GEMINI_API_KEY，触发断网物理直出模式！")
            return self._generate_fallback_ai_data(candidates)

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
        # ==============================================================
        # 🛡️ 绝对防弹的 API 调用包裹层，抓取所有 504 异常并直出数据
        # ==============================================================
        try:
            import google.generativeai as genai
            import warnings
            warnings.filterwarnings("ignore") # 忽略过期警告
            genai.configure(api_key=self.config.gemini_api_key)
            model = genai.GenerativeModel(model_name=self.config.gemini_model)
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = model.generate_content(
                        prompt, 
                        generation_config={"temperature": 0.6, "max_output_tokens": 4096},
                        request_options={"timeout": 120} # 超时延长至120秒
                    )
                    
                    text = response.text
                    m = re.search(r'(\{.*\})', text, re.DOTALL)
                    json_str = m.group(1) if m else text
                    return json.loads(repair_json(json_str))
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
            <h3>🧠 A股实战游资战法全局反思</h3>
            <div style="background-color: #fdfbf7; padding: 15px; border-left: 5px solid #d4af37; margin-bottom: 20px;">
                <p><b>🔄 闭环反思：</b>{ai_data.get('ai_reflection', '无')}</p>
                <p><b>🌍 宏观定调：</b>{ai_data.get('macro_view', '无')}</p>
            </div>
            
            <h3>🏆 今日 TOP 5 定制战法金股 (自 {target_count} 只异动池精选)</h3>
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
            <h2 style="color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px;">🚀 A股定制级 尾盘强势选股雷达 ({today_str})</h2>
            {review_html}
            {top5_html}
            <br>
            <p style="font-size: 12px; color: #999; text-align: center;">💡 提示：本报告由尾盘强势多头选股模型 + 绝对防弹兜底机制生成，保证每日送达。</p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header(f"【尾盘强势多头】AI 闭环复盘与 TOP 5 金股 - {today_str}", 'utf-8')
        
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

        logger.info("========== 启动【A股三大定制战法】选股雷达 ==========")
        
        df, source = self.get_market_spot()
        if df.empty: return
            
        review_summary, review_records = self.process_review_and_history(df)
        
        # 1. 宽泛初筛：剔除垃圾股，保障成交额 > 1亿
        df = df[~df['name'].str.contains('ST|退')]
        df = df[~df['code'].str.startswith(('8', '4', '68'))] 
        df = df[df['amount'] >= 100000000]
        
        candidates = df.sort_values(by='amount', ascending=False).head(150)
        logger.info(f"👉 初筛完成：锁定全市场 {len(candidates)} 只高活跃标的，即将启动 K 线因子强算...")

        quant_pool = []
        backup_pool = [] 
        total_c = len(candidates)
        
        for i, (idx, row) in enumerate(candidates.iterrows(), 1):
            if i % 10 == 0:
                logger.info(f"⏳ 量化推算中... 当前进度: {i} / {total_c} (正在推算: {row['name']})")
                
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
                low_p = float(last['最低'])
                
                ma5 = float(last['MA5'])
                ma10 = float(last['MA10'])
                ma20 = float(last['MA20'])
                
                vma5 = float(last['VMA5'])
                vol = float(last['成交量'])
                vr = vol / vma5 if vma5 > 0 else 1.0
                
                strategy_matched = None
                
                # ========================================================
                # 🎯 尾盘选股系统强势来袭 (图片完全定制战法)
                # ========================================================
                
                # 【因子 1：流通盘介于50亿到100亿】
                circ_mv = row.get('circ_mv', row.get('market_cap', 0))
                # 如果接口没给市值数据，防漏杀设为 True，有数据则严格过滤
                is_cap_ok = (50_0000_0000 <= circ_mv <= 100_0000_0000) if circ_mv > 0 else True
                
                # 【因子 2：收盘涨幅介于2%到5%】
                is_pct_ok = (2.0 <= row['pct_chg'] <= 5.0)
                
                # 【因子 3：高开幅度不超过3%】
                prev_close = float(prev['收盘'])
                open_gap = (open_p - prev_close) / prev_close * 100 if prev_close > 0 else 0
                is_gap_ok = (open_gap <= 3.0)
                
                # 【因子 4：成交量温和放大，多头放量上涨】
                # 温和放大：量比在 1.1 到 4.0 之间
                # 多头：MA5 > MA10 > MA20
                # 放量上涨：收盘价 > 开盘价 (纯正阳线)
                is_vol_bull_ok = (1.1 <= vr <= 4.0) and (ma5 > ma10 > ma20) and (close_p > open_p)

                if is_cap_ok and is_pct_ok and is_gap_ok and is_vol_bull_ok:
                    strategy_matched = "🎯 尾盘多头温和放量"

                # 记录符合策略的股票
                if strategy_matched:
                    quant_pool.append({
                        "代码": code, "名称": name, "现价": close_p,
                        "匹配策略": strategy_matched, "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": f"{vr:.2f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                elif row['pct_chg'] > 1.0: 
                    # 只要红盘且涨幅大于1%的，抓进兜底池
                    backup_pool.append({
                        "代码": code, "名称": name, "现价": close_p,
                        "匹配策略": "🛡️ 兜底资金活口", "今日涨幅": f"{row['pct_chg']:.2f}%", 
                        "量比": f"{vr:.2f}", "成交额": f"{row['amount']/100000000:.1f}亿"
                    })
                    
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                continue
                
        # =========================================================
        # 💣 物理级强制兜底机制：缺几只，补几只！
        # =========================================================
        if len(quant_pool) < 5:
            needed = 5 - len(quant_pool)
            existing_codes = {q['代码'] for q in quant_pool}
            logger.warning(f"⚠️ 三大策略仅选出 {len(quant_pool)} 只，触发物理级兜底补充 {needed} 只！")
            
            # 从兜底池按涨幅排序补充
            backup_pool = sorted(backup_pool, key=lambda x: float(x['今日涨幅'].strip('%')), reverse=True)
            for r in backup_pool:
                if r['代码'] not in existing_codes:
                    quant_pool.append(r)
                    existing_codes.add(r['代码'])
                if len(quant_pool) >= 5: break
                
            # 如果极度冰点连备胎都不够，硬抓全市场大屁股
            if len(quant_pool) < 5:
                for _, r in candidates.sort_values(by='amount', ascending=False).iterrows():
                    if r['code'] not in existing_codes:
                        quant_pool.append({
                            "代码": r['code'], "名称": r['name'], "现价": r['close'],
                            "匹配策略": "🩸 极致冰点硬拿", "今日涨幅": f"{r['pct_chg']:.2f}%", 
                            "量比": "N/A", "成交额": f"{r['amount']/100000000:.1f}亿"
                        })
                        existing_codes.add(r['code'])
                    if len(quant_pool) >= 5: break
            
        self.target_count = len(quant_pool)
        
        ai_result = None
        if quant_pool:
            macro_news = self.fetch_macro_news()
            # 排序传给AI，真实策略排在前面，兜底的在后面
            sorted_pool = sorted(quant_pool, key=lambda x: ("兜底" not in x['匹配策略'] and "冰点" not in x['匹配策略']), reverse=True)[:30]
            ai_result = self.ai_select_top5(sorted_pool, macro_news, review_summary)
            if ai_result and "top_5" in ai_result:
                self.save_todays_picks(ai_result["top_5"])
        
        self.send_email_report(ai_result, review_records, self.target_count)
        
        print("\n" + "="*80)
        print(f"          🏆 A股【定制三大战法+防弹兜底】成功捕获 {self.target_count} 只标的")
        print("="*80)
        if review_records: print(f"✅ 昨日实盘打脸核算完毕！")
        if ai_result and "top_5" in ai_result:
            print(f"🌟 绝不空仓！最强阵容已生成！")
            if self.config.email_sender:
                print("📧 报告已发送至您的邮箱！")
        print("================================================================================")

if __name__ == "__main__":
    screener = ReboundScreener()
    screener.run_screen()
