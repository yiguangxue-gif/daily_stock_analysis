# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (5日波段主升浪 + 四大战法 + 八边形终极引擎版)
===================================

核心进化：
1. 🌊【波段视角对齐】：AI 认知从“隔夜跑路”全面升级为“3-5日波段潜伏与格局”。
2. 🏆【四大神级战法注入】：自动检测该股是否触发（趋势低吸/底部起爆/强庄首阴/均线粘合），指引波段操作。
3. 🐉【保留八边形引擎】：无损保留 Google 表格持仓同步、内资外资(北向)流向、RSI 情绪极值、真实打脸回测等所有顶级功能！
4. ⚖️【凯利动态仓位】：基于胜率计算安全下注比例。
"""

import json
import logging
import time
import re
import os
import csv
import io
import glob
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from json_repair import repair_json

import pandas as pd
import numpy as np
import akshare as ak

from src.config import get_config

logger = logging.getLogger(__name__)

# 股票名称映射
STOCK_NAME_MAP = {
    '600519': '贵州茅台', '000001': '平安银行', '300750': '宁德时代', '002594': '比亚迪',
    '600036': '招商银行', '601318': '中国平安', '000858': '五粮液', '600276': '恒瑞医药',
    '601012': '隆基绿能', '002475': '立讯精密', '300059': '东方财富', '002415': '海康威视',
    '600900': '长江电力', '601166': '兴业银行', '600028': '中国石化', 'AAPL': '苹果',
    'TSLA': '特斯拉', 'MSFT': '微软', 'NVDA': '英伟达', '00700': '腾讯控股'
}

def get_stock_name_multi_source(stock_code: str, context: Optional[Dict] = None, data_manager = None) -> str:
    if context:
        if context.get('stock_name') and not context['stock_name'].startswith('股票'):
            return context['stock_name']
        if 'realtime' in context and context['realtime'].get('name'):
            return context['realtime']['name']
    if stock_code in STOCK_NAME_MAP:
        return STOCK_NAME_MAP[stock_code]
    if data_manager:
        try:
            name = data_manager.get_stock_name(stock_code)
            if name:
                STOCK_NAME_MAP[stock_code] = name
                return name
        except: 
            pass
    return f'股票{stock_code}'

@dataclass
class AnalysisResult:
    code: str
    name: str
    sentiment_score: int
    trend_prediction: str
    operation_advice: str
    decision_type: str = "hold"
    confidence_level: str = "中"
    debate_process: Optional[Dict[str, str]] = None
    dashboard: Optional[Dict[str, Any]] = None
    trend_analysis: str = ""
    short_term_outlook: str = ""
    medium_term_outlook: str = ""
    technical_analysis: str = ""
    ma_analysis: str = ""
    volume_analysis: str = ""
    pattern_analysis: str = ""
    fundamental_analysis: str = ""
    sector_position: str = ""
    company_highlights: str = ""
    news_summary: str = ""
    market_sentiment: str = ""
    hot_topics: str = ""
    analysis_summary: str = ""
    key_points: str = ""
    risk_warning: str = ""
    buy_reason: str = ""
    market_snapshot: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None
    search_performed: bool = False
    data_sources: str = ""
    success: bool = True
    error_message: Optional[str] = None
    current_price: Optional[float] = None
    change_pct: Optional[float] = None
    user_cost: Optional[float] = None
    user_shares: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__

class GeminiAnalyzer:
    SYSTEM_PROMPT = """你是一个由三位顶尖专家组成的【A股游资量化私募决策委员会】的“总指挥”。
你的核心操盘理念是：【3-5日波段潜伏，寻找极致缩量回踩，博取轮动主升浪，冲高必砸！】

### 🤖 内部 Agent 对抗机制：
在 `debate_process` 中模拟多空专家的激烈对抗：
- **[Agent A - 波段爆发手]**：死盯“四大波段战法”（首阴、均线粘合、底部起爆、趋势低吸）。判断未来 3-5 天的最大冲高潜力。
- **[Agent B - 空头狙击手]**：死盯上方的筹码套牢密集区、存贷双高、主力资金和外资出逃漏洞，随时警告破位风险。
- **[Agent C - 认知诊断官]**：根据“打脸回测铁证”，执行归因分析。如果上次看错导致亏损，必须指出是技术、资金还是情绪误判，并强制避坑！

## 🛑 【核心法则：魂穿实盘与凯利公式】
1. **波段真实操作**：在 `ai_real_operation` 给出具体的波段操作，如“当前底仓拿5天，冲高至X元减半，跌破X元无条件斩仓”。
2. **凯利动态仓位**：在 `kelly_position_sizing` 根据系统胜率算出百分比仓位。**若大盘破位，波段仓位强制减半！**
3. **盘中条件单**：在 `conditional_order_script` 中，生成一段网格或波段防守挂单脚本。
4. **波段推演剧本**：在 `call_auction_script` 中，推演未来3-5天的走势应对（如：若连续三天无量弱反弹则清仓）。

## 输出格式：决策仪表盘 JSON (必须是纯 JSON)
```json
{
    "stock_name": "股票名称", "sentiment_score": 50, "trend_prediction": "震荡", "operation_advice": "持有",
    "decision_type": "hold", "confidence_level": "中",
    "debate_process": { 
        "bull_agent": "波段爆发手意见...", 
        "bear_agent": "空头狙击手意见...", 
        "cognitive_bias_diagnosis": "打脸归因..." 
    },
    "dashboard": {
        "core_conclusion": { "one_sentence": "...", "signal_type": "...", "time_sensitivity": "波段持仓期(3-5天)", "position_advice": { "no_position": "...", "has_position": "..." } },
        "data_perspective": {
            "a_share_features": { "market_cap_style": "...", "limit_up_gene": "...", "lhb_status": "...", "anti_harvest_radar": "..." },
            "indicator_trinity": { "macd_status": "...", "kdj_cci_status": "...", "boll_status": "..." },
            "price_position": { "current_price": 0.0, "ma5": 0.0, "ma10": 0.0, "ma20": 0.0, "bias_ma5": 0.0, "bias_status": "...", "support_level": "XX元", "resistance_level": "XX元" },
            "volume_analysis": { "volume_ratio": 0.0, "turnover_rate": 0.0, "volume_status": "...", "sentiment_temperature": "..." }
        },
        "intelligence": { 
            "latest_news": "...", "macro_impact": "...", "sector_rotation": "...", "macro_liquidity": "...", "financial_audit": "...", "risk_alerts": ["..."], "positive_catalysts": ["..."]
        },
        "battle_plan": {
            "ai_real_operation": "我的波段实盘操作是：...",
            "conditional_order_script": "波段条件单：...",
            "call_auction_script": "未来3天波段剧本推演：...",
            "sniper_points": { "ideal_buy": "XX元", "secondary_buy": "XX元", "trailing_stop": "XX元(波段破位价)", "take_profit": "XX元(波段目标价)" },
            "grid_trading_plan": { "is_recommended": true, "grid_spacing": "XX元", "buy_grid": "...", "sell_grid": "..." },
            "position_strategy": { "personal_cost_review": "...", "kelly_position_sizing": "XX%", "entry_plan": "...", "risk_control": "..." },
            "action_checklist": ["✅...", "⚠️..."]
        }
    },
    "analysis_summary": "...", "key_points": "...", "risk_warning": "...", "buy_reason": "..."
}
```
"""

    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self._api_key = api_key or config.gemini_api_key
        self._model = self._openai_client = self._anthropic_client = None
        self._current_model_name = None
        self._use_openai = self._use_anthropic = self._using_fallback = False

        if self._api_key and not self._api_key.startswith('your_') and len(self._api_key) > 10:
            try: self._init_model()
            except: self._try_anthropic_then_openai()
        else:
            self._try_anthropic_then_openai()

    def _try_anthropic_then_openai(self) -> None:
        self._init_anthropic_fallback()
        self._init_openai_fallback()

    def _init_anthropic_fallback(self) -> None:
        cfg = get_config()
        if cfg.anthropic_api_key and not cfg.anthropic_api_key.startswith('your_'):
            try:
                from anthropic import Anthropic
                self._anthropic_client = Anthropic(api_key=cfg.anthropic_api_key)
                self._current_model_name = cfg.anthropic_model
                self._use_anthropic = True
            except: pass

    def _init_openai_fallback(self) -> None:
        cfg = get_config()
        if cfg.openai_api_key and not cfg.openai_api_key.startswith('your_'):
            try:
                from openai import OpenAI
                kw = {"api_key": cfg.openai_api_key}
                if cfg.openai_base_url: kw["base_url"] = cfg.openai_base_url
                self._openai_client = OpenAI(**kw)
                self._current_model_name = cfg.openai_model
                self._use_openai = True
            except: pass

    def _init_model(self) -> None:
        try:
            import google.generativeai as genai
            genai.configure(api_key=self._api_key)
            cfg = get_config()
            self._model = genai.GenerativeModel(model_name=cfg.gemini_model, system_instruction=self.SYSTEM_PROMPT)
            self._current_model_name = cfg.gemini_model
        except:
            self._model = None

    def is_available(self) -> bool:
        return bool(self._model or self._anthropic_client or self._openai_client)

    def _call_api_with_retry(self, prompt: str, gen_cfg: dict) -> str:
        if self._use_anthropic:
            msg = self._anthropic_client.messages.create(model=self._current_model_name, max_tokens=8192, system=self.SYSTEM_PROMPT, messages=[{"role": "user", "content": prompt}])
            return msg.content[0].text
            
        if self._use_openai:
            res = self._openai_client.chat.completions.create(model=self._current_model_name, messages=[{"role": "system", "content": self.SYSTEM_PROMPT}, {"role": "user", "content": prompt}])
            return res.choices[0].message.content

        max_retries = max(get_config().gemini_max_retries, 8)
        
        for attempt in range(max_retries):
            try:
                resp = self._model.generate_content(prompt, generation_config=gen_cfg, request_options={"timeout": 120})
                if resp.text: return resp.text
            except Exception as e:
                err_str = str(e).lower()
                if '429' in err_str or 'quota' in err_str or 'rate' in err_str:
                    match = re.search(r'retry in (\d+\.?\d*)s', err_str)
                    sleep_time = float(match.group(1)) + 3.0 if match else 30.0
                    time.sleep(sleep_time)
                else:
                    time.sleep(5)
                if attempt == max_retries - 1: raise e
        return ""

    def analyze(self, context: Dict[str, Any], news_context: Optional[str] = None, announcement_context: Optional[str] = None) -> AnalysisResult:
        code = context.get('code', 'Unknown')
        name = get_stock_name_multi_source(code, context)
        if not self.is_available(): 
            return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='震荡', operation_advice='观望')
        
        try:
            google_news_text = "未发现该股票的新闻快讯"
            try:
                query = urllib.parse.quote(f"{name} 股票 质押 解禁 违规 存贷双高")
                rss_url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=5) as res:
                    root = ET.fromstring(res.read())
                    lines = []
                    fatal_keywords = ['立案', '调查', '违规', '减持', '处罚', '退市', '造假', '暴雷', 'ST', '平仓', '存贷双高', '解禁']
                    for it in root.findall('.//item')[:6]:
                        title = it.find('title').text
                        if any(kw in title for kw in fatal_keywords):
                            lines.append(f"☢️ [风控警报: 利空/解禁减持] {title}")
                        else:
                            lines.append(f"- {title}")
                    if lines: google_news_text = "\n".join(lines)
            except: pass

            prompt = self._format_prompt(context, name, news_context, google_news_text)
            res_text = self._call_api_with_retry(prompt, {"temperature": 0.7, "max_output_tokens": 8192})
            result = self._parse_response(res_text, code, name, context)
            
            result.market_snapshot = self._build_market_snapshot(context)
            result.user_cost = context.get('user_cost')
            result.user_shares = context.get('user_shares')
            return result
            
        except Exception as e:
            logger.error(f"分析异常: {e}")
            fallback_summary = f"⚠️ API 断联物理直出 ({str(e)[:80]})：\n" + context.get('_last_personal', '') + "\n" + context.get('_last_radar', '')
            result = AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='未知', operation_advice='观望', analysis_summary=fallback_summary, success=True, raw_response=fallback_summary)
            result.market_snapshot = self._build_market_snapshot(context)
            return result

    def _safe_float(self, val: Any) -> Optional[float]:
        try: return float(str(val).replace(',', '').replace('%', '').strip())
        except: return None

    def _format_prompt(self, context: Dict[str, Any], stock_name: str, news_context: Optional[str], google_news: str) -> str:
        code = context.get('code', 'Unknown')
        today = context.get('today', {})
        curr_price = self._safe_float(today.get('close'))
        
        calc_ma5 = calc_ma10 = calc_ma20 = calc_vr = poc_price = current_atr = 0.0
        strategy_str = "未触发四大波段神级战法"
        
        # 🚀 [核心注入] 四大波段神级战法在分析引擎中的应用
        if 'history' in context and len(context['history']) > 0:
            try:
                df = pd.DataFrame(context['history']).tail(120)
                for c in ['close', 'open', 'high', 'low', 'volume', 'pct_chg']:
                    if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').ffill().fillna(0)
                
                if 'pct_chg' not in df.columns:
                    df['pct_chg'] = df['close'].pct_change() * 100
                    
                df['ma5'] = df['close'].rolling(5).mean()
                df['ma10'] = df['close'].rolling(10).mean()
                df['ma20'] = df['close'].rolling(20).mean()
                df['ma30'] = df['close'].rolling(30).mean()
                df['ma60'] = df['close'].rolling(60).mean()
                df['Vol_MA5'] = df['volume'].rolling(5).mean()
                df['Max_Pct_10d'] = df['pct_chg'].rolling(10).max()
                
                last = df.iloc[-1]
                calc_ma5, calc_ma10, calc_ma20 = last['ma5'], last['ma10'], last['ma20']
                
                vol_today = float(last['volume'])
                vol_ma5 = float(last['Vol_MA5'])
                close_p = float(last['close'])
                open_p = float(last['open'])
                pct_chg = float(last['pct_chg'])
                
                if vol_ma5 > 0: calc_vr = vol_today / vol_ma5
                
                # 战法判断
                matched_strategies = []
                
                # 战法A: 趋势低吸
                if last['ma20'] > last['ma60'] and abs(close_p - last['ma20'])/last['ma20'] <= 0.03 and vol_today < vol_ma5 * 0.8:
                    matched_strategies.append("🥇 战法A: 趋势低吸 (均线多头，缩量回踩MA20生命线)")
                    
                # 战法B: 底部起爆
                if len(df) > 2 and df['close'].iloc[-2] < df['ma60'].iloc[-2] and close_p > last['ma60'] and vol_today > vol_ma5 * 2.0 and pct_chg > 4.0:
                    matched_strategies.append("🥇 战法B: 底部起爆 (低位爆量突破半年线，反转确立)")
                    
                # 战法C: 强庄首阴
                if last['Max_Pct_10d'] > 8.0 and -6.0 <= pct_chg < 0 and vol_today < vol_ma5 * 0.7:
                    matched_strategies.append("🥇 战法C: 强庄首阴 (强庄股首次极致缩量回踩，极佳潜伏点)")
                    
                # 战法D: 均线粘合
                ma_max = max(calc_ma5, calc_ma10, calc_ma20)
                ma_min = min(calc_ma5, calc_ma10, calc_ma20)
                if ma_min > 0 and (ma_max - ma_min)/ma_min < 0.03 and close_p > ma_max and open_p < ma_min and pct_chg > 3.0:
                    matched_strategies.append("🥇 战法D: 均线粘合 (一阳穿三线，波段主升浪爆发前夕)")
                    
                if matched_strategies:
                    strategy_str = "\n".join(matched_strategies)
                    
                # ATR 和 筹码密集区
                current_atr = pd.concat([df['high']-df['low'], (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1).rolling(14).mean().iloc[-1]
                if df['close'].nunique() > 1: poc_price = df.groupby(pd.cut(df['close'], bins=12, duplicates='drop'), observed=False)['volume'].sum().idxmax().mid 
                else: poc_price = curr_price
                
                context['calc_ma5'] = calc_ma5; context['calc_ma10'] = calc_ma10; context['calc_ma20'] = calc_ma20
                context['calc_vr'] = calc_vr; context['calc_poc'] = poc_price; context['calc_atr'] = current_atr
                
            except Exception as e: logger.debug(f"强算引擎异常: {e}")

        # =======================================================
        # 🛡️ 八边形终极引擎 (外资追踪 + 胜率打脸系统 + Google Sheet)
        # =======================================================
        personal_status_text = ""
        try:
            # 【1. 云端仓位与盈亏计算】
            csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv"
            my_cost, my_shares = None, None
            if curr_price and curr_price != 'N/A':
                req = urllib.request.Request(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=15) as res:
                    content = res.read().decode('utf-8-sig')
                    reader = csv.reader(io.StringIO(content))
                    for row in reader:
                        if len(row) >= 2 and row[0].strip():
                            r_code = ''.join(filter(str.isdigit, str(row[0]))).zfill(6)
                            if r_code == code:
                                my_cost = float(str(row[1]).replace(',', '').strip())
                                my_shares = int(float(str(row[2]).replace(',', '').strip())) if len(row) >= 3 and row[2].strip() else 0
                if my_cost:
                    context['user_cost'] = my_cost
                    context['user_shares'] = my_shares
                    profit_pct = ((float(curr_price) - my_cost) / my_cost) * 100
                    status_emoji = "🔴套牢中" if profit_pct < 0 else "🟢盈利中"
                    personal_status_text += f"\n### 💰 我的私人波段持仓 (实时同步)\n* **成本价**：{my_cost:.2f} 元 | **持仓**：{my_shares} 股\n* **当前盈亏**：{profit_pct:.2f}% ({status_emoji})\n* **🚨 操盘指令**：针对此成本，必须在 `ai_real_operation` 中给出波段做T解套或格局冲高的具体操作！\n"

            # 【2. 主力内资流向】
            try:
                fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
                latest_flow = fund_flow.iloc[-1]
                flow_desc = f"主力净流入: {latest_flow['主力净流入-净额']/10000:.1f}万" if latest_flow['主力净流入-净额'] != 0 else "微小"
                personal_status_text += f"\n### 🌊 聪明钱动向 (内资)\n* **今日资金流**：{flow_desc}\n"
            except: pass

            # 【3. 深度暗网雷达：北向外资追踪】
            try:
                hk_funds = ak.stock_hsgt_stock_statistics_em()
                my_hk = hk_funds[hk_funds['代码'] == code]
                if not my_hk.empty:
                    hk_hold = my_hk.iloc[0]['持股比例']
                    hk_change = my_hk.iloc[0]['今日增持估计-市值']
                    action_str = "🟢流入" if hk_change > 0 else "🔴砸盘流出"
                    personal_status_text += f"### 🕵️‍♂️ 外资(北向)追踪\n* **北向资金持股比例**：{hk_hold}%\n* **今日外资动向**：{action_str} {abs(hk_change)/10000:.1f}万元\n"
            except: pass

            # 【4. RSI 技术极值与板块共振】
            try:
                if 'history' in context:
                    prices = [d['close'] for d in context['history'][-20:]]
                    if len(prices) > 6:
                        delta = pd.Series(prices).diff()
                        gain = delta.where(delta > 0, 0).rolling(window=6).mean()
                        loss = -delta.where(delta < 0, 0).rolling(window=6).mean()
                        rs = gain / loss
                        rsi6 = 100 - (100 / (1 + rs.iloc[-1]))
                        personal_status_text += f"\n### 📊 波段技术情绪\n* **6日RSI**：{rsi6:.1f} (若>80则严重超买，易现波段高点)\n"
                
                industry_df = ak.stock_board_industry_name_em()
                top_up = industry_df.head(3)['板块名称'].tolist()
                top_down = industry_df.tail(3)['板块名称'].tolist()
                personal_status_text += f"* **今日领涨板块**：{', '.join(top_up)} | **领跌板块**：{', '.join(top_down)}\n"
            except: pass

            # 【5. AI 波段闭环回测与胜率打脸系统】
            try:
                db_file = "reports/ai_trade_log.csv"
                today_str = today.get('date', datetime.now().strftime('%Y-%m-%d'))
                file_exists = os.path.isfile(db_file)
                
                with open(db_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not file_exists: writer.writerow(['Date', 'Code', 'ClosePrice'])
                    writer.writerow([today_str, code, curr_price])
                
                if file_exists:
                    df_log = pd.read_csv(db_file)
                    df_code = df_log[df_log['Code'] == code].tail(3)
                    if len(df_code) >= 2:
                        past_price = float(df_code.iloc[-2]['ClosePrice'])
                        past_date = df_code.iloc[-2]['Date']
                        curr_p = float(curr_price)
                        ai_impact = ((curr_p - past_price) / past_price) * 100
                        status_judge = "❌ 波段导致亏损" if ai_impact < 0 else "✅ 波段成功获利"
                        
                        evidence_text = f"上次分析时 ({past_date}) 股价：{past_price:.2f}元 ➔ 最新股价：{curr_p:.2f}元 | 区间涨跌：{ai_impact:.2f}% ({status_judge})"
                        context['_evidence_impact'] = evidence_text
                        personal_status_text += f"\n### ⚖️ 波段绩效审判庭 (实盘回测)\n* {evidence_text}\n* **【处刑指令】**：如果波段区间涨跌为负，必须在 debate_process 中进行深刻的【波段归因诊断】，并立刻调整为防守模型！\n"
            except Exception as e:
                pass

        except Exception as e:
            logger.error(f"增强引擎加载失败: {e}")
        # =======================================================
        # 八边形终极引擎结束
        # =======================================================

        hardcore_radar_text = f"""### 🎯 Agent A 专属波段雷达
* 🏆 **波段战法共振探测**：\n{strategy_str}
* 🧱 **最大套牢筹码峰 (波段阻力位)**：约 {poc_price:.2f}元
"""
        context['_last_personal'] = personal_status_text
        context['_last_radar'] = hardcore_radar_text

        prompt = f"""# 决策仪表盘深度波段分析: {stock_name}({code})

{personal_status_text}
{hardcore_radar_text}

## 📰 全球与个股情报双引擎
{google_news}

请严格输出 JSON 格式决策仪表盘。
"""
        return prompt

    def _format_volume(self, volume: Optional[float]) -> str:
        if volume is None or volume == 'N/A': return 'N/A'
        try:
            vol = float(volume)
            if vol >= 1e8: return f"{vol / 1e8:.2f} 亿股"
            elif vol >= 1e4: return f"{vol / 1e4:.2f} 万股"
            else: return f"{vol:.0f} 股"
        except: return 'N/A'

    def _format_amount(self, amount: Optional[float]) -> str:
        if amount is None or amount == 'N/A': return 'N/A'
        try:
            amt = float(amount)
            if amt >= 1e8: return f"{amt / 1e8:.2f} 亿元"
            elif amt >= 1e4: return f"{amt / 1e4:.2f} 万元"
            else: return f"{amt:.0f} 元"
        except: return 'N/A'

    def _format_percent(self, value: Optional[float]) -> str:
        if value is None or value == 'N/A': return 'N/A'
        try: return f"{float(value):.2f}%"
        except: return 'N/A'

    def _format_price(self, value: Optional[float]) -> str:
        if value is None or value == 'N/A': return 'N/A'
        try: return f"{float(value):.2f}"
        except: return 'N/A'

    def _build_market_snapshot(self, context: Dict[str, Any]) -> Dict[str, Any]:
        today = context.get('today', {})
        rt = context.get('realtime', {})
        yesterday = context.get('yesterday', {})
        
        close_p = today.get('close') if today.get('close') not in [None, 'N/A', ''] else context.get('computed_close')
        open_p = today.get('open') if today.get('open') not in [None, 'N/A', ''] else context.get('computed_open')
        high_p = today.get('high') if today.get('high') not in [None, 'N/A', ''] else context.get('computed_high')
        low_p = today.get('low') if today.get('low') not in [None, 'N/A', ''] else context.get('computed_low')
        vol_p = today.get('volume') if today.get('volume') not in [None, 'N/A', ''] else context.get('computed_volume')
        amt_p = today.get('amount') if today.get('amount') not in [None, 'N/A', ''] else context.get('computed_amount')
        pct_chg = today.get('pct_chg') if today.get('pct_chg') not in [None, 'N/A', ''] else context.get('computed_pct_chg')
        
        prev_close = yesterday.get('close')
        if prev_close in [None, 'N/A', ''] and 'history' in context and len(context['history']) >= 2:
            prev_close = context['history'][-2].get('close')

        amplitude = change_amount = None
        if prev_close and high_p and low_p:
            try: amplitude = (float(high_p) - float(low_p)) / float(prev_close) * 100
            except: pass
        if prev_close and close_p:
            try: change_amount = float(close_p) - float(prev_close)
            except: pass
            
        source_str = '量化推算引擎'

        return {
            "date": context.get('date', '未知'),
            "close": self._format_price(close_p),
            "open": self._format_price(open_p),
            "high": self._format_price(high_p),
            "low": self._format_price(low_p),
            "prev_close": self._format_price(prev_close),
            "pct_chg": self._format_percent(pct_chg),
            "change_amount": self._format_price(change_amount),
            "amplitude": self._format_percent(amplitude),
            "volume": self._format_volume(vol_p),
            "amount": self._format_amount(amt_p),
            "price": self._format_price(rt.get('price', close_p)),
            "volume_ratio": rt.get('volume_ratio', 'N/A'),
            "turnover_rate": self._format_percent(rt.get('turnover_rate')),
            "source": source_str
        }

    def _parse_response(self, text: str, code: str, name: str, context: Dict[str, Any] = None) -> AnalysisResult:
        
        def _clean_n_a(s):
            if not isinstance(s, str): return s
            s = re.sub(r'N/A', '', s, flags=re.IGNORECASE)
            s = re.sub(r'未知', '', s)
            return s.strip()

        def _is_empty_or_na(val):
            if val is None: return True
            v = str(val).strip().upper()
            return any(x in v for x in ['N/A', 'NA', '未知', 'NONE', 'NULL', '暂无']) or v in ['', '0', '0.0']

        try:
            m = re.search(r'(\{.*\})', text, re.DOTALL)
            json_str = m.group(1) if m else text
            d = json.loads(repair_json(json_str))
            
            dash = d.get('dashboard'); 
            if not isinstance(dash, dict): dash = {}; d['dashboard'] = dash
            
            bp = dash.get('battle_plan'); 
            if not isinstance(bp, dict): bp = {}; dash['battle_plan'] = bp
            
            intel = dash.get('intelligence'); 
            if not isinstance(intel, dict): intel = {}; dash['intelligence'] = intel
            
            debate = d.get('debate_process'); 
            if not isinstance(debate, dict): debate = {}; d['debate_process'] = debate
            
            dp = dash.get('data_perspective'); 
            if not isinstance(dp, dict): dp = {}; dash['data_perspective'] = dp
            
            pp = dp.get('price_position'); 
            if not isinstance(pp, dict): pp = {}; dp['price_position'] = pp
            
            va = dp.get('volume_analysis'); 
            if not isinstance(va, dict): va = {}; dp['volume_analysis'] = va
            
            sp = bp.get('sniper_points'); 
            if not isinstance(sp, dict): sp = {}; bp['sniper_points'] = sp
            
            ps = bp.get('position_strategy'); 
            if not isinstance(ps, dict): ps = {}; bp['position_strategy'] = ps
            
            if context:
                if _is_empty_or_na(pp.get('ma5')): pp['ma5'] = f"{context.get('calc_ma5', 0):.2f}"
                if _is_empty_or_na(pp.get('ma10')): pp['ma10'] = f"{context.get('calc_ma10', 0):.2f}"
                if _is_empty_or_na(pp.get('ma20')): pp['ma20'] = f"{context.get('calc_ma20', 0):.2f}"
                if _is_empty_or_na(pp.get('support_level')): pp['support_level'] = f"{context.get('calc_ma20', 0):.2f}元" 
                if _is_empty_or_na(pp.get('resistance_level')): pp['resistance_level'] = f"{context.get('calc_poc', 0):.2f}元" 
                if _is_empty_or_na(va.get('volume_ratio')): va['volume_ratio'] = f"{context.get('calc_vr', 1.0):.2f}"
                
                if _is_empty_or_na(sp.get('trailing_stop')):
                    calc_atr = context.get('calc_atr', 0.0)
                    curr_p = context.get('computed_close', 0.0)
                    stop_p = curr_p - (1.5 * calc_atr) if calc_atr > 0 else curr_p * 0.95
                    sp['trailing_stop'] = f"{stop_p:.2f}元" if curr_p > 0 else "破位离场"
                else:
                    sp['trailing_stop'] = _clean_n_a(sp['trailing_stop']) or f"{context.get('calc_ma20', 0):.2f}元"

                if _is_empty_or_na(sp.get('take_profit')):
                    calc_poc = context.get('calc_poc', 0.0)
                    sp['take_profit'] = f"{calc_poc:.2f}元" if calc_poc > 0 else "波段冲高止盈"
                else:
                    sp['take_profit'] = _clean_n_a(sp['take_profit']) or "波段冲高止盈"
                    
                if _is_empty_or_na(sp.get('ideal_buy')):
                    sp['ideal_buy'] = f"{context.get('computed_close', 0.0):.2f}元"
                else:
                    sp['ideal_buy'] = _clean_n_a(sp['ideal_buy']) or f"{context.get('computed_close', 0.0):.2f}元"
                    
                if _is_empty_or_na(sp.get('secondary_buy')):
                    sp['secondary_buy'] = f"{context.get('calc_ma10', 0.0):.2f}元"
                else:
                    sp['secondary_buy'] = _clean_n_a(sp['secondary_buy']) or f"{context.get('calc_ma10', 0.0):.2f}元"

            # =======================================================
            # 🛡️ 降维寄生补丁：把所有核武级数据无损寄生到基础文本中！
            # =======================================================
            ai_real_op = bp.get('ai_real_operation', '')
            kelly_pos = ps.get('kelly_position_sizing', '')
            cond_script = bp.get('conditional_order_script', '')
            auc_script = bp.get('call_auction_script', '')
            cog_bias = debate.get('cognitive_bias_diagnosis', '')
            bull_agent = debate.get('bull_agent', '')
            bear_agent = debate.get('bear_agent', '')
            fin_audit = intel.get('financial_audit', '')
            
            flattened_points = ""
            evidence = context.get('_evidence_impact', '')
            if evidence: flattened_points += f" [波段审判铁证]: {evidence}"
            
            def clean_text(t):
                if _is_empty_or_na(t) or "..." in t: return ""
                return str(t).replace('\n', ' ').replace('\r', '').replace('**', '').replace('*', '')

            op_cl = clean_text(ai_real_op)
            if op_cl: flattened_points += f" [波段操作]: {op_cl}"
            
            kelly_cl = clean_text(kelly_pos)
            if kelly_cl: flattened_points += f" [凯利仓位]: {kelly_cl}"
            
            auc_cl = clean_text(auc_script)
            if auc_cl: flattened_points += f" [波段推演]: {auc_cl}"

            cond_cl = clean_text(cond_script)
            if cond_cl: flattened_points += f" [条件挂单]: {cond_cl}"
            
            bias_cl = clean_text(cog_bias)
            if bias_cl: flattened_points += f" [归因诊断]: {bias_cl}"
            
            bull_cl = clean_text(bull_agent)
            if bull_cl: flattened_points += f" [多头意见]: {bull_cl}"
            
            bear_cl = clean_text(bear_agent)
            if bear_cl: flattened_points += f" [空头意见]: {bear_cl}"
            
            fin_cl = clean_text(fin_audit)
            if fin_cl: flattened_points += f" [风控审计]: {fin_cl}"

            if flattened_points:
                cc = dash.setdefault('core_conclusion', {})
                old_one_sentence = cc.get('one_sentence', '')
                cc['one_sentence'] = old_one_sentence + "\n\n🤖 联合战报 ➔" + flattened_points

            d['key_points'] = "详情请阅上方联合战报及风控清单"

            fixed_json_str = json.dumps(d, ensure_ascii=False)

            ai_stock_name = d.get('stock_name')
            if ai_stock_name and (name.startswith('股票') or name == code or 'Unknown' in name): name = ai_stock_name

            decision_type = d.get('decision_type', '')
            if not decision_type:
                op = d.get('operation_advice', '持有')
                if op in ['买入', '加仓', '强烈买入']: decision_type = 'buy'
                elif op in ['卖出', '减仓', '强烈卖出']: decision_type = 'sell'
                else: decision_type = 'hold'
            
            return AnalysisResult(
                code=code, name=name, sentiment_score=int(d.get('sentiment_score', 50)),
                trend_prediction=d.get('trend_prediction', '震荡'), operation_advice=d.get('operation_advice', '持有'),
                decision_type=decision_type, confidence_level=d.get('confidence_level', '中'),
                debate_process=debate, dashboard=dash,
                analysis_summary=d.get('analysis_summary', '完成'), 
                key_points=d.get('key_points', '详情请阅上方联合战报'),
                success=True, raw_response=fixed_json_str 
            )
        except Exception as e:
            logger.warning(f"JSON 解析失败: {e}，触发纯文本兜底解析")
            return self._parse_text_response(text, code, name)

    def _parse_text_response(self, response_text: str, code: str, name: str) -> AnalysisResult:
        sentiment_score = 50
        trend = '震荡'
        advice = '持有'
        text_lower = response_text.lower()
        if sum(1 for kw in ['看多', '买入', '上涨', '突破', '强势'] if kw in text_lower) > sum(1 for kw in ['看空', '卖出', '下跌', '跌破', '弱势'] if kw in text_lower) + 1:
            sentiment_score, trend, advice, decision_type = 65, '看多', '买入', 'buy'
        elif sum(1 for kw in ['看空', '卖出', '下跌', '跌破', '弱势'] if kw in text_lower) > sum(1 for kw in ['看多', '买入', '上涨', '突破', '强势'] if kw in text_lower) + 1:
            sentiment_score, trend, advice, decision_type = 35, '看空', '卖出', 'sell'
        else:
            decision_type = 'hold'
            
        return AnalysisResult(
            code=code, name=name, sentiment_score=sentiment_score, 
            trend_prediction=trend, operation_advice=advice, 
            decision_type=decision_type, confidence_level='低', 
            analysis_summary=response_text[:500] if response_text else 'API已触发限流或出现异常，未能生成有效分析，建议观望。', 
            key_points='API调用受限，触发安全模式。', 
            risk_warning='建议查阅原始文本。', raw_response=response_text, success=True
        )

    def batch_analyze(self, contexts: List[Dict[str, Any]], delay_between: float = 2.0) -> List[AnalysisResult]:
        results = []
        for i, context in enumerate(contexts):
            if i > 0: time.sleep(delay_between)
            results.append(self.analyze(context))
        return results

def get_analyzer() -> GeminiAnalyzer:
    return GeminiAnalyzer()
