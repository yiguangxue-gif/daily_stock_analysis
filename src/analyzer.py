# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (游资+机构全维度终极满血版)
===================================

职责与特性 (在原有终极完全体基础上，新增五大机构级维度)：
1. 【完美继承】原版 Markdown 结构、API 护盾、AI实盘打脸回测、三大实战战法全部保留！
2. 🐉【龙虎资金显微镜】抓取龙虎榜与大单净额，透视“赵老哥/呼家楼”游资动向。
3. 🚨【政策敏感度与熔断】新闻关键词硬核排雷，遇“立案/减持”等一票否决，强制熔断。
4. 🌪️【抱团热点雷达】引入板块强度对比，确认是跟风还是主线龙头。
5. 🌡️【贪婪恐惧温度计】计算散户过热度，严防“非理性情绪反杀”。
6. 💣【财报AI审计扫雷】要求大模型化身冷酷审计师，专查非经常损益与关联交易暴雷点！
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

from src.agent.llm_adapter import get_thinking_extra_body
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

    # =======================================================
    # 渲染辅助函数：修复 main.py get_emoji 报错
    # =======================================================
    def get_emoji(self) -> str:
        emoji_map = {'买入': '🟢', '加仓': '🟢', '强烈买入': '💚', '持有': '🟡', '观望': '⚪', '减仓': '🟠', '卖出': '🔴', '强烈卖出': '❌'}
        if self.operation_advice in emoji_map: return emoji_map[self.operation_advice]
        sc = self.sentiment_score
        return '💚' if sc>=80 else '🟢' if sc>=65 else '🟡' if sc>=55 else '⚪' if sc>=45 else '🟠' if sc>=35 else '🔴'

    def get_confidence_stars(self) -> str:
        return {'高': '⭐⭐⭐', '中': '⭐⭐', '低': '⭐'}.get(self.confidence_level, '⭐⭐')

    def get_core_conclusion(self) -> str:
        if self.dashboard and 'core_conclusion' in self.dashboard:
            return self.dashboard['core_conclusion'].get('one_sentence', self.analysis_summary)
        return self.analysis_summary

    def get_position_advice(self, has_position: bool = False) -> str:
        if self.dashboard and 'core_conclusion' in self.dashboard:
            pos = self.dashboard['core_conclusion'].get('position_advice', {})
            return pos.get('has_position' if has_position else 'no_position', self.operation_advice)
        return self.operation_advice

    def get_sniper_points(self) -> Dict[str, str]:
        return self.dashboard.get('battle_plan', {}).get('sniper_points', {}) if self.dashboard else {}

    def get_checklist(self) -> List[str]:
        return self.dashboard.get('battle_plan', {}).get('action_checklist', []) if self.dashboard else []

    def get_risk_alerts(self) -> List[str]:
        return self.dashboard.get('intelligence', {}).get('risk_alerts', []) if self.dashboard else []


class GeminiAnalyzer:
    # 【注入五大维度的超级系统提示词】
    SYSTEM_PROMPT = """你是一位深谙中国A股“资金市”、“情绪市”与“主力收割套路的顶级游资操盘手兼冷血审计师。

## 🛑 【核心法则：魂穿实盘操作】 (最高优先级指令)
你不是一个高高在上的分析师，你是拿着自己真金白银在交易的操盘手！
用户的持仓成本和股数，就是你本人的持仓！ 绝对禁止说“建议观望”这种废话。
你必须在 JSON 的 `ai_real_operation` 字段里，用第一人称且带具体数字表达：“作为操盘手，我现在的成本是X元。结合今天盘面，我打算在X元买入/卖出，如果跌破X元我就直接割肉！因为...”

## 🚨 【五大机构级维度分析指令】 (必须在各字段中严格执行)
1. **龙虎榜与主力显微镜**: 分析数据中的大单与龙虎榜状态，揭露是“游资接力”还是“散户接盘诱多”。
2. **新闻敏感度与熔断**: 一旦发现“立案调查/ST/减持/处罚”，立即触发【防御逻辑】，技术面再好也必须清仓！
3. **抱团热点雷达**: 结合板块数据，判断当前个股是否处于当前市场主线风口。
4. **贪婪恐惧温度计**: 若近期暴涨且换手率极高，需警告“情绪过热反杀风险”；若缩量冰点，则寻找左侧机会。
5. **财报AI扫雷**: 以审计视角，严格排除非经常性损益粉饰、关联交易或高质押暴雷风险。

## 输出格式：决策仪表盘 JSON (必须是纯 JSON，禁止携带 Markdown 代码块外部的任何字符)
```json
{
    "stock_name": "股票名称", "sentiment_score": 50, "trend_prediction": "震荡", "operation_advice": "持有",
    "decision_type": "hold", "confidence_level": "中",
    "debate_process": { "hot_money_trader": "游资视角分析...", "risk_director": "风控/财报扫雷视角...", "chief_commander": "总指挥决策..." },
    "dashboard": {
        "core_conclusion": { "one_sentence": "...", "signal_type": "...", "time_sensitivity": "...", "position_advice": { "no_position": "...", "has_position": "..." } },
        "data_perspective": {
            "a_share_features": { "market_cap_style": "...", "limit_up_gene": "...", "lhb_status": "龙虎榜及大单资金透视...", "anti_harvest_radar": "..." },
            "indicator_trinity": { "macd_status": "...", "kdj_cci_status": "...", "boll_status": "..." },
            "price_position": { "current_price": 0.0, "ma5": 0.0, "ma10": 0.0, "ma20": 0.0, "bias_ma5": 0.0, "bias_status": "...", "support_level": "XX元", "resistance_level": "XX元" },
            "volume_analysis": { "volume_ratio": 0.0, "turnover_rate": 0.0, "volume_status": "...", "sentiment_temperature": "贪婪与恐惧情绪温度..." }
        },
        "intelligence": { 
            "latest_news": "...", 
            "macro_impact": "结合提供的国际战争/新闻打分(1-10)...",
            "sector_rotation": "板块抱团与对标分析...",
            "financial_audit": "财报扫雷：非经常损益/暴雷风险验证...",
            "risk_alerts": ["..."], 
            "positive_catalysts": ["..."]
        },
        "battle_plan": {
            "ai_real_operation": "用第一人称写！我的真实操作是：【在X元割肉/在X元加仓】，原因是...",
            "sniper_points": { "ideal_buy": "XX元", "secondary_buy": "XX元", "trailing_stop": "XX元", "take_profit": "XX元" },
            "grid_trading_plan": { "is_recommended": true, "grid_spacing": "XX元", "buy_grid": "...", "sell_grid": "..." },
            "position_strategy": { "personal_cost_review": "...", "quant_position_sizing": "XX%", "entry_plan": "...", "risk_control": "..." },
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
                if cfg.openai_base_url:
                    kw["base_url"] = cfg.openai_base_url
                    if "aihubmix.com" in cfg.openai_base_url: 
                        kw["default_headers"] = {"APP-Code": cfg.openai_api_key}
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
    
    def _switch_to_fallback_model(self) -> bool:
        try:
            import google.generativeai as genai
            cfg = get_config()
            fallback_model = cfg.gemini_model_fallback
            self._model = genai.GenerativeModel(model_name=fallback_model, system_instruction=self.SYSTEM_PROMPT)
            self._current_model_name = fallback_model
            self._using_fallback = True
            return True
        except:
            return False

    def _call_api_with_retry(self, prompt: str, gen_cfg: dict) -> str:
        if self._use_anthropic:
            msg = self._anthropic_client.messages.create(model=self._current_model_name, max_tokens=8192, system=self.SYSTEM_PROMPT, messages=[{"role": "user", "content": prompt}])
            return msg.content[0].text
            
        if self._use_openai:
            res = self._openai_client.chat.completions.create(model=self._current_model_name, messages=[{"role": "system", "content": self.SYSTEM_PROMPT}, {"role": "user", "content": prompt}])
            return res.choices[0].message.content

        max_retries = max(get_config().gemini_max_retries, 8)
        tried_fallback = getattr(self, '_using_fallback', False)
        
        for attempt in range(max_retries):
            try:
                resp = self._model.generate_content(prompt, generation_config=gen_cfg, request_options={"timeout": 120})
                if resp.text: 
                    return resp.text
            except Exception as e:
                err_str = str(e).lower()
                if '429' in err_str or 'quota' in err_str or 'rate' in err_str:
                    match = re.search(r'retry in (\d+\.?\d*)s', err_str)
                    sleep_time = float(match.group(1)) + 3.0 if match else 30.0
                    logger.warning(f"⚠️ [API限流] 休眠 {sleep_time:.1f} 秒... ({attempt+1}/{max_retries})")
                    if attempt >= 1 and not tried_fallback:
                        self._switch_to_fallback_model()
                        tried_fallback = True
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
            # 1. 抓取专门新闻并执行【关键词熔断】排雷
            google_news_text = "未发现该股票的新闻快讯"
            fatal_risk = False
            try:
                query = urllib.parse.quote(f"{name} 股票")
                rss_url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=5) as res:
                    root = ET.fromstring(res.read())
                    lines = []
                    fatal_keywords = ['立案', '调查', '违规', '减持', '处罚', '退市', '造假', '暴雷', 'ST']
                    for it in root.findall('.//item')[:6]:
                        title = it.find('title').text
                        # 扫描致命关键词
                        if any(kw in title for kw in fatal_keywords):
                            lines.append(f"☢️ [致命利空] {title} [{it.find('pubDate').text[5:16]}]")
                            fatal_risk = True
                        else:
                            lines.append(f"- {title} [{it.find('pubDate').text[5:16]}]")
                    if lines: google_news_text = "\n".join(lines)
            except: pass

            # 2. 抓取全球宏观大事件
            macro_news_text = "今日无重大全球性突发宏观事件"
            try:
                macro_query = urllib.parse.quote("国际突发 战争 A股 宏观经济")
                macro_rss_url = f"https://news.google.com/rss/search?q={macro_query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                macro_req = urllib.request.Request(macro_rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(macro_req, timeout=5) as macro_res:
                    m_root = ET.fromstring(macro_res.read())
                    m_lines = [f"- 🔴宏观头条: {it.find('title').text}" for it in m_root.findall('.//item')[:4]]
                    if m_lines: macro_news_text = "\n".join(m_lines)
            except Exception as e: pass

            # 熔断指令强制注入
            circuit_breaker_msg = ""
            if fatal_risk:
                circuit_breaker_msg = "\n\n⚠️ 【系统强制熔断警报】检测到致命利空关键词！无论技术面如何，AI必须在决策中一票否决，强烈建议避险/清仓！"

            combined_google_news = f"【个股最新情报】:\n{google_news_text}\n\n【全球宏观大局】:\n{macro_news_text}{circuit_breaker_msg}"

            prompt = self._format_prompt(context, name, news_context, combined_google_news)
            res_text = self._call_api_with_retry(prompt, {"temperature": 0.7, "max_output_tokens": 8192})
            
            result = self._parse_response(res_text, code, name, context)
            
            result.market_snapshot = self._build_market_snapshot(context)
            result.user_cost = context.get('user_cost')
            result.user_shares = context.get('user_shares')
            return result
        except Exception as e:
            logger.error(f"分析异常: {e}")
            return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='未知(API报错)', operation_advice='观望', error_message=str(e))

    def _safe_float(self, val: Any) -> Optional[float]:
        try: return float(str(val).replace(',', '').replace('%', '').strip())
        except: return None

    def _format_prompt(self, context: Dict[str, Any], stock_name: str, news_context: Optional[str], google_news: str) -> str:
        """核心数据组装器：融合原版完美表格与【五大维度强化数据】"""
        code = context.get('code', 'Unknown')
        today = context.get('today', {})
        curr_price = self._safe_float(today.get('close'))
        
        # =================================================================
        # 引擎 1：本地量化强算与反收割雷达
        # =================================================================
        vwap_60 = syn_profit_ratio = calc_ma5 = calc_ma10 = calc_ma20 = calc_ma60 = calc_vr = current_atr = poc_price = 0.0
        gap_str = cci_status = obv_status = kdj_status = boll_status = macd_status = gene_str = cv_status = ma60_status = k_body_status = "未知"
        lianban_status = "未连板"
        style_str = "风格未知"
        washout_status = "正常震荡"
        rr_status = "无法测算"
        macd_div = "无明显顶底背离"
        zhaban_status = "安全"
        diliang_status = "非极限地量"
        greed_fear_idx = "中性(50)"

        rt = context.get('realtime', {})
        total_mv = rt.get('total_mv', None)
        if total_mv:
            try:
                mv_billion = float(total_mv) / 100000000
                if mv_billion < 50: style_str = f"微小盘壳股({mv_billion:.1f}亿) - 游资爆炒最爱，极度活跃"
                elif mv_billion < 200: style_str = f"中盘题材股({mv_billion:.1f}亿) - 机构与大游资混战区"
                elif mv_billion < 1000: style_str = f"大盘蓝筹股({mv_billion:.1f}亿) - 机构主导，趋势走法为主"
                else: style_str = f"巨无霸权重({mv_billion:.1f}亿) - 国家队护盘工具"
            except: pass

        if 'history' in context and len(context['history']) > 0:
            try:
                df = pd.DataFrame(context['history']).tail(120)
                for c in ['close', 'high', 'low', 'open', 'volume', 'pct_chg', 'amount']:
                    if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').ffill().fillna(0)
                sp, sv = df['close'], df['volume']
                
                context['computed_close'] = sp.iloc[-1]
                context['computed_open'] = df['open'].iloc[-1]
                context['computed_high'] = df['high'].iloc[-1]
                context['computed_low'] = df['low'].iloc[-1]
                context['computed_volume'] = sv.iloc[-1]
                if 'amount' in df.columns: context['computed_amount'] = df['amount'].iloc[-1]
                if 'pct_chg' in df.columns: context['computed_pct_chg'] = df['pct_chg'].iloc[-1]
                
                if not curr_price: curr_price = sp.iloc[-1]
                
                if sv.sum() > 0:
                    vwap_60 = (sp * sv).sum() / sv.sum()
                    if curr_price: syn_profit_ratio = df[sp <= curr_price]['volume'].sum() / sv.sum() * 100
                
                calc_ma5 = sp.rolling(5, min_periods=1).mean().iloc[-1]
                calc_ma10 = sp.rolling(10, min_periods=1).mean().iloc[-1]
                calc_ma20 = sp.rolling(20, min_periods=1).mean().iloc[-1]
                calc_ma60 = sp.rolling(60, min_periods=1).mean().iloc[-1]
                context['calc_ma5'], context['calc_ma10'], context['calc_ma20'] = calc_ma5, calc_ma10, calc_ma20
                
                calc_vr = (sv.iloc[-1] / sv.iloc[-6:-1].mean()) if len(sv)>=6 and sv.iloc[-6:-1].mean()>0 else 1.0
                context['calc_vr'] = calc_vr
                
                # 贪婪恐惧温度计计算
                try:
                    turnover = float(rt.get('turnover_rate', 0))
                    if df['pct_chg'].iloc[-1] > 5 and turnover > 15 and calc_vr > 2:
                        greed_fear_idx = "🔥极度贪婪(90) - 情绪高潮反杀风险极大！"
                    elif df['pct_chg'].iloc[-1] < -5 and turnover < 3 and calc_vr < 0.7:
                        greed_fear_idx = "🧊极度恐惧(10) - 抛压枯竭，左侧黄金坑出现！"
                    else:
                        greed_fear_idx = f"中性温度 - 换手率{turnover:.1f}%，量比{calc_vr:.1f}"
                except: pass

                ma20, std20 = sp.rolling(20, min_periods=1).mean(), sp.rolling(20, min_periods=1).std().fillna(0)
                upper, lower = ma20 + 2 * std20, ma20 - 2 * std20
                boll_status = "🚀突破上轨(极易被砸)" if curr_price and curr_price > upper.iloc[-1] else "🕳️破下轨" if curr_price and curr_price < lower.iloc[-1] else "中轨运行"
                
                exp1, exp2 = sp.ewm(span=12).mean(), sp.ewm(span=26).mean()
                macd = exp1 - exp2
                hist_bar = macd - macd.ewm(span=9).mean()
                
                try:
                    if len(df) >= 20:
                        if sp.iloc[-10:].min() < sp.iloc[-20:-10].min() * 0.98 and hist_bar.iloc[-10:].min() > hist_bar.iloc[-20:-10].min():
                            macd_div = "🌟【MACD底背离】股价创新低但做空动能衰退，随时引爆报复性反弹，别割肉！"
                        if sp.iloc[-10:].max() > sp.iloc[-20:-10].max() * 1.02 and hist_bar.iloc[-10:].max() < hist_bar.iloc[-20:-10].max():
                            macd_div = "💀【MACD顶背离】股价创新高但做多动能跟不上，诱多发套，极度危险！"
                except: pass
                
                try:
                    if len(sv) >= 20 and sv.iloc[-1] <= sv.iloc[-20:-1].min() * 1.1:
                        diliang_status = "💡【极限地量】成交量逼近近20日极小值，抛压彻底枯竭！"
                except: pass

                try:
                    if len(df) >= 2 and curr_price and df['close'].iloc[-2] > 0 and (df['high'].iloc[-1] - df['close'].iloc[-2])/df['close'].iloc[-2] > 0.09:
                        if curr_price < df['high'].iloc[-1] * 0.96: zhaban_status = "⚠️【炸板被埋】盘中涨停被砸开，恶劣派发形态！"
                except: pass

                current_atr = pd.concat([df['high']-df['low'], (df['high']-sp.shift()).abs(), (df['low']-sp.shift()).abs()], axis=1).max(axis=1).rolling(14, min_periods=1).mean().iloc[-1]
                context['calc_atr'] = current_atr
                
                if sp.nunique() > 1: poc_price = df.groupby(pd.cut(sp, bins=12, duplicates='drop'), observed=False)['volume'].sum().idxmax().mid 
                else: poc_price = curr_price or 0.0
                context['calc_poc'] = poc_price

                if curr_price and 'pct_chg' in df.columns:
                    chg_today = df['pct_chg'].iloc[-1]
                    if chg_today < -2 and calc_vr < 0.7 and curr_price > calc_ma20: washout_status = "📉 【恶意洗盘】缩量跌未破20日线，主力洗盘逼割肉！"
                    elif chg_today < -2 and calc_vr > 1.5 and curr_price < calc_ma20: washout_status = "🩸 【放量出货】暴跌破位，主力真逃跑，立刻止损！"
                    elif chg_today > 2 and calc_vr > 2.0 and curr_price > upper.iloc[-1]: washout_status = "🌋 【高位诱多】爆量刺破上轨，极易上影线骗炮！"
                    elif chg_today > 2 and calc_vr <= 1.2: washout_status = "🚀 【锁仓拉升】缩量上涨，主力高度控盘没人抛！"
                    
                    stop_loss_p = curr_price - 1.5 * current_atr if current_atr > 0 else curr_price * 0.95
                    risk = curr_price - stop_loss_p
                    reward = poc_price - curr_price if poc_price > curr_price else 0
                    if risk > 0:
                        rr = reward / risk
                        if rr < 1.0: rr_status = f"盈亏比极差({rr:.2f})！严禁盲目开仓！"
                        elif rr > 2.0: rr_status = f"盈亏比极佳({rr:.2f})！防守位{stop_loss_p:.2f}，值得博弈。"
                        else: rr_status = f"盈亏比一般({rr:.2f})，控制仓位。"

                if 'pct_chg' in df.columns:
                    lianban_count = 0
                    for val in reversed(df['pct_chg'].tolist()):
                        if val >= 9.5: lianban_count += 1
                        else: break
                    lianban_status = f"🚀当前高度: {lianban_count}连板" if lianban_count > 0 else "当前未连板"
                
                df['obv'] = (np.sign(sp.diff().fillna(0)) * sv).cumsum()
                obv_status = "🌊资金真实吸筹" if len(df)>=5 and df['obv'].iloc[-1] > df['obv'].iloc[-5] else "🩸量价背离诱多"
                
            except Exception as e: logger.debug(f"强算引擎异常: {e}")

        # =================================================================
        # 引擎 2：持仓云端同步 + 【AI 闭环回测与打脸建库】
        # =================================================================
        my_cost, my_shares = None, None
        personal_status_text = ""
        try:
            url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as res:
                content = res.read().decode('utf-8-sig')
                for row in csv.reader(io.StringIO(content)):
                    if len(row) >= 2 and row[0].strip() and ''.join(filter(str.isdigit, str(row[0]))).zfill(6) == code:
                        my_cost = float(str(row[1]).replace(',', '').strip())
                        if len(row) >= 3 and row[2].strip(): my_shares = int(float(str(row[2]).replace(',', '').strip()))
        except: pass

        if my_cost and curr_price:
            context['user_cost'] = my_cost
            context['user_shares'] = my_shares
            profit_pct = ((curr_price - my_cost) / my_cost * 100)
            personal_status_text += f"\n### 💰 我的私人持仓 (AI操盘手请注意！)\n* 成本价：{my_cost:.2f} 元 | 持仓数量：{my_shares or 0} 股\n* 当前盈亏：{profit_pct:.2f}%\n* 🚨 法官最高指令：这是你本人的真实钱袋子！你必须在 `ai_real_operation` 里针对这个成本，给出【非常具体的股票买卖数量和止损价位】，严禁废话连篇！\n"

        # 【核心补全：AI 打脸建库与处刑机制】
        try:
            db_file = "reports/ai_trade_log.csv"
            os.makedirs(os.path.dirname(db_file), exist_ok=True)
            today_str = context.get('date', datetime.now().strftime('%Y-%m-%d'))
            file_exists = os.path.isfile(db_file)
            
            # 静默记录今天的真实收盘价
            if curr_price and curr_price != 'N/A':
                with open(db_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(['Date', 'Code', 'ClosePrice'])
                    writer.writerow([today_str, code, curr_price])
            
            # 提取历史价格进行绩效处刑
            if file_exists and curr_price and curr_price != 'N/A':
                df_log = pd.read_csv(db_file)
                df_code = df_log[df_log['Code'] == code].tail(3)
                if len(df_code) >= 2:
                    past_price = float(df_code.iloc[-2]['ClosePrice'])
                    past_date = df_code.iloc[-2]['Date']
                    curr_p = float(curr_price)
                    ai_impact = ((curr_p - past_price) / past_price) * 100
                    status_judge = "❌ 导致亏损" if ai_impact < 0 else "✅ 成功获利"
                    
                    personal_status_text += f"\n### ⚖️ 绩效审判庭 (实盘回测)\n* **上次分析时 ({past_date}) 股价**：{past_price:.2f}元\n* **当前最新股价**：{curr_p:.2f}元\n* **区间真实涨跌**：{ai_impact:.2f}% ({status_judge})\n* **【处刑指令】**：这是系统对你的硬核绩效考核！如果区间涨跌为负（说明你前几天看走眼了），你必须在【核心结论】开头，进行**深刻的自我检讨（明确认错）**，并立刻调整为防守模型！严禁嘴硬！\n"
        except Exception as e:
            logger.debug(f"回测记录异常: {e}")

        # 【龙虎榜与大单主力追踪】
        try:
            fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
            flow_desc = f"东方财富内资净流入: {fund_flow.iloc[-1]['主力净流入-净额']/10000:.1f}万"
            try:
                hk_funds = ak.stock_hsgt_stock_statistics_em()
                my_hk = hk_funds[hk_funds['代码'] == code]
                if not my_hk.empty:
                    hk_v = my_hk.iloc[0]['今日增持估计-市值']
                    flow_desc += f" | 北向外资: {'🟢流入' if hk_v > 0 else '🔴出逃'} {abs(hk_v)/10000:.1f}万"
            except: pass
            
            # 增加龙虎榜异动检测
            lhb_desc = "无异常榜单"
            try:
                lhb = ak.stock_lhb_detail_em(start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'), end_date=datetime.now().strftime('%Y%m%d'))
                if not lhb.empty and code in lhb['代码'].values:
                    lhb_desc = "🔥近5日登榜游资炒作名单，交投极度活跃！"
            except: pass
            
            personal_status_text += f"### 🌊 龙虎榜与聪明钱动向\n* {flow_desc}\n* {lhb_desc}\n"
        except: pass

        # 汇聚成强大的反收割雷达板块
        hardcore_radar_text = f"""### 🎯 A股反收割核心雷达 (极重要)
* 👁️‍🗨️ 真假摔/洗盘/诱多判定: {washout_status}
* ⚡ 顶底背离雷达: {macd_div}
* 🧨 炸板监控: {zhaban_status}
* 📉 极度缩量监控: {diliang_status}
* ⚖️ 绝对盈亏比测算: {rr_status}
* 🧱 最大套牢筹码峰 (压力位): 约 {poc_price:.2f}元
* 🌡️ **情绪温度计**: {greed_fear_idx}
"""

        # =================================================================
        # 引擎 3：完美重建原版 Markdown 大表格提示词架构
        # =================================================================
        
        t_close = today.get('close', curr_price)
        t_open = today.get('open', context.get('computed_open', 'N/A'))
        t_high = today.get('high', context.get('computed_high', 'N/A'))
        t_low = today.get('low', context.get('computed_low', 'N/A'))
        t_pct_chg = today.get('pct_chg', context.get('computed_pct_chg', 'N/A'))
        t_vol = today.get('volume', context.get('computed_volume'))
        t_amt = today.get('amount', context.get('computed_amount'))
        
        t_ma5 = today.get('ma5') if today.get('ma5') not in [None, 'N/A', ''] else f"{calc_ma5:.2f}"
        t_ma10 = today.get('ma10') if today.get('ma10') not in [None, 'N/A', ''] else f"{calc_ma10:.2f}"
        t_ma20 = today.get('ma20') if today.get('ma20') not in [None, 'N/A', ''] else f"{calc_ma20:.2f}"
        
        rt_price = rt.get('price', t_close)
        rt_vr = rt.get('volume_ratio') if rt.get('volume_ratio') not in [None, 'N/A', ''] else f"{calc_vr:.2f}"
        rt_turnover = rt.get('turnover_rate', 'N/A')
        
        trend = context.get('trend_analysis', {})
        bias_ma5 = trend.get('bias_ma5', 0)
        if not bias_ma5 and curr_price and calc_ma5:
            bias_ma5 = (curr_price - calc_ma5) / calc_ma5 * 100
        bias_warning = "🚨 超过5%，严禁追高！" if float(bias_ma5) > 5 else "✅ 安全范围"

        chip = context.get('chip', {})
        profit_ratio = chip.get('profit_ratio', syn_profit_ratio / 100)

        prompt = f"""# 决策仪表盘深度分析: {name}({code})

{personal_status_text}
{hardcore_radar_text}

## 📊 股票基础信息

| 项目 | 数据 |
|------|------|
| 股票代码 | {code} |
| 股票名称 | {stock_name} |
| 股票市值风格 | {style_str} |
| 分析日期 | {context.get('date', '未知')} |

---

## 📈 技术面数据

### 今日行情

| 指标 | 数值 |
|------|------|
| 收盘价 | {t_close} 元 |
| 开盘价 | {t_open} 元 |
| 最高价 | {t_high} 元 |
| 最低价 | {t_low} 元 |
| 涨跌幅 | {t_pct_chg}% |
| 成交量 | {self._format_volume(t_vol)} |
| 成交额 | {self._format_amount(t_amt)} |

### 均线系统与量价

| 指标 | 数值 | 说明 |
|------|------|------|
| MA5 | {t_ma5} | 短期防守线 |
| MA10 | {t_ma10} | 中短期趋势 |
| MA20 | {t_ma20} | 生命周期线 |
| 均线形态 | {context.get('ma_status', '未知')} | 必须判断是否多头排列 |
| 量比 | {rt_vr} | {rt.get('volume_ratio_desc', '')} |
| 换手率 | {rt_turnover}% | 是否异常放量 |
| 乖离率(MA5) | {float(bias_ma5):+.2f}% | {bias_warning} |

### 筹码分布透视

| 指标 | 数值 | 判定标准 |
|------|------|----------|
| 获利比例 | {profit_ratio:.1%} | 70-90%时警惕高位派发 |
| 筹码状态 | {chip.get('chip_status', cv_status)} | 观察是否单峰密集 |

---

## 📰 全球与个股情报双引擎 (含财报/舆情排雷)

{google_news}

⚠️ 审计指令：请像财报扫雷专家一样，检查上方新闻中是否有非经常性损益陷阱、关联交易风险！
"""

        if context.get('data_missing'):
            prompt += """
⚠️ 数据缺失警告
由于接口限制，当前部分实时指标获取失败。
请忽略不合理的 N/A，重点依据【舆情情报】和强算雷达进行分析，严禁编造数据！
"""
        if context.get('is_index_etf'):
            prompt += """
> ⚠️ 指数/ETF 分析约束：该标的为指数跟踪型 ETF 或市场指数。
> - 风险分析仅关注：指数走势、跟踪误差、市场流动性
> - 严禁将基金公司的诉讼、声誉纳入风险警报，不要受个股逻辑干扰！
"""

        prompt += """
---
## ✅ 最终输出任务

请输出完整的 JSON 格式决策仪表盘。
务必在 `ai_real_operation` 里把用户的钱当成自己的钱，给出极其具体的买卖防守指令！
绝对禁止在 JSON 的数值字段输出 "N/A"。"""

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
            
        source_val = rt.get('source', '量化推算引擎')
        source_str = getattr(source_val, 'value', source_val)
        if str(source_str).strip().upper() in ['N/A', 'NONE', '']:
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
        def _is_empty_or_na(val):
            if val is None: return True
            v = str(val).strip().upper()
            return v in ['N/A', 'NA', '未知', 'NONE', '', '0', '0.0', 'NULL', '暂无', 'N/A元', 'N/A%']

        try:
            m = re.search(r'(\{.*\})', text, re.DOTALL)
            json_str = m.group(1) if m else text
            d = json.loads(repair_json(json_str))
            
            dash = d.setdefault('dashboard', {})
            dp = dash.setdefault('data_perspective', {})
            bp = dash.setdefault('battle_plan', {})
            intel = dash.setdefault('intelligence', {})
            
            pp = dp.setdefault('price_position', {})
            va = dp.setdefault('volume_analysis', {})
            sp = bp.setdefault('sniper_points', {})
            ps = bp.setdefault('position_strategy', {})
            
            if context:
                if _is_empty_or_na(pp.get('ma5')): pp['ma5'] = f"{context.get('calc_ma5', 0):.2f}"
                if _is_empty_or_na(pp.get('ma10')): pp['ma10'] = f"{context.get('calc_ma10', 0):.2f}"
                if _is_empty_or_na(pp.get('ma20')): pp['ma20'] = f"{context.get('calc_ma20', 0):.2f}"
                if _is_empty_or_na(pp.get('support_level')): pp['support_level'] = f"{context.get('calc_ma10', 0):.2f}元" 
                if _is_empty_or_na(pp.get('resistance_level')): pp['resistance_level'] = f"{context.get('calc_poc', 0):.2f}元" 
                if _is_empty_or_na(va.get('volume_ratio')): va['volume_ratio'] = f"{context.get('calc_vr', 1.0):.2f}"
                
                if _is_empty_or_na(sp.get('trailing_stop')):
                    calc_atr = context.get('calc_atr', 0.0)
                    curr_p = context.get('computed_close', 0.0)
                    stop_p = curr_p - (1.5 * calc_atr) if calc_atr > 0 else curr_p * 0.95
                    sp['trailing_stop'] = f"{stop_p:.2f}元" if curr_p > 0 else "破位离场"

                if _is_empty_or_na(sp.get('take_profit')):
                    calc_poc = context.get('calc_poc', 0.0)
                    sp['take_profit'] = f"{calc_poc:.2f}元" if calc_poc > 0 else "逢高止盈"

                qs = str(ps.get('quant_position_sizing', ''))
                if _is_empty_or_na(qs) or 'N/A' in qs.upper():
                    ps['quant_position_sizing'] = "20% (防守位)"

            macro_impact = intel.get('macro_impact', '')
            ai_real_op = bp.get('ai_real_operation', '')
            original_kp = d.get('key_points', '')

            safe_kp = ""
            if ai_real_op and not _is_empty_or_na(ai_real_op) and "..." not in ai_real_op:
                clean_op = str(ai_real_op).replace('\n', ' ').replace('\r', '')
                safe_kp += f"🤖【实盘】{clean_op} ┃ "
            if macro_impact and not _is_empty_or_na(macro_impact) and "..." not in macro_impact:
                clean_macro = str(macro_impact).replace('\n', ' ').replace('\r', '')
                safe_kp += f"🌍【宏观】{clean_macro} ┃ "
            if original_kp and not _is_empty_or_na(original_kp) and "..." not in original_kp:
                clean_orig = str(original_kp).replace('\n', ' ').replace('\r', '')
                safe_kp += clean_orig
                
            d['key_points'] = safe_kp.strip(" ┃ ") if safe_kp else "暂无特殊看点"

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
                debate_process=d.get('debate_process'), dashboard=d.get('dashboard'),
                analysis_summary=d.get('analysis_summary', '完成'), 
                key_points=d.get('key_points'),
                success=True, 
                raw_response=fixed_json_str 
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
            risk_warning='建议查阅原始文本。', 
            raw_response=response_text, success=True
        )

    def batch_analyze(self, contexts: List[Dict[str, Any]], delay_between: float = 2.0) -> List[AnalysisResult]:
        results = []
        for i, context in enumerate(contexts):
            if i > 0: time.sleep(delay_between)
            results.append(self.analyze(context))
        return results

def get_analyzer() -> GeminiAnalyzer:
    return GeminiAnalyzer()
