# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (多 Agent 虚拟协同 + 宏观流动性 + 凯利风控 终极强渲染版)
===================================

核心进化：
1. 🐉【庄家筹码与游资跟踪】：Agent B 专项识别龙虎榜关联席位及筹码密集套牢区。
2. 📜【政策与小作文因果链】：实时交叉验证新闻，区分“真政策”与“假传闻”。
3. 💣【A股特色排雷系统】：Agent C 强制审查“存贷双高”、质押平仓线、解禁期。
4. 🌡️【情绪周期与资金流向】：结合换手与连板高度，界定“冰点-一致-高潮-崩塌”周期。
5. 🧠【多空对抗与认知诊断】：多头与空头思维互搏，打脸回测强制归因（技术/政策/情绪/资金），生成避坑黑名单！
6. 💸【宏观流动性与杠杆】：跟踪两融/汇率逻辑，预防外资抛压与杠杆踩踏。
7. ⚖️【智能仓位与条件单】：基于近期真实胜率应用凯利公式（Kelly Criterion），输出防梭哈仓位及具体的自动化网格/条件单脚本！
8. 🛡️【强渲染补丁】：将复杂嵌套的 JSON 字段强行拍扁，硬塞入 key_points，确保老旧邮件模板也能完美展示所有高级功能！
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
    # 渲染辅助函数
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
    SYSTEM_PROMPT = """你不再是一个单一的分析师。你现在是一个由三位顶尖专家组成的【A股游资量化私募决策委员会】的“总指挥”。

### 🤖 你的内部 Agent 运作与对抗机制：
在做出最终结论前，你必须在内部模拟多空专家的激烈对抗，并将结论写入 `debate_process` 中：
- **[Agent A - 多头爆破手]**：死盯 MA/MACD/放量突破。只看多，疯狂寻找情绪共振点与游资主升浪的借口。
- **[Agent B - 空头狙击手]**：只看空，死盯上方的筹码套牢密集区，寻找诱多出货、财务造假、存贷双高的漏洞，随时准备砸盘。
- **[Agent C - 认知诊断官]**：根据系统提供的“昨日回测打脸记录”，执行【归因分析】。如果上一次看错导致亏损，必须诊断出是（技术/政策/情绪/资金）哪方面出了问题，并强制要求本次分析避开该坑！

## 🛑 【核心法则：魂穿实盘与凯利公式】
用户的持仓成本和股数，就是你总指挥本人的真金白银！
1. **真实交易操作**：必须在 `ai_real_operation` 给出具体的买卖股数、价格和止损位，绝不打太极！
2. **凯利动态仓位**：系统会给你提供最近的历史胜率和盈亏比，你必须在 `kelly_position_sizing` 字段利用凯利公式计算出该次下注的**具体百分比仓位**，严禁盲目梭哈！
3. **自动化条件单脚本**：在 `conditional_order_script` 中，生成一段可供量化软件执行的自然语言脚本。

## 🌍 【宏观流动性与避雷法则】
跟踪汇率波动对北向资金的影响，警惕融资融券规模暴增导致的“杠杆爆仓风险”。遇到“减持、立案、平仓、解禁”一票否决清仓！

## 输出格式：决策仪表盘 JSON (必须是纯 JSON)
```json
{
    "stock_name": "股票名称", "sentiment_score": 50, "trend_prediction": "震荡", "operation_advice": "持有",
    "decision_type": "hold", "confidence_level": "中",
    "debate_process": { 
        "bull_agent": "多头爆发手意见...", 
        "bear_agent": "空头狙击手意见...", 
        "cognitive_bias_diagnosis": "打脸归因：若前次失败，必须写明是技术/政策/资金/情绪的哪一项误判，形成避坑黑名单..." 
    },
    "dashboard": {
        "core_conclusion": { "one_sentence": "...", "signal_type": "...", "time_sensitivity": "...", "position_advice": { "no_position": "...", "has_position": "..." } },
        "data_perspective": {
            "a_share_features": { "market_cap_style": "...", "limit_up_gene": "...", "lhb_status": "龙虎榜及游资合力透视...", "anti_harvest_radar": "..." },
            "indicator_trinity": { "macd_status": "...", "kdj_cci_status": "...", "boll_status": "..." },
            "price_position": { "current_price": 0.0, "ma5": 0.0, "ma10": 0.0, "ma20": 0.0, "bias_ma5": 0.0, "bias_status": "...", "support_level": "XX元", "resistance_level": "XX元" },
            "volume_analysis": { "volume_ratio": 0.0, "turnover_rate": 0.0, "volume_status": "...", "sentiment_temperature": "贪婪恐惧/情绪周期温度..." }
        },
        "intelligence": { 
            "latest_news": "真政策与小作文验证...", 
            "macro_impact": "结合提供的国际战争/新闻打分(1-10)...",
            "sector_rotation": "抱团热点雷达与轮动对标...",
            "macro_liquidity": "两融杠杆变动与汇率/国债宏观关联分析...",
            "financial_audit": "财报扫雷：存贷双高/质押解禁等...",
            "risk_alerts": ["..."], 
            "positive_catalysts": ["..."]
        },
        "battle_plan": {
            "ai_real_operation": "用第一人称写！我的真实操作是：【在X元割肉/在X元加仓】...",
            "conditional_order_script": "若10:30前不破MA5且放量超30%，执行XX仓位挂单，触发价XX元...",
            "sniper_points": { "ideal_buy": "XX元", "secondary_buy": "XX元", "trailing_stop": "XX元", "take_profit": "XX元" },
            "grid_trading_plan": { "is_recommended": true, "grid_spacing": "XX元", "buy_grid": "...", "sell_grid": "..." },
            "position_strategy": { "personal_cost_review": "...", "kelly_position_sizing": "基于凯利公式计算的具体仓位XX%...", "entry_plan": "...", "risk_control": "..." },
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
            # 1. 抓取专门新闻并执行排雷
            google_news_text = "未发现该股票的新闻快讯"
            fatal_risk = False
            try:
                query = urllib.parse.quote(f"{name} 股票 质押 解禁 违规 存贷双高")
                rss_url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=5) as res:
                    root = ET.fromstring(res.read())
                    lines = []
                    fatal_keywords = ['立案', '调查', '违规', '减持', '处罚', '退市', '造假', '暴雷', 'ST', '平仓', '存贷双高', '商誉减值', '解禁']
                    for it in root.findall('.//item')[:8]:
                        title = it.find('title').text
                        if any(kw in title for kw in fatal_keywords):
                            lines.append(f"☢️ [风控官警报: 致命利空/解禁减持] {title} [{it.find('pubDate').text[5:16]}]")
                            fatal_risk = True
                        else:
                            lines.append(f"- {title} [{it.find('pubDate').text[5:16]}]")
                    if lines: google_news_text = "\n".join(lines)
            except: pass

            # 2. 抓取全球宏观大事件
            macro_news_text = "今日无重大全球性突发宏观事件"
            try:
                macro_query = urllib.parse.quote("国际突发 战争 A股 宏观经济 降息")
                macro_rss_url = f"https://news.google.com/rss/search?q={macro_query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                macro_req = urllib.request.Request(macro_rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(macro_req, timeout=5) as macro_res:
                    m_root = ET.fromstring(macro_res.read())
                    m_lines = [f"- 🔴宏观头条: {it.find('title').text}" for it in m_root.findall('.//item')[:4]]
                    if m_lines: macro_news_text = "\n".join(m_lines)
            except: pass

            circuit_breaker_msg = "\n\n⚠️ 【总指挥强制指令】风控官已探明致命利空！一票否决任何技术面看多逻辑，强烈建议避险清仓！" if fatal_risk else ""
            combined_google_news = f"【情报官与风控官情报池】:\n{google_news_text}\n\n【全球宏观大局】:\n{macro_news_text}{circuit_breaker_msg}"

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
        code = context.get('code', 'Unknown')
        today = context.get('today', {})
        curr_price = self._safe_float(today.get('close'))
        
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
        emotion_cycle = "混沌震荡期"

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
                
                try:
                    turnover = float(rt.get('turnover_rate', 0))
                    zt_mask = df['pct_chg'] > 9.5
                    zt_count = 0
                    for val in reversed(zt_mask.tolist()):
                        if val: zt_count += 1
                        else: break
                    
                    if zt_count >= 3:
                        emotion_cycle = "🔥 情绪高潮主升期 (留意随时分歧反杀)"
                    elif df['pct_chg'].iloc[-1] < -5 and zt_count == 0 and turnover > 15:
                        emotion_cycle = "💀 情绪崩塌退潮期 (高位派发，严禁接刀)"
                    elif calc_vr < 0.7 and abs(df['pct_chg'].iloc[-1]) < 2:
                        emotion_cycle = "🧊 情绪冰点期 (抛压枯竭，寻找左侧试错点)"

                    if df['pct_chg'].iloc[-1] > 5 and turnover > 20 and calc_vr > 2:
                        greed_fear_idx = "🔥极度贪婪(90) - 筹码极其松动，散户接盘预警！"
                    elif df['pct_chg'].iloc[-1] < -5 and turnover < 3 and calc_vr < 0.7:
                        greed_fear_idx = "🧊极度恐惧(10) - 恐慌盘已出尽，存在被错杀可能。"
                    else:
                        greed_fear_idx = f"中性温度(50) - 换手率{turnover:.1f}%"
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

        my_cost, my_shares = None, None
        personal_status_text = ""
        system_win_rate = 50.0
        system_odds = 1.0
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
            personal_status_text += f"\n### 💰 我的私人持仓 (总指挥底牌！)\n* 成本价：{my_cost:.2f} 元 | 持仓数量：{my_shares or 0} 股\n* 当前盈亏：{profit_pct:.2f}%\n* 🚨 总指挥执行指令：这是你自己真金白银的持仓！必须在 `ai_real_operation` 里针对这个成本，给出具体的股票买卖数量和防守割肉止损价！\n"

        try:
            db_file = "reports/ai_trade_log.csv"
            os.makedirs(os.path.dirname(db_file), exist_ok=True)
            today_str = context.get('date', datetime.now().strftime('%Y-%m-%d'))
            file_exists = os.path.isfile(db_file)
            
            if curr_price and curr_price != 'N/A':
                with open(db_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(['Date', 'Code', 'ClosePrice'])
                    writer.writerow([today_str, code, curr_price])
            
            if file_exists:
                df_log = pd.read_csv(db_file)
                if len(df_log) >= 2:
                    df_log['Return'] = df_log.groupby('Code')['ClosePrice'].pct_change() * 100
                    valid_returns = df_log['Return'].dropna()
                    if not valid_returns.empty:
                        wins = valid_returns[valid_returns > 0]
                        losses = valid_returns[valid_returns <= 0]
                        system_win_rate = (len(wins) / len(valid_returns)) * 100
                        avg_win = wins.mean() if not wins.empty else 2.0
                        avg_loss = abs(losses.mean()) if not losses.empty else 2.0
                        system_odds = avg_win / avg_loss if avg_loss > 0 else 1.0

                df_code = df_log[df_log['Code'] == code].tail(3)
                if len(df_code) >= 2 and curr_price and curr_price != 'N/A':
                    past_price = float(df_code.iloc[-2]['ClosePrice'])
                    past_date = df_code.iloc[-2]['Date']
                    curr_p = float(curr_price)
                    ai_impact = ((curr_p - past_price) / past_price) * 100
                    status_judge = "❌ 导致亏损" if ai_impact < 0 else "✅ 成功获利"
                    
                    personal_status_text += f"\n### ⚖️ 绩效审判庭与凯利风控 (实盘回测)\n* **上次分析时 ({past_date}) 股价**：{past_price:.2f}元\n* **当前最新股价**：{curr_p:.2f}元\n* **区间真实涨跌**：{ai_impact:.2f}% ({status_judge})\n"
                    personal_status_text += f"* **📈 系统全局胜率**: {system_win_rate:.1f}% | **平均盈亏比**: {system_odds:.2f}\n"
                    personal_status_text += f"* **【认知诊断与凯利指令】**：\n  1. 如果区间涨跌为负，Agent C 必须在 debate_process 的 `cognitive_bias_diagnosis` 中进行**归因诊断（指出是技术/政策/资金误判）**！\n  2. 总指挥必须根据上述系统胜率和盈亏比，利用凯利公式动态计算本次建议的投入仓位比例，填入 `kelly_position_sizing`！\n"
        except: pass

        usd_cny_desc = "宏观汇率暂稳"
        try:
            fx_df = ak.fx_spot_quote()
            usd_cny = float(fx_df[fx_df['货币对'] == '美元兑人民币']['最新价'].iloc[0])
            usd_cny_desc = f"美元兑人民币: {usd_cny:.4f} (若破7.25警惕外资流出白马股)"
        except: pass

        try:
            fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
            flow_desc = f"内资主力净流入: {fund_flow.iloc[-1]['主力净流入-净额']/10000:.1f}万"
            try:
                hk_funds = ak.stock_hsgt_stock_statistics_em()
                my_hk = hk_funds[hk_funds['代码'] == code]
                if not my_hk.empty:
                    hk_v = my_hk.iloc[0]['今日增持估计-市值']
                    flow_desc += f" | 北向外资: {'🟢流入' if hk_v > 0 else '🔴出逃'} {abs(hk_v)/10000:.1f}万"
            except: pass
            
            lhb_desc = "无异常榜单"
            try:
                lhb = ak.stock_lhb_detail_em(start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'), end_date=datetime.now().strftime('%Y%m%d'))
                if not lhb.empty and code in lhb['代码'].values:
                    lhb_desc = "🔥近5日登榜！发现顶级游资席位运作痕迹，交投极度活跃！"
            except: pass
            
            personal_status_text += f"### 🕵️‍♂️ Agent B 专属雷达：宏观流动性与龙虎榜动向\n* **{usd_cny_desc}**\n* {flow_desc}\n* {lhb_desc}\n"
        except: pass

        hardcore_radar_text = f"""### 🎯 Agent A 专属雷达：A股反收割技术面 (极重要)
* 👁️‍🗨️ 真假摔/洗盘/诱多判定: {washout_status}
* ⚡ 顶底背离雷达: {macd_div}
* 🧨 炸板监控: {zhaban_status}
* 📉 极度缩量监控: {diliang_status}
* ⚖️ 绝对盈亏比测算: {rr_status}
* 🧱 最大套牢筹码峰 (压力位): 约 {poc_price:.2f}元
* 🌊 **当前所处情绪周期**: {emotion_cycle}
* 🌡️ **情绪温度计**: {greed_fear_idx}
"""

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

        prompt = f"""# 决策仪表盘深度多空对抗分析: {stock_name}({code})

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

## 📈 技术面数据 (多空博弈基础)

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

## 📰 全球与个股情报双引擎 (Agent B & C 研判基础)

{google_news}

⚠️ 审计与舆情指令：
1. Agent B 请区分上述信息是“确切政策”还是纯粹诱多的“小作文/传闻”。
2. Agent C 请排查是否有“商誉减值、解禁、高质押平仓、存贷双高”的暴雷风险！
"""

        if context.get('data_missing'):
            prompt += """
⚠️ 数据缺失警告
由于接口限制，部分指标获取失败。
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
务必生成具体的【条件单交易脚本】及基于历史胜率的【凯利仓位测算】！绝对禁止在 JSON 的数值字段输出 "N/A"。"""

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
            debate = d.setdefault('debate_process', {})
            
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

            # =======================================================
            # 🛡️ 终极强渲染补丁：将所有深层嵌套的牛逼逻辑，强行拍扁，
            # 并入老旧模板必定会渲染的 key_points 字段中！
            # =======================================================
            ai_real_op = bp.get('ai_real_operation', '')
            kelly_pos = ps.get('kelly_position_sizing', '')
            cond_script = bp.get('conditional_order_script', '')
            cog_bias = debate.get('cognitive_bias_diagnosis', '')
            bull_agent = debate.get('bull_agent', '')
            bear_agent = debate.get('bear_agent', '')
            fin_audit = intel.get('financial_audit', '')
            macro_impact = intel.get('macro_impact', '')
            
            # 使用换行符和加粗标签，让邮件里的 key_points 变成一个极具战斗力的面板
            flattened_points = ""
            
            if not _is_empty_or_na(ai_real_op): flattened_points += f"<br>🤖 **【总指挥实盘】**：{ai_real_op}"
            if not _is_empty_or_na(kelly_pos): flattened_points += f"<br>⚖️ **【凯利仓位】**：{kelly_pos}"
            if not _is_empty_or_na(cond_script): flattened_points += f"<br>⚡ **【自动化挂单】**：{cond_script}"
            if not _is_empty_or_na(cog_bias): flattened_points += f"<br>🧠 **【归因诊断】**：{cog_bias}"
            if not _is_empty_or_na(bull_agent): flattened_points += f"<br>🐂 **【多头爆破手】**：{bull_agent}"
            if not _is_empty_or_na(bear_agent): flattened_points += f"<br>🐻 **【空头狙击手】**：{bear_agent}"
            if not _is_empty_or_na(fin_audit): flattened_points += f"<br>💣 **【Agent C 审计】**：{fin_audit}"
            
            original_kp = d.get('key_points', '')
            if not _is_empty_or_na(original_kp):
                flattened_points += f"<br>📌 **【核心看点】**：{original_kp}"
                
            d['key_points'] = flattened_points if flattened_points else "暂无特殊看点"

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
                debate_process=debate, dashboard=d.get('dashboard'),
                analysis_summary=d.get('analysis_summary', '完成'), 
                key_points=d.get('key_points'), # <--- 这里已经被强行塞满了所有牛逼的高级数据
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
