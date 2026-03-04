# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (A股超神特化·热修复防断网版)
===================================
职责：
1. 封装 Gemini API 调用逻辑 (附带 OpenAI/Claude 无缝备用)
2. 利用 Google Search Grounding 获取实时新闻 (双引擎交叉验证)
3. 【A股特化】龙虎榜追踪、连板基因、OBV能量潮、CCI妖股雷达、大盘宏观水温
4. 【最新加强】跳空缺口雷达、MA5均线斜率拐点探测
5. 【抗断网引擎】自研 VWAP 筹码分布测算兜底算法，彻底修复东财 API 频繁断网问题。
6. 【极值护盾】底层增加除零(ZeroDivision)与NaN免疫机制。
"""

import json
import logging
import time
import re
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


# 股票名称映射（常见股票）
STOCK_NAME_MAP = {
    '600519': '贵州茅台', '000001': '平安银行', '300750': '宁德时代', '002594': '比亚迪',
    '600036': '招商银行', '601318': '中国平安', '000858': '五粮液', '600276': '恒瑞医药',
    '601012': '隆基绿能', '002475': '立讯精密', '300059': '东方财富', '002415': '海康威视',
    '600900': '长江电力', '601166': '兴业银行', '600028': '中国石化',
}


def get_stock_name_multi_source(
    stock_code: str,
    context: Optional[Dict] = None,
    data_manager = None
) -> str:
    if context:
        if context.get('stock_name'):
            name = context['stock_name']
            if name and not name.startswith('股票'):
                return name
        if 'realtime' in context and context['realtime'].get('name'):
            return context['realtime']['name']

    if stock_code in STOCK_NAME_MAP:
        return STOCK_NAME_MAP[stock_code]

    if data_manager is None:
        try:
            from data_provider.base import DataFetcherManager
            data_manager = DataFetcherManager()
        except Exception as e:
            logger.debug(f"无法初始化 DataFetcherManager: {e}")

    if data_manager:
        try:
            name = data_manager.get_stock_name(stock_code)
            if name:
                STOCK_NAME_MAP[stock_code] = name
                return name
        except Exception as e:
            logger.debug(f"从数据源获取股票名称失败: {e}")

    return f'股票{stock_code}'


@dataclass
class AnalysisResult:
    """
    AI 分析结果数据类 - 决策仪表盘版
    """
    code: str
    name: str

    # ========== 核心指标 ==========
    sentiment_score: int
    trend_prediction: str
    operation_advice: str
    decision_type: str = "hold"
    confidence_level: str = "中"

    # ========== 思维链（CoT） ==========
    debate_process: Optional[Dict[str, str]] = None

    # ========== 决策仪表盘 ==========
    dashboard: Optional[Dict[str, Any]] = None

    # ========== 走势分析 ==========
    trend_analysis: str = ""
    short_term_outlook: str = ""
    medium_term_outlook: str = ""

    # ========== 技术面分析 ==========
    technical_analysis: str = ""
    ma_analysis: str = ""
    volume_analysis: str = ""
    pattern_analysis: str = ""

    # ========== 基本面分析 ==========
    fundamental_analysis: str = ""
    sector_position: str = ""
    company_highlights: str = ""

    # ========== 情绪面/消息面分析 ==========
    news_summary: str = ""
    market_sentiment: str = ""
    hot_topics: str = ""

    # ========== 综合分析 ==========
    analysis_summary: str = ""
    key_points: str = ""
    risk_warning: str = ""
    buy_reason: str = ""

    # ========== 元数据 ==========
    market_snapshot: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None
    search_performed: bool = False
    data_sources: str = ""
    success: bool = True
    error_message: Optional[str] = None

    # ========== 价格与持仓数据 ==========
    current_price: Optional[float] = None
    change_pct: Optional[float] = None
    user_cost: Optional[float] = None  # 直接保存用户成本
    user_shares: Optional[int] = None  # 直接保存用户持仓数

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'name': self.name,
            'sentiment_score': self.sentiment_score,
            'trend_prediction': self.trend_prediction,
            'operation_advice': self.operation_advice,
            'decision_type': self.decision_type,
            'confidence_level': self.confidence_level,
            'debate_process': self.debate_process,
            'dashboard': self.dashboard,  
            'trend_analysis': self.trend_analysis,
            'short_term_outlook': self.short_term_outlook,
            'medium_term_outlook': self.medium_term_outlook,
            'technical_analysis': self.technical_analysis,
            'ma_analysis': self.ma_analysis,
            'volume_analysis': self.volume_analysis,
            'pattern_analysis': self.pattern_analysis,
            'fundamental_analysis': self.fundamental_analysis,
            'sector_position': self.sector_position,
            'company_highlights': self.company_highlights,
            'news_summary': self.news_summary,
            'market_sentiment': self.market_sentiment,
            'hot_topics': self.hot_topics,
            'analysis_summary': self.analysis_summary,
            'key_points': self.key_points,
            'risk_warning': self.risk_warning,
            'buy_reason': self.buy_reason,
            'market_snapshot': self.market_snapshot,
            'search_performed': self.search_performed,
            'success': self.success,
            'error_message': self.error_message,
            'current_price': self.current_price,
            'change_pct': self.change_pct,
            'user_cost': self.user_cost,
            'user_shares': self.user_shares,
        }

    def get_core_conclusion(self) -> str:
        if self.dashboard and 'core_conclusion' in self.dashboard:
            return self.dashboard['core_conclusion'].get('one_sentence', self.analysis_summary)
        return self.analysis_summary

    def get_position_advice(self, has_position: bool = False) -> str:
        if self.dashboard and 'core_conclusion' in self.dashboard:
            pos_advice = self.dashboard['core_conclusion'].get('position_advice', {})
            if has_position:
                return pos_advice.get('has_position', self.operation_advice)
            return pos_advice.get('no_position', self.operation_advice)
        return self.operation_advice

    def get_sniper_points(self) -> Dict[str, str]:
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('sniper_points', {})
        return {}

    def get_checklist(self) -> List[str]:
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('action_checklist', [])
        return []

    def get_risk_alerts(self) -> List[str]:
        if self.dashboard and 'intelligence' in self.dashboard:
            return self.dashboard['intelligence'].get('risk_alerts', [])
        return []

    def get_emoji(self) -> str:
        emoji_map = {
            '买入': '🟢', '加仓': '🟢', '强烈买入': '💚',
            '持有': '🟡', '观望': '⚪',
            '减仓': '🟠', '卖出': '🔴', '强烈卖出': '❌',
        }
        advice = self.operation_advice or ''
        if advice in emoji_map: return emoji_map[advice]
        for part in advice.replace('/', '|').split('|'):
            part = part.strip()
            if part in emoji_map: return emoji_map[part]
        score = self.sentiment_score
        if score >= 80: return '💚'
        elif score >= 65: return '🟢'
        elif score >= 55: return '🟡'
        elif score >= 45: return '⚪'
        elif score >= 35: return '🟠'
        else: return '🔴'

    def get_confidence_stars(self) -> str:
        star_map = {'高': '⭐⭐⭐', '中': '⭐⭐', '低': '⭐'}
        return star_map.get(self.confidence_level, '⭐⭐')


class GeminiAnalyzer:
    """
    Gemini AI 分析器 (A股工业级特化版)
    """

    SYSTEM_PROMPT = """你是一位深谙中国A股“资金市”、“情绪市”与“龙头战法”的顶级游资操盘手兼风控总监。

## 🛑 反偷懒协议 (Anti-N/A Protocol) - 优先级最高
你的字典里没有 "N/A"、"暂无"、"未知"。无论输入数据多么残缺，你都必须完成量化计算：
1. 狙击点位禁止 N/A：如果均线或支撑位缺失，必须用当前价格推算！(例如：止损位=现价×0.95，买入点=现价×0.98)。
2. 量化仓位禁止 N/A：仓位策略必须是一个明确的百分比(如 `20%` 或 `0%`)。绝不能空着或填 N/A！
3. 成本分析：即使系统没有获取到用户的成本数据，你也要假定用户是“空仓准备买入”的状态。

## 🧠 A股专属思维链 (CoT) 推演
在输出结论前，必须在 `debate_process` 中展现三位A股顶尖专家的内部推演：
1. 【打板接力客】：寻找妖股基因。分析该股是否上了龙虎榜、是否有连板涨停基因、换手率是否活跃、CCI是否超强、是否有跳空缺口。
2. 【公募风控总监】：专挑刺防诱多。紧盯 OBV量价背离、KDJ极值、高位巨量换手率（派发）、以及大盘宏观冰点。
3. 【铁血总舵主】：结合主人的【私人持仓成本】和【打脸回测雷达】进行终审。如果套牢，严格按 ATR 给割肉位；如果被打脸，必须认错！

## A股终极量化法则

### 1. 股性与缺口理论 (Gap & Dragon Head)
- 弱水三千只取一瓢。如果该股近15日频现涨停，且上过龙虎榜，说明有游资记忆，逢低回踩(MA10/BOLL中轨)是大买点。
- 向上跳空缺口：极强进攻信号，只要缺口不补，坚定看多！向下跳空直接判死刑。
- 换手率判定：>25%高度警惕见顶派发；8%-15%为健康游资接力活跃区；<3%说明是死鱼跟风盘。

### 2. 量价识破诱多 (OBV & MACD)
- A股主力最喜欢拉高骗炮。如果股价创新高，但 OBV 能量潮往下走，或者 MACD 出现顶背离，必须判定为【诱多出货】，强烈建议减仓！

### 3. ATR 波动率与网格自救
- A股震荡市或深套自救时，必须启用 T+0 网格战法。利用系统提供的 ATR（日均真实波幅），设计网格买卖间距（如每跌 X 元买入，反弹 X 元卖出）。

### 4. 极端大盘情绪一票否决
- 倾巢之下无完卵。如果提供的大盘赚钱效应极低（如<20%的极度冰点），不论个股图形多好看，必须把评级下调至【观望/减仓】！

### 5. 双新闻引擎排雷
- A股是政策市。重点排查公告中隐藏的【减持计划】、【违规立案】与【业绩下修】。

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出：

```json
{
    "stock_name": "股票中文名称",
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "confidence_level": "高/中/低",

    "debate_process": {
        "hot_money_trader": "打板接力客发言(查股性/龙虎榜/缺口/资金)：...",
        "risk_director": "风控总监发言(查OBV背离/均线斜率/死亡换手/大盘)：...",
        "chief_commander": "总舵主总结(结合打脸历史与个人成本的终审)：..."
    },

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "一句话核心结论（30字以内，直接下达军事级指令）",
            "signal_type": "🟢龙头接力买点/🟡缩量回踩观望/🔴量价顶背离警告/⚠️破位强制割肉",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice": {
                "no_position": "空仓者建议：...",
                "has_position": "持仓者建议：..."
            }
        },

        "data_perspective": {
            "a_share_features": {
                "limit_up_gene": "是否有涨停基因与妖股潜质",
                "lhb_status": "是否有游资龙虎榜足迹",
                "gap_and_trend": "跳空缺口情况及短期均线拐点"
            },
            "indicator_trinity": {
                "macd_status": "金叉/死叉/顶背离/底背离",
                "kdj_cci_status": "KDJ与CCI极值解读",
                "boll_status": "突破上轨/跌破中轨等"
            },
            "price_position": {
                "current_price": 0.00,
                "bias_ma5": 0.00,
                "bias_status": "安全/警戒/危险"
            },
            "volume_analysis": {
                "turnover_rate": 0.00,
                "volume_status": "放量/缩量/平量",
                "obv_trend": "OBV能量潮是否与股价背离(主力真实意图)"
            }
        },

        "intelligence": {
            "latest_news": "【最新舆情】近期重要新闻摘要",
            "announcements": "【核心公告】核心公告与A股政策面提炼",
            "risk_alerts": ["必须包含大盘宏观风险或主力派发等点位", "打脸反思：如果是误判，必须在此处深刻反思认错"],
            "positive_catalysts": ["利好或超额Alpha收益"],
            "sentiment_summary": "双引擎情绪一致性总结"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "XX元（严禁填N/A，基于缺口或MA10推算）",
                "secondary_buy": "XX元",
                "trailing_stop": "XX元（严禁填N/A，基于ATR波幅计算的防守位）",
                "take_profit": "XX元（基于POC筹码峰阻力）"
            },
            "grid_trading_plan": {
                "is_recommended": true,
                "grid_spacing": "网格做T间距：XX元 (基于ATR测算)",
                "buy_grid": "每回调XX元买入X股/成",
                "sell_grid": "每反弹XX元卖出X股/成"
            },
            "position_strategy": {
                "personal_cost_review": "用户持仓与成本状况分析...",
                "quant_position_sizing": "XX% (必须是纯百分比数字，如 20%)",
                "entry_plan": "建仓/逃生策略",
                "risk_control": "风控策略"
            },
            "action_checklist": [
                "✅/⚠️/❌ 检查项1：具备涨停基因且资金活跃",
                "✅/⚠️/❌ 检查项2：换手率健康未现高位派发",
                "✅/⚠️/❌ 检查项3：均线拐头向上且无OBV背离",
                "✅/⚠️/❌ 检查项4：A股宏观水温正常，非连环冰点"
            ]
        }
    },

    "analysis_summary": "100字A股实战深度总结（若昨日判断亏损，必须在此深刻检讨认错）",
    "key_points": "3-5个核心看点",
    "risk_warning": "退市/减持/天地板风险提示",
    "buy_reason": "冷血操作理由",

    "trend_analysis": "走势形态分析",
    "short_term_outlook": "短期1-3日展望",
    "medium_term_outlook": "中期1-2周展望",
    "technical_analysis": "技术面综合分析",
    "ma_analysis": "均线与指标三剑客分析",
    "volume_analysis": "量价背离深度分析",
    "pattern_analysis": "K线形态分析",
    "fundamental_analysis": "基本面分析",
    "sector_position": "板块与题材炒作分析",
    "company_highlights": "亮点与风险",
    "news_summary": "新闻与公告综合摘要",
    "market_sentiment": "散户与主力博弈情绪",
    "hot_topics": "相关概念风口",

    "search_performed": true,
    "data_sources": "数据来源说明"
}

决策仪表盘最高原则
 * 显式思维链先行：必须在 debate_process 中完成游资与风控的实盘博弈。
 * 量化与网格：仓位必须输出明确的百分比(%)。震荡期必须依靠 ATR 输出 T+0 网格间距。
 * 禁止N/A偷懒：如果支撑压力等数据缺失，必须通过当前收盘价向下按百分比强行推算止损位！绝对禁止输出 N/A。
   """
   def init(self, api_key: Optional[str] = None):
   config = get_config()
   self._api_key = api_key or config.gemini_api_key
   self._model = None
   self._current_model_name = None
   self._using_fallback = False
   self._use_openai = False
   self._use_anthropic = False
   self._openai_client = None
   self._anthropic_client = None
   gemini_key_valid = self._api_key and not self.api_key.startswith('your') and len(self._api_key) > 10
   if gemini_key_valid:
   try:
   self._init_model()
   except Exception as e:
   logger.warning(f"Gemini init failed: {e}, trying Anthropic then OpenAI")
   self._try_anthropic_then_openai()
   else:
   logger.info("Gemini API Key not configured, trying Anthropic then OpenAI")
   self._try_anthropic_then_openai()
   def _try_anthropic_then_openai(self) -> None:
   self._init_anthropic_fallback()
   self._init_openai_fallback()
   def init_anthropic_fallback(self) -> None:
   config = get_config()
   anthropic_key_valid = (
   config.anthropic_api_key
   and not config.anthropic_api_key.startswith('your')
   and len(config.anthropic_api_key) > 10
   )
   if not anthropic_key_valid:
   return
   try:
   from anthropic import Anthropic
   self._anthropic_client = Anthropic(api_key=config.anthropic_api_key)
   self._current_model_name = config.anthropic_model
   self._use_anthropic = True
   except Exception as e:
   logger.error(f"Anthropic API init failed: {e}")
   def init_openai_fallback(self) -> None:
   config = get_config()
   openai_key_valid = (
   config.openai_api_key and
   not config.openai_api_key.startswith('your') and
   len(config.openai_api_key) > 10
   )
   if not openai_key_valid:
   return
   try:
   from openai import OpenAI
   client_kwargs = {"api_key": config.openai_api_key}
   if config.openai_base_url and config.openai_base_url.startswith('http'):
   client_kwargs["base_url"] = config.openai_base_url
   # 支持 AIHubMix API Key (通过 APP-Code 传递)
   if config.openai_base_url and "aihubmix.com" in config.openai_base_url:
   aihubmix_key = config.openai_api_key
   if not aihubmix_key or aihubmix_key.startswith('your_'):
   aihubmix_key = "GPIJ3886"
   client_kwargs["default_headers"] = {"APP-Code": aihubmix_key}
   self._openai_client = OpenAI(**client_kwargs)
   self._current_model_name = config.openai_model
   self._use_openai = True
   except Exception as e:
   logger.error(f"OpenAI 兼容 API 初始化失败: {e}")
   def _init_model(self) -> None:
   try:
   try:
   import google.generativeai as genai
   genai.configure(api_key=self._api_key)
   except ImportError:
   pass
   config = get_config()
   model_name = config.gemini_model
   fallback_model = config.gemini_model_fallback
   try:
   self._model = genai.GenerativeModel(
   model_name=model_name,
   system_instruction=self.SYSTEM_PROMPT,
   )
   self._current_model_name = model_name
   self._using_fallback = False
   except Exception as model_error:
   self._model = genai.GenerativeModel(
   model_name=fallback_model,
   system_instruction=self.SYSTEM_PROMPT,
   )
   self._current_model_name = fallback_model
   self._using_fallback = True
   except Exception as e:
   self._model = None
   def _switch_to_fallback_model(self) -> bool:
   try:
   import google.generativeai as genai
   config = get_config()
   fallback_model = config.gemini_model_fallback
   self._model = genai.GenerativeModel(
   model_name=fallback_model,
   system_instruction=self.SYSTEM_PROMPT,
   )
   self._current_model_name = fallback_model
   self._using_fallback = True
   logger.warning(f"🔄 [Gemini] 触发防宕机降级机制，成功切换到备选模型: {fallback_model}")
   return True
   except Exception as e:
   logger.error(f"❌ [Gemini] 切换备选模型失败: {e}")
   return False
   def is_available(self) -> bool:
   return (
   self._model is not None
   or self._anthropic_client is not None
   or self._openai_client is not None
   )
   def _call_anthropic_api(self, prompt: str, generation_config: dict) -> str:
   config = get_config()
   max_retries = config.gemini_max_retries
   base_delay = config.gemini_retry_delay
   temperature = generation_config.get('temperature', config.anthropic_temperature)
   max_tokens = generation_config.get('max_output_tokens', config.anthropic_max_tokens)
   for attempt in range(max_retries):
   try:
   if attempt > 0:
   delay = min(base_delay * (2 ** (attempt - 1)), 60)
   time.sleep(delay)
   message = self._anthropic_client.messages.create(
   model=self._current_model_name,
   max_tokens=max_tokens,
   system=self.SYSTEM_PROMPT,
   messages=[{"role": "user", "content": prompt}],
   temperature=temperature,
   )
   if message.content and len(message.content) > 0 and hasattr(message.content[0], 'text'):
   return message.content[0].text
   raise ValueError("Anthropic API returned empty response")
   except Exception as e:
   if attempt == max_retries - 1:
   raise
   raise Exception("Anthropic API failed")
   def _call_openai_api(self, prompt: str, generation_config: dict) -> str:
   config = get_config()
   max_retries = config.gemini_max_retries
   base_delay = config.gemini_retry_delay
   def _build_base_request_kwargs() -> dict:
   model_name = self._current_model_name
   kwargs = {
   "model": model_name,
   "messages": [
   {"role": "system", "content": self.SYSTEM_PROMPT},
   {"role": "user", "content": prompt},
   ],
   "temperature": generation_config.get('temperature', config.openai_temperature),
   }
   payload = get_thinking_extra_body(model_name)
   if payload:
   kwargs["extra_body"] = payload
   return kwargs
   def _is_unsupported_param_error(error_message: str, param_name: str) -> bool:
   lower_msg = error_message.lower()
   return ('400' in lower_msg or "unsupported parameter" in lower_msg) and param_name in lower_msg
   if not hasattr(self, "_token_param_mode"):
   self._token_param_mode = {}
   max_output_tokens = generation_config.get('max_output_tokens', 8192)
   model_name = self._current_model_name
   mode = self._token_param_mode.get(model_name, "max_tokens")
   def _kwargs_with_mode(mode_value):
   kwargs = _build_base_request_kwargs()
   if mode_value is not None:
   kwargs[mode_value] = max_output_tokens
   return kwargs
   for attempt in range(max_retries):
   try:
   if attempt > 0:
   delay = min(base_delay * (2 ** (attempt - 1)), 60)
   logger.info(f"[OpenAI] 第 {attempt + 1} 次重试，等待 {delay:.1f} 秒...")
   time.sleep(delay)
   try:
   response = self._openai_client.chat.completions.create(_kwargs_with_mode(mode))
   except Exception as e:
   error_str = str(e)
   if mode == "max_tokens" and _is_unsupported_param_error(error_str, "max_tokens"):
   mode = "max_completion_tokens"
   self._token_param_mode[model_name] = mode
   response = self._openai_client.chat.completions.create(_kwargs_with_mode(mode))
   elif mode == "max_completion_tokens" and _is_unsupported_param_error(error_str, "max_completion_tokens"):
   mode = None
   self._token_param_mode[model_name] = mode
   response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
   else:
   raise
   if response and response.choices and response.choices[0].message.content:
   return response.choices[0].message.content
   else:
   raise ValueError("OpenAI API 返回空响应")
   except Exception as e:
   error_str = str(e)
   is_rate_limit = '429' in error_str or 'rate' in error_str.lower() or 'quota' in error_str.lower()
   if is_rate_limit:
   logger.warning(f"[OpenAI] API 限流，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:100]}")
   else:
   logger.warning(f"[OpenAI] API 调用失败，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:100]}")
   if attempt == max_retries - 1:
   raise
   raise Exception("OpenAI API failed")
   def _call_api_with_retry(self, prompt: str, generation_config: dict) -> str:
   if self._use_anthropic:
   try:
   return self._call_anthropic_api(prompt, generation_config)
   except Exception as anthropic_error:
   if self._openai_client:
   return self._call_openai_api(prompt, generation_config)
   raise anthropic_error
   if self._use_openai:
   return self._call_openai_api(prompt, generation_config)
   config = get_config()
   # 强制提高保底重试次数到 8 次，确保哪怕高频限流也能最终熬过去
   max_retries = max(config.gemini_max_retries, 8)
   base_delay = config.gemini_retry_delay
   last_error = None
   tried_fallback = getattr(self, '_using_fallback', False)
   for attempt in range(max_retries):
   try:
   response = self._model.generate_content(
   prompt,
   generation_config=generation_config,
   request_options={"timeout": 120}
   )
   if response and response.text:
   return response.text
   else:
   raise ValueError("Gemini 返回空响应")
   except Exception as e:
   last_error = e
   error_str = str(e)
   is_rate_limit = '429' in error_str or 'quota' in error_str.lower() or 'rate' in error_str.lower()
   if is_rate_limit:
   logger.warning(f"[Gemini] API 限流 (429)，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:150]}")
   match = re.search(r'retry in (\d+.?\d*)s', error_str)
   if match:
   delay = float(match.group(1)) + 5.0
   else:
   delay = min(base_delay * (2 ** attempt), 60)
   if attempt >= max_retries // 2 and not tried_fallback:
   logger.info("⚠️ [Gemini] 已达最大重试阈值的一半，尝试切换备用模型...")
   if self._switch_to_fallback_model():
   tried_fallback = True
   logger.info(f"⏳ [Gemini] 触发限流，强制休眠等待 API 额度恢复 {delay:.2f} 秒...")
   time.sleep(delay)
   else:
   logger.warning(f"[Gemini] API 调用失败，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:100]}")
   delay = min(base_delay * (2 ** attempt), 30)
   time.sleep(delay)
   if self._anthropic_client:
   try:
   return self._call_anthropic_api(prompt, generation_config)
   except Exception as anthropic_error:
   if self._openai_client:
   try:
   return self._call_openai_api(prompt, generation_config)
   except Exception as openai_error:
   raise last_error or anthropic_error or openai_error
   raise last_error or anthropic_error
   if self._openai_client:
   try:
   return self._call_openai_api(prompt, generation_config)
   except Exception as openai_error:
   raise last_error or openai_error
   if config.anthropic_api_key and not self._anthropic_client:
   self._init_anthropic_fallback()
   if self._anthropic_client:
   try:
   return self._call_anthropic_api(prompt, generation_config)
   except Exception as ae:
   if self._openai_client:
   try:
   return self._call_openai_api(prompt, generation_config)
   except Exception as oe:
   raise last_error or ae or oe
   raise last_error or ae
   if config.openai_api_key and not self._openai_client:
   self._init_openai_fallback()
   if self._openai_client:
   try:
   return self._call_openai_api(prompt, generation_config)
   except Exception as openai_error:
   raise last_error or openai_error
   raise last_error or Exception("所有 AI API 调用失败")
   def analyze(
   self,
   context: Dict[str, Any],
   news_context: Optional[str] = None,
   announcement_context: Optional[str] = None
   ) -> AnalysisResult:
   code = context.get('code', 'Unknown')
   config = get_config()
   request_delay = config.gemini_request_delay
   if request_delay > 0:
   time.sleep(request_delay)
   name = context.get('stock_name')
   if not name or name.startswith('股票'):
   if 'realtime' in context and context['realtime'].get('name'):
   name = context['realtime']['name']
   else:
   name = STOCK_NAME_MAP.get(code, f'股票{code}')
   if not self.is_available():
   return AnalysisResult(
   code=code, name=name, sentiment_score=50, trend_prediction='震荡', operation_advice='持有',
   analysis_summary='AI 分析功能未启用', success=False, error_message='API Key 未配置'
   )
   try:
   # 格式化输入（包含技术面数据、双新闻引擎、打脸回测、指标三剑客、A股特化核武）
   prompt = self._format_prompt(context, name, news_context, announcement_context)
   generation_config = {
   "temperature": config.gemini_temperature,
   "max_output_tokens": 8192,
   }
   response_text = self._call_api_with_retry(prompt, generation_config)
   result = self._parse_response(response_text, code, name)
   result.raw_response = response_text
   result.search_performed = bool(news_context or announcement_context)
   result.market_snapshot = self._build_market_snapshot(context)
   # 同步保存提取到的仓位数据
   result.user_cost = context.get('user_cost')
   result.user_shares = context.get('user_shares')
   return result
   except Exception as e:
   logger.error(f"AI 分析 {name}({code}) 失败: {e}")
   return AnalysisResult(
   code=code, name=name, sentiment_score=50, trend_prediction='未知(API报错)', operation_advice='观望',
   analysis_summary=f'系统提示: API 额度耗尽或发生网络异常，导致分析生成失败。异常信息: {str(e)[:100]}',
   success=False, error_message=str(e),
   user_cost=context.get('user_cost'), user_shares=context.get('user_shares')
   )
   =====================================================================
   辅助方法
   =====================================================================
   def _safe_float(self, val: Any) -> Optional[float]:
   try:
   if isinstance(val, (int, float)): return float(val)
   if isinstance(val, str):
   cleaned = val.replace(',', '').replace('%', '').strip()
   return float(cleaned)
   except:
   pass
   return None
   def _format_prompt(
   self,
   context: Dict[str, Any],
   name: str,
   news_context: Optional[str] = None,
   announcement_context: Optional[str] = None
   ) -> str:
   code = context.get('code', 'Unknown')
   stock_name = context.get('stock_name', name)
   if not stock_name or stock_name == f'股票{code}':
   stock_name = STOCK_NAME_MAP.get(code, f'股票{code}')
   today = context.get('today', {})
   # ========== [底层A股本土特化引擎] ==========
   personal_status_text = ""
   try:
   import urllib.request, csv, io, glob
   import akshare as ak
   import pandas as pd
   import numpy as np
   from datetime import datetime, timedelta
   # 【A股核武 1：宏观情绪滤网 (大盘冰点一票否决)】
   try:
   spot_df = ak.stock_zh_a_spot_em()
   if not spot_df.empty:
   up_count = len(spot_df[spot_df['涨跌幅'] > 0])
   down_count = len(spot_df[spot_df['涨跌幅'] < 0])
   if (up_count + down_count) > 0:
   up_ratio = up_count / (up_count + down_count) * 100
   temp_str = "🌋 逼空极度贪婪" if up_ratio > 80 else "🥶 多杀多极度冰点" if up_ratio < 20 else "😐 情绪中性"
   personal_status_text += f"### 📉 A股大盘宏观水温\n* 全市场赚钱效应：{up_ratio:.1f}% ({temp_str})\n* ⚠️ 风控指令：若处于“极度冰点”，发生系统性杀跌概率极大，务必将评级下调至观望防守！\n\n"
   except Exception as e: logger.debug(f"宏观数据异常: {e}")
   # 【A股核武 2：云端仓位算账 (盯死成本底线)】
   csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv"
   my_cost, my_shares = None, None
   curr_price = today.get('close')
   try:
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
   except Exception as sheet_err:
   logger.debug(f"云端持仓数据访问异常: {sheet_err}")
   cp_val = self._safe_float(curr_price)
   if my_cost and cp_val:
   context['user_cost'] = my_cost
   context['user_shares'] = my_shares
   safe_cost = my_cost if my_cost > 0 else 1e-9
   profit_pct = ((cp_val - safe_cost) / safe_cost) * 100
   status_emoji = "🔴套牢亏损中" if profit_pct < 0 else "🟢主升浪吃肉中"
   personal_status_text += f"### 💰 机构持仓底牌 (实时同步)\n* 成本价：{my_cost:.2f} 元 | 当前盈亏：{profit_pct:.2f}% ({status_emoji})\n* 🚨 法官最高指令：用户目前处于{status_emoji}状态。你必须在 position_strategy 中显式输出针对此成本的【严酷应对策略】！\n\n"
   else:
   personal_status_text += "### 💰 机构持仓底牌\n* 当前状态：空仓 或 未获取到历史持仓。\n* 🚨 法官最高指令：按【新建仓】视角规划，必须给出明确量化建议仓位（如 15%），绝不能输出 N/A！\n\n"
   # 【A股核武 3：主力龙虎榜与北向资金透视】
   try:
   fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
   flow_desc = f"东方财富内资净流入: {fund_flow.iloc[-1]['主力净流入-净额']/10000:.1f}万"
   try:
   hk_funds = ak.stock_hsgt_stock_statistics_em()
   my_hk = hk_funds[hk_funds['代码'] == code]
   if not my_hk.empty:
   hk_c = my_hk.iloc[0]['今日增持估计-市值']
   flow_desc += f" | 北向资金: {'🟢流入' if hk_c > 0 else '🔴砸盘流出'} {abs(hk_c)/10000:.1f}万"
   except: pass
   lhb_desc = "暂无龙虎榜游资数据"
   try:
   end_date_str = datetime.now().strftime('%Y%m%d')
   start_date_str = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
   lhb_df = ak.stock_lhb_detail_em(start_date=start_date_str, end_date=end_date_str)
   if not lhb_df.empty:
   my_lhb = lhb_df[lhb_df['代码'] == code]
   if not my_lhb.empty:
   lhb_desc = f"🚨 近10日登榜 {len(my_lhb)} 次，有顶级游资运作痕迹！"
   else:
   lhb_desc = "🧊 近10日未上龙虎榜，属于非核心跟风标的。"
   except: pass
   personal_status_text += f"### 🌊 A股资金博弈与游资雷达\n* 今日资金动向：{flow_desc}\n* 龙虎榜足迹：{lhb_desc}\n\n"
   except Exception as e: logger.debug(f"资金雷达异常: {e}")
   # 【A股核武 4：自研 VWAP 筹码兜底引擎 (防东财API断网)】
   vwap_60 = 0
   syn_profit_ratio = 0
   try:
   if 'history' in context and len(context['history']) >= 60:
   df_hist = pd.DataFrame(context['history']).tail(60)
   for col in ['close', 'volume']:
   if col in df_hist.columns:
   df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce').fillna(0)
   total_vol = df_hist['volume'].sum()
   if total_vol > 0:
   vwap_60 = (df_hist['close'] * df_hist['volume']).sum() / total_vol
   if cp_val:
   profit_vol = df_hist[df_hist['close'] <= cp_val]['volume'].sum()
   syn_profit_ratio = (profit_vol / total_vol) * 100
   except Exception as e: logger.debug(f"筹码兜底测算异常: {e}")
   # 【A股核武 5：量价背离、极值三剑客、跳空缺口、均线斜率】
   try:
   if 'history' in context and len(context['history']) >= 60:
   df_hist = pd.DataFrame(context['history']).tail(60)
   for col in ['close', 'high', 'low', 'open', 'volume', 'pct_chg']:
   if col in df_hist.columns:
   df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce').fillna(method='ffill')
   s_prices = df_hist['close']
   # -> 妖股连板基因
   df_hist_15 = df_hist.tail(15)
   if 'pct_chg' in df_hist_15.columns:
   zt_count = len(df_hist_15[df_hist_15['pct_chg'] >= 9.5])
   gene_str = "🔥股性极度活跃(具备妖股基因)" if zt_count >= 2 else "🌟有异动基因(主力试盘)" if zt_count == 1 else "🧊股性沉闷(不适合打板接力)"
   else:
   gene_str = "未知"
   # -> 跳空缺口雷达 (Gap Analysis)
   yesterday_high = df_hist['high'].iloc[-2]
   yesterday_low = df_hist['low'].iloc[-2]
   today_low = df_hist['low'].iloc[-1]
   today_high = df_hist['high'].iloc[-1]
   gap_str = "无明显跳空缺口"
   if today_low > yesterday_high:
   gap_str = f"🚀 向上跳空缺口 ({yesterday_high:.2f}-{today_low:.2f}) [极强进攻看多信号]"
   elif today_high < yesterday_low:
   gap_str = f"🕳️ 向下跳空缺口 ({today_high:.2f}-{yesterday_low:.2f}) [极度弱势破位信号]"
   # -> MA5 均线斜率拐点探测
   ma5_series = s_prices.rolling(5, min_periods=1).mean()
   ma5_trend = "↗️ 拐头向上 (短线做多)" if len(ma5_series)>=2 and ma5_series.iloc[-1] > ma5_series.iloc[-2] else "↘️ 拐头向下 (短线承压)"
   # -> CCI 妖股顺势雷达
   if 'high' in df_hist.columns and 'low' in df_hist.columns:
   tp = (df_hist['high'] + df_hist['low'] + s_prices) / 3
   ma_tp = tp.rolling(14, min_periods=1).mean()
   md = tp.rolling(14, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
   cci = (tp - ma_tp) / (0.015 * md.replace(0, 1e-9))
   cci_val = cci.iloc[-1]
   cci_status = "🔥 异动妖股主升浪区间 (>100)" if cci_val > 100 else "🥶 极度超跌错杀区间 (<-100)" if cci_val < -100 else "震荡摩擦区间"
   else:
   cci_val, cci_status = 0, "未知"
   # -> OBV 量能潮汐背离
   df_hist['obv_sign'] = np.sign(s_prices.diff().fillna(0))
   df_hist['OBV'] = (df_hist['obv_sign'] * df_hist['volume']).cumsum()
   obv_trend = df_hist['OBV'].iloc[-1] - df_hist['OBV'].iloc[-5]
   obv_status = "🌊资金真实吸筹(OBV向好)" if obv_trend > 0 else "🩸警惕阴跌诱多(OBV量价背离)"
   # -> KDJ 极值护盾
   low_min9 = s_prices.rolling(9, min_periods=1).min()
   high_max9 = s_prices.rolling(9, min_periods=1).max()
   denom = (high_max9 - low_min9).replace(0, 1e-9)
   rsv = (s_prices - low_min9) / denom * 100
   k = rsv.ewm(com=2, adjust=False).mean()
   d = k.ewm(com=2, adjust=False).mean()
   j = 3 * k - 2 * d
   j_val = j.iloc[-1]
   kdj_status = "💡黄金坑超卖 (J<0)" if j_val < 0 else ("⚠️严重超买钝化 (J>100)" if j_val > 100 else "安全区")
   # -> BOLL 轨道
   ma20 = s_prices.rolling(20, min_periods=1).mean()
   std20 = s_prices.rolling(20, min_periods=1).std().fillna(0)
   upper = ma20 + 2 * std20
   lower = ma20 - 2 * std20
   curr = s_prices.iloc[-1]
   boll_status = "🚀突破上轨(强动能或随时被砸)" if curr > upper.iloc[-1] else ("🕳️跌破下轨(弱势期)" if curr < lower.iloc[-1] else "通道内运行")
   # -> MACD 动能
   exp1 = s_prices.ewm(span=12, adjust=False).mean()
   exp2 = s_prices.ewm(span=26, adjust=False).mean()
   macd = exp1 - exp2
   signal = macd.ewm(span=9, adjust=False).mean()
   hist = macd - signal
   macd_status = "🔴死叉向下" if hist.iloc[-1] < 0 else "🟢金叉向上"
   # -> ATR 真实波幅
   highs = df_hist['high']
   lows = df_hist['low']
   tr = pd.concat([highs - lows, (highs - s_prices.shift()).abs(), (lows - s_prices.shift()).abs()], axis=1).max(axis=1)
   atr14 = tr.rolling(14, min_periods=1).mean()
   current_atr = atr14.iloc[-1]
   # -> POC 筹码峰
   df_hist['price_bin'] = pd.cut(s_prices, bins=12)
   vp = df_hist.groupby('price_bin', observed=False)['volume'].sum()
   poc_price = vp.idxmax().mid
   personal_status_text += f"""### 🎯 A股核心量化指标 (机密)
 * 涨停基因(近15日): {gene_str}
 * 缺口与均线雷达: {gap_str} | MA5斜率: {ma5_trend}
 * CCI 顺势雷达: {cci_val:.1f} ({cci_status})
 * OBV 资金潮汐: {obv_status} (若股价新高但OBV跌，即为诱多派发)
 * KDJ 情绪极值: J值={j_val:.1f} ({kdj_status})
 * BOLL 布林测压: 上轨{upper.iloc[-1]:.2f} / 下轨{lower.iloc[-1]:.2f} [{boll_status}]
 * MACD 动能: {macd_status} (柱值: {hist.iloc[-1]:.3f})
 * ATR 真实波幅: {current_atr:.2f}元 (网格T+0与1.5倍ATR防洗盘止损核心依据)
 * POC 历史筹码峰: 约 {poc_price:.2f}元 (突破则海阔天空，受压则万劫不复)
   """
   except Exception as e: logger.debug(f"量价深度指标异常: {e}")
   # 【A股核武 6：RPS 超额收益】
   try:
   if 'history' in context and len(context['history']) >= 20:
   df_h = pd.DataFrame(context['history'])
   stock_c = (float(df_h['close'].iloc[-1]) - float(df_h['close'].iloc[-20])) / float(df_h['close'].iloc[-20]) * 100
   sh_idx = ak.stock_zh_index_daily_em(symbol="sh000001")
   if not sh_idx.empty and len(sh_idx) >= 20:
   sh_c = (float(sh_idx['close'].iloc[-1]) - float(sh_idx['close'].iloc[-20])) / float(sh_idx['close'].iloc[-20]) * 100
   alpha = stock_c - sh_c
   personal_status_text += f"\n### 🚀 RPS 相对强弱跑赢率\n* 近20日个股涨幅: {stock_c:.2f}% | 同期上证: {sh_c:.2f}%\n* 超额收益(Alpha): {alpha:.2f}% ({'🔥远超大盘，妖股潜质' if alpha>10 else '🔴跑输大盘，弱势跟风'})\n"
   except: pass
   # 【A股核武 7：AI 昨日打脸回测雷达】
   try:
   report_files = glob.glob("reports/report_.md")
   report_files.sort()
   if report_files:
   with open(report_files[-1], 'r', encoding='utf-8') as f:
   past_content = f.read()
   stock_idx = past_content.find(f"({code})")
   if stock_idx != -1:
   snippet = past_content[stock_idx:stock_idx+400]
   personal_status_text += f"\n### 🪞 机构打脸回测雷达\ntext\n{snippet}...\n\n 💥 打脸复盘硬指令：如果当前走势与你看多的预测相反，导致主人亏钱，你必须在《风险提示》中以“【系统打脸反思】：”开头，无条件承认误判，并重新校准为防守模型！\n"
   except Exception as e: logger.debug(f"回测日志读取异常: {e}")
   except Exception as e:
   logger.error(f"底层特化引擎加载出现未能预料的异常: {e}")
   # ========== [微创手术结束] ==========
   fallback_hints = ""
   if cp_val:
   fallback_hints = f"7. 防N/A硬指令：如果缺乏支撑位数据，止损位必须填 {cp_val * 0.95:.2f}元，目标位填 {cp_val * 1.08:.2f}元，绝对不准输出 N/A！"
   prompt = f"""# A股顶级机构决策请求
   {personal_status_text}
📊 股票基础信息
| 项目 | 数据 |
|---|---|
| 股票代码 | {code} |
| 股票名称 | {stock_name} |
| 分析日期 | {context.get('date', '未知')} |
📈 技术面与量价数据
今日行情
| 收盘价 | 涨跌幅 | 成交量 | 成交额 |
|---|---|---|---|
| {today.get('close', 'N/A')} 元 | {today.get('pct_chg', 'N/A')}% | {self._format_volume(today.get('volume'))} | {self._format_amount(today.get('amount'))} |
均线系统 (若数据断层请执行降维防守)
| MA5 | MA10 | MA20 | 均线形态 |
|---|---|---|---|
| {today.get('ma5', 'N/A')} | {today.get('ma10', 'N/A')} | {today.get('ma20', 'N/A')} | {context.get('ma_status', '未知')} |
| """ |  |  |  |
if 'realtime' in context:
rt = context['realtime']
turnover = float(rt.get('turnover_rate', 0) if str(rt.get('turnover_rate')).replace('.','').isdigit() else 0)
to_alert = "💀死亡换手极度派发危险" if turnover > 25 else "⚠️游资高位剧烈博弈" if turnover > 15 else "活跃健康"
prompt += f"""
行情增强数据
 * 量比: {rt.get('volume_ratio', 'N/A')} | 换手率: {rt.get('turnover_rate', 'N/A')}% ({to_alert})
 * 市盈率(PE): {rt.get('pe_ratio', 'N/A')}
   """
   # 筹码分布处理（如果东财API获取失败，使用我们计算的 VWAP 兜底数据）
   chip_info = context.get('chip', {})
   if chip_info and 'profit_ratio' in chip_info:
   profit_ratio = chip_info.get('profit_ratio', 0)
   avg_cost = chip_info.get('avg_cost', 'N/A')
   source_str = "官方API获取"
   else:
   profit_ratio = (syn_profit_ratio / 100) if 'syn_profit_ratio' in locals() else 0
   avg_cost = f"{vwap_60:.2f}" if 'vwap_60' in locals() else 'N/A'
   source_str = "量化自算兜底(VWAP)"
   prompt += f"""
筹码分布（抛压测算 - 数据来源: {source_str}）
 * 获利比例: {profit_ratio:.1%} (70-90%时极易引发高位抛压)
 * 平均成本: {avg_cost} 元
   """
   if 'trend_analysis' in context:
   trend = context['trend_analysis']
   bias_warning = "🚨 超过5%，严禁追高！" if trend.get('bias_ma5', 0) > 5 else "✅ 乖离率处于安全范围"
   prompt += f"""
基础趋势测算
 * 乖离率(MA5): {trend.get('bias_ma5', 0):+.2f}% ({bias_warning})
   """
   # 【双新闻引擎排雷】
   prompt += """
📰 双引擎舆情情报 (排雷交叉验证)
"""
if news_context or announcement_context:
prompt += f"以下是 {stock_name}({code}) 近期的多源情报，请敏锐捕捉预期差与雷区：\n"
if news_context: prompt += f"### 🔍 引擎一：全网搜索舆情\ntext\n{news_context}\n\n"
if announcement_context: prompt += f"### 📢 引擎二：官方权威公告\ntext\n{announcement_context}\n\n"
prompt += "\n【防雷比对】：传闻若未被公告证实，提示炒作熄火风险；重点深挖公告中隐藏的【减持计划】与【业绩下修】。\n"
else:
prompt += "未搜索到近期相关新闻或公告，本轮分析纯粹依靠量化技术面、筹码与资金博弈。\n"
prompt += f"""
✅ 终极JSON生成任务
请为 {stock_name}({code}) 生成最终 JSON 格式的【决策仪表盘】。
必答核心检查点：
 * 妖股基因与缺口：结合龙虎榜、CCI超买、连板基因与跳空缺口，判定是主流妖股还是边缘跟风盘。注意死亡换手率派发风险！
 * ATR 网格战法：必须根据日均真实波幅，给出明确的 T+0 高抛低吸网格价差（在 grid_trading_plan 中）。
 * 量价诱多防线：OBV 资金潮汐与价格是否背离？MACD/KDJ极值发出了什么明确信号？
 * 量化仓位：请务必给出一个百分比仓位数字（如 15% 或 0%）。
 * 止损铁律：如果均线数据缺失，请直接按收盘价下方一定百分比测算防守位，不准留空。
 * 打脸追溯：如果提示了你昨天的判断错误，必须在反思字段中低头认错。
   {fallback_hints}
> 注意：请只输出合法的 JSON 字符串，不要携带 Markdown 格式代码块围栏，也不要附加多余解释。
> """
> 
return prompt
def _format_volume(self, volume: Optional[float]) -> str:
if volume is None: return 'N/A'
if volume >= 1e8: return f"{volume / 1e8:.2f} 亿股"
elif volume >= 1e4: return f"{volume / 1e4:.2f} 万股"
else: return f"{volume:.0f} 股"
def _format_amount(self, amount: Optional[float]) -> str:
if amount is None: return 'N/A'
if amount >= 1e8: return f"{amount / 1e8:.2f} 亿元"
elif amount >= 1e4: return f"{amount / 1e4:.2f} 万元"
else: return f"{amount:.0f} 元"
def _format_percent(self, value: Optional[float]) -> str:
if value is None: return 'N/A'
try: return f"{float(value):.2f}%"
except: return 'N/A'
def _format_price(self, value: Optional[float]) -> str:
if value is None: return 'N/A'
try: return f"{float(value):.2f}"
except: return 'N/A'
def _build_market_snapshot(self, context: Dict[str, Any]) -> Dict[str, Any]:
today = context.get('today', {}) or {}
realtime = context.get('realtime', {}) or {}
yesterday = context.get('yesterday', {}) or {}
prev_close = yesterday.get('close')
close = today.get('close')
high = today.get('high')
low = today.get('low')
amplitude = change_amount = None
if prev_close not in (None, 0) and high is not None and low is not None:
try: amplitude = (float(high) - float(low)) / float(prev_close) * 100
except: pass
if prev_close is not None and close is not None:
try: change_amount = float(close) - float(prev_close)
except: pass
snapshot = {
"date": context.get('date', '未知'),
"close": self._format_price(close),
"open": self._format_price(today.get('open')),
"high": self._format_price(high),
"low": self._format_price(low),
"prev_close": self._format_price(prev_close),
"pct_chg": self._format_percent(today.get('pct_chg')),
"change_amount": self._format_price(change_amount),
"amplitude": self._format_percent(amplitude),
"volume": self._format_volume(today.get('volume')),
"amount": self._format_amount(today.get('amount')),
}
if realtime:
snapshot.update({
"price": self._format_price(realtime.get('price')),
"volume_ratio": realtime.get('volume_ratio', 'N/A'),
"turnover_rate": self._format_percent(realtime.get('turnover_rate')),
})
return snapshot
def _parse_response(self, response_text: str, code: str, name: str) -> AnalysisResult:
try:
# 增强型 JSON 解析护盾，剥离对话废话
cleaned_text = response_text
# 尝试正则捕获最外层的 {}
json_match = re.search(r'({.*})', cleaned_text, re.DOTALL)
if json_match:
json_str = json_match.group(1)
else:
json_str = cleaned_text
json_str = self._fix_json_string(json_str)
data = json.loads(json_str)
dashboard = data.get('dashboard', None)
debate_process = data.get('debate_process', None)
ai_stock_name = data.get('stock_name')
if ai_stock_name and (name.startswith('股票') or name == code or 'Unknown' in name):
name = ai_stock_name
decision_type = data.get('decision_type', '')
if not decision_type:
op = data.get('operation_advice', '持有')
if op in ['买入', '加仓', '强烈买入']: decision_type = 'buy'
elif op in ['卖出', '减仓', '强烈卖出']: decision_type = 'sell'
else: decision_type = 'hold'
return AnalysisResult(
code=code,
name=name,
sentiment_score=int(data.get('sentiment_score', 50)),
trend_prediction=data.get('trend_prediction', '震荡'),
operation_advice=data.get('operation_advice', '持有'),
decision_type=decision_type,
confidence_level=data.get('confidence_level', '中'),
debate_process=debate_process,
dashboard=dashboard,
trend_analysis=data.get('trend_analysis', ''),
short_term_outlook=data.get('short_term_outlook', ''),
medium_term_outlook=data.get('medium_term_outlook', ''),
technical_analysis=data.get('technical_analysis', ''),
ma_analysis=data.get('ma_analysis', ''),
volume_analysis=data.get('volume_analysis', ''),
pattern_analysis=data.get('pattern_analysis', ''),
fundamental_analysis=data.get('fundamental_analysis', ''),
sector_position=data.get('sector_position', ''),
company_highlights=data.get('company_highlights', ''),
news_summary=data.get('news_summary', ''),
market_sentiment=data.get('market_sentiment', ''),
hot_topics=data.get('hot_topics', ''),
analysis_summary=data.get('analysis_summary', '分析完成'),
key_points=data.get('key_points', ''),
risk_warning=data.get('risk_warning', ''),
buy_reason=data.get('buy_reason', ''),
search_performed=data.get('search_performed', False),
data_sources=data.get('data_sources', '技术面数据'),
success=True,
)
except json.JSONDecodeError as e:
logger.warning(f"JSON 解析失败 (大模型格式错乱): {e}，触发备用纯文本解析")
return self._parse_text_response(response_text, code, name)
def _fix_json_string(self, json_str: str) -> str:
# 移除行内注释
json_str = re.sub(r'//.?\n', '\n', json_str)
# 移除多行注释
json_str = re.sub(r'/*.?*/', '', json_str, flags=re.DOTALL)
# 修复非法的尾随逗号 (对象和数组)
json_str = re.sub(r',\s*}', '}', json_str)
json_str = re.sub(r',\s*]', ']', json_str)
# 统一布尔值
json_str = json_str.replace('True', 'true').replace('False', 'false')
# 终极修复：依赖 json_repair 强行补齐括号与转移引号
json_str = repair_json(json_str)
return json_str
def _parse_text_response(self, response_text: str, code: str, name: str) -> AnalysisResult:
sentiment_score = 50
trend = '震荡'
advice = '持有'
text_lower = response_text.lower()
positive_count = sum(1 for kw in ['看多', '买入', '上涨', '突破', '强势'] if kw in text_lower)
negative_count = sum(1 for kw in ['看空', '卖出', '下跌', '跌破', '弱势'] if kw in text_lower)
if positive_count > negative_count + 1:
sentiment_score, trend, advice, decision_type = 65, '看多', '买入', 'buy'
elif negative_count > positive_count + 1:
sentiment_score, trend, advice, decision_type = 35, '看空', '卖出', 'sell'
else:
decision_type = 'hold'
return AnalysisResult(
code=code, name=name, sentiment_score=sentiment_score, trend_prediction=trend,
operation_advice=advice, decision_type=decision_type, confidence_level='低',
analysis_summary=response_text[:500] if response_text else '无分析结果',
key_points='大模型 JSON 输出越界或破损，触发安全模式。', risk_warning='建议查阅原始响应文本。',
raw_response=response_text, success=True,
)
def batch_analyze(
self, contexts: List[Dict[str, Any]], news_contexts: Optional[List[Optional[str]]] = None,
announcement_contexts: Optional[List[Optional[str]]] = None, delay_between: float = 2.0
) -> List[AnalysisResult]:
results = []
for i, context in enumerate(contexts):
if i > 0: time.sleep(delay_between)
nc = news_contexts[i] if news_contexts and i < len(news_contexts) else None
ac = announcement_contexts[i] if announcement_contexts and i < len(announcement_contexts) else None
results.append(self.analyze(context, news_context=nc, announcement_context=ac))
return results
def get_analyzer() -> GeminiAnalyzer:
return GeminiAnalyzer()

