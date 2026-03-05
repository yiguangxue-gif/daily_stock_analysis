# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (A股超神特化·彻底消灭N/A版)
===================================

职责：
1. 封装 Gemini API 调用逻辑 (附带 OpenAI/Claude 无缝备用)
2. 利用 Google Search Grounding 获取实时新闻 (双引擎交叉验证)
3. 【A股特化】龙虎榜追踪、连板基因、OBV能量潮、CCI妖股雷达、大盘宏观水温
4. 【最新加强】跳空缺口、日内K线实体动能、MA60牛熊分界、筹码集中度变盘雷达
5. 【终极防N/A】本地强算 MA5/10/20/60、量比与筹码成本，彻底根治面板显示 N/A 问题！
6. 【抗断网引擎】自研 VWAP 筹码分布测算兜底算法，无视 API 频繁断网。
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
    """AI 分析结果数据类 - 决策仪表盘版"""
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
    user_cost: Optional[float] = None
    user_shares: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code, 'name': self.name, 'sentiment_score': self.sentiment_score,
            'trend_prediction': self.trend_prediction, 'operation_advice': self.operation_advice,
            'decision_type': self.decision_type, 'confidence_level': self.confidence_level,
            'debate_process': self.debate_process, 'dashboard': self.dashboard,  
            'trend_analysis': self.trend_analysis, 'short_term_outlook': self.short_term_outlook,
            'medium_term_outlook': self.medium_term_outlook, 'technical_analysis': self.technical_analysis,
            'ma_analysis': self.ma_analysis, 'volume_analysis': self.volume_analysis,
            'pattern_analysis': self.pattern_analysis, 'fundamental_analysis': self.fundamental_analysis,
            'sector_position': self.sector_position, 'company_highlights': self.company_highlights,
            'news_summary': self.news_summary, 'market_sentiment': self.market_sentiment,
            'hot_topics': self.hot_topics, 'analysis_summary': self.analysis_summary,
            'key_points': self.key_points, 'risk_warning': self.risk_warning, 'buy_reason': self.buy_reason,
            'market_snapshot': self.market_snapshot, 'search_performed': self.search_performed,
            'success': self.success, 'error_message': self.error_message,
            'current_price': self.current_price, 'change_pct': self.change_pct,
            'user_cost': self.user_cost, 'user_shares': self.user_shares,
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
    """Gemini AI 分析器 (A股工业级防N/A特化版)"""

    SYSTEM_PROMPT = """你是一位深谙中国A股“资金市”、“情绪市”与“龙头战法”的顶级游资操盘手兼风控总监。

## 🛑 反偷懒与绝对量化协议 - 优先级最高
你的字典里没有 "N/A"、"暂无"、"未知"。系统已经在 Prompt 中为你准备了所有精算好的 MA5/MA10/MA20/MA60 和阻力支撑位，你必须：
1. 完整且精确输出 JSON 中要求的 `ma5`、`ma10`、`ma20`、`support_level`、`resistance_level`、`volume_ratio` 等字段的数值！绝对禁止在数值字段填入 "N/A" 或空字符串。
2. 仓位策略必须是一个明确的百分比(如 `20%` 或 `0%`)。
3. 成本分析：即使系统没有获取到用户的成本数据，你也要假定用户是“空仓准备买入”的状态。

## 🧠 A股专属思维链 (CoT) 推演
在输出结论前，必须在 `debate_process` 中展现三位A股顶尖专家的内部推演：
1. 【打板接力客】：寻找妖股基因。分析该股是否上了龙虎榜、换手率是否活跃、是否有跳空缺口或筹码高度集中变盘信号。
2. 【公募风控总监】：专挑刺防诱多。紧盯 MA60牛熊线、OBV量价背离、KDJ极值、日内K线长上影线（派发）、以及大盘宏观冰点。
3. 【铁血总舵主】：结合主人的【私人持仓成本】和【打脸回测雷达】进行终审。如果套牢，严格按 ATR 给割肉位；如果被打脸，必须认错！

## A股终极量化法则

### 1. 股性、缺口与筹码变盘
- 向上跳空缺口：极强进攻信号，缺口不补坚定看多。向下跳空直接判死刑。
- 筹码集中度异动：当价格波动率(CV)极低时，说明筹码高度集中，面临暴力变盘（向上或向下突破）。
- 换手率判定：>25%高度警惕见顶派发；8%-15%为健康游资接力活跃区；<3%说明是死鱼跟风盘。

### 2. 量价识破诱多 (Volume-Price & OBV)
- A股主力最喜欢拉高骗炮。如果日内K线实体是“大阴线”或“长上影线”，且 OBV 能量潮往下走，必须判定为【诱多出货】，强烈建议减仓！
- 跌破 MA60（牛熊分界线）的任何反弹都叫“逃命波”，严禁重仓。

### 3. ATR 波动率与网格自救
- A股震荡市或深套自救时，必须启用 T+0 网格战法。利用系统提供的 ATR（日均真实波幅），设计网格买卖间距。

### 4. 极端大盘情绪一票否决
- 倾巢之下无完卵。如果提供的大盘赚钱效应低于 20%（极度冰点），不论个股图形多好看，必须把评级下调至【观望/减仓】！

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出，不要遗漏哪怕一个字段！确保是标准 JSON 字符串！

```json
{
    "stock_name": "股票中文名称",
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "confidence_level": "高/中/低",

    "debate_process": {
        "hot_money_trader": "打板接力客发言(查股性/筹码变盘/缺口/资金)：...",
        "risk_director": "风控总监发言(查MA60/日内实体/OBV背离/大盘)：...",
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
                "gap_and_trend": "跳空缺口情况及MA60牛熊状态",
                "chip_concentration": "筹码集中度与变盘信号解读"
            },
            "indicator_trinity": {
                "macd_status": "金叉/死叉/顶背离/底背离",
                "kdj_cci_status": "KDJ与CCI极值解读",
                "boll_status": "突破上轨/跌破中轨等"
            },
            "price_position": {
                "current_price": 0.00,
                "ma5": 0.00,
                "ma10": 0.00,
                "ma20": 0.00,
                "bias_ma5": 0.00,
                "bias_status": "安全/警戒/危险",
                "support_level": "具体价格数值(基于MA10或筹码峰推算)",
                "resistance_level": "具体价格数值(基于POC套牢峰推算)"
            },
            "volume_analysis": {
                "volume_ratio": 0.00,
                "turnover_rate": 0.00,
                "volume_status": "放量/缩量/平量",
                "obv_trend": "OBV能量潮与日内K线实体多空判定"
            }
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
            "ideal_buy": "XX元（严禁填N/A）",
            "secondary_buy": "XX元",
            "trailing_stop": "XX元（基于ATR波幅计算的防守位，严禁填N/A）",
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
            "✅/⚠️/❌ 具备涨停基因且资金活跃",
            "✅/⚠️/❌ 换手率健康未现高位派发",
            "✅/⚠️/❌ 站上MA60且无OBV顶背离",
            "✅/⚠️/❌ A股宏观水温正常，非连环冰点"
        ]
    },

    "analysis_summary": "100字A股实战深度总结（若昨日判断亏损，必须在此深刻检讨认错）",
    "key_points": "3-5个核心看点",
    "risk_warning": "退市/减持/天地板风险提示",
    "buy_reason": "冷血操作理由"
}
```
"""

    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self._api_key = api_key or config.gemini_api_key
        self._model = None
        self._current_model_name = None
        self._using_fallback = False
        self._use_openai = False
        self._use_anthropic = False
        self._openai_client = None
        self._anthropic_client = None

        gemini_key_valid = self._api_key and not self._api_key.startswith('your_') and len(self._api_key) > 10

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

    def _init_anthropic_fallback(self) -> None:
        config = get_config()
        anthropic_key_valid = (
            config.anthropic_api_key
            and not config.anthropic_api_key.startswith('your_')
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

    def _init_openai_fallback(self) -> None:
        config = get_config()
        openai_key_valid = (
            config.openai_api_key and
            not config.openai_api_key.startswith('your_') and
            len(config.openai_api_key) > 10
        )
        if not openai_key_valid:
            return
        try:
            from openai import OpenAI
            client_kwargs = {"api_key": config.openai_api_key}
            if config.openai_base_url and config.openai_base_url.startswith('http'):
                client_kwargs["base_url"] = config.openai_base_url
            
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
            logger.warning(f"🔄 [Gemini] 降级切换到备选模型: {fallback_model}")
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
                    time.sleep(min(base_delay * (2 ** (attempt - 1)), 60))
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
                if attempt == max_retries - 1: raise
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
            if payload: kwargs["extra_body"] = payload
            return kwargs

        if not hasattr(self, "_token_param_mode"): self._token_param_mode = {}
        max_output_tokens = generation_config.get('max_output_tokens', 8192)
        model_name = self._current_model_name
        mode = self._token_param_mode.get(model_name, "max_tokens")

        def _kwargs_with_mode(mode_value):
            kwargs = _build_base_request_kwargs()
            if mode_value is not None: kwargs[mode_value] = max_output_tokens
            return kwargs

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    time.sleep(min(base_delay * (2 ** (attempt - 1)), 60))
                try:
                    response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
                except Exception as e:
                    error_str = str(e).lower()
                    if mode == "max_tokens" and "unsupported param" in error_str:
                        mode = "max_completion_tokens"
                        self._token_param_mode[model_name] = mode
                        response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
                    elif mode == "max_completion_tokens" and "unsupported param" in error_str:
                        mode = None
                        self._token_param_mode[model_name] = mode
                        response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
                    else: raise

                if response and response.choices and response.choices[0].message.content:
                    return response.choices[0].message.content
                else: raise ValueError("OpenAI API 返回空响应")
            except Exception as e:
                if attempt == max_retries - 1: raise
        raise Exception("OpenAI API failed")
    
    def _call_api_with_retry(self, prompt: str, generation_config: dict) -> str:
        if self._use_anthropic:
            try: return self._call_anthropic_api(prompt, generation_config)
            except Exception as e:
                if self._openai_client: return self._call_openai_api(prompt, generation_config)
                raise e

        if self._use_openai:
            return self._call_openai_api(prompt, generation_config)

        config = get_config()
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
                if response and response.text: return response.text
                raise ValueError("Gemini 返回空响应")
            except Exception as e:
                last_error = e
                error_str = str(e)
                is_rate_limit = '429' in error_str or 'quota' in error_str.lower() or 'rate' in error_str.lower()
                if is_rate_limit:
                    logger.warning(f"[Gemini] API 限流 (429)，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:150]}")
                    match = re.search(r'retry in (\d+\.?\d*)s', error_str)
                    delay = float(match.group(1)) + 5.0 if match else min(base_delay * (2 ** attempt), 60)
                    if attempt >= max_retries // 2 and not tried_fallback:
                        if self._switch_to_fallback_model(): tried_fallback = True
                    logger.info(f"⏳ [Gemini] 强制休眠等待 API 恢复 {delay:.2f} 秒...")
                    time.sleep(delay)
                else:
                    logger.warning(f"[Gemini] API 调用失败，第 {attempt + 1}/{max_retries} 次尝试: {error_str[:100]}")
                    time.sleep(min(base_delay * (2 ** attempt), 30))
        
        if self._anthropic_client:
            try: return self._call_anthropic_api(prompt, generation_config)
            except Exception as e:
                if self._openai_client:
                    try: return self._call_openai_api(prompt, generation_config)
                    except Exception as e2: raise last_error or e or e2
                raise last_error or e

        if self._openai_client:
            try: return self._call_openai_api(prompt, generation_config)
            except Exception as e: raise last_error or e

        if config.anthropic_api_key and not self._anthropic_client:
            self._init_anthropic_fallback()
            if self._anthropic_client:
                try: return self._call_anthropic_api(prompt, generation_config)
                except Exception as e: raise last_error or e
                    
        if config.openai_api_key and not self._openai_client:
            self._init_openai_fallback()
            if self._openai_client:
                try: return self._call_openai_api(prompt, generation_config)
                except Exception as e: raise last_error or e

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
        if request_delay > 0: time.sleep(request_delay)
        
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
            prompt = self._format_prompt(context, name, news_context, announcement_context)
            generation_config = {"temperature": config.gemini_temperature, "max_output_tokens": 8192}
            response_text = self._call_api_with_retry(prompt, generation_config)
            
            result = self._parse_response(response_text, code, name)
            result.raw_response = response_text
            result.search_performed = bool(news_context or announcement_context)
            result.market_snapshot = self._build_market_snapshot(context)
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

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            if isinstance(val, (int, float)): return float(val)
            if isinstance(val, str): return float(val.replace(',', '').replace('%', '').strip())
        except: pass
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
        rt = context.get('realtime', {})
        curr_price = today.get('close')
        cp_val = self._safe_float(curr_price)
        
        # =========================================================================
        # 🛡️ [底层硬核强算引擎] 专门对付缺失数据、停牌和除零错误，彻底消灭 N/A
        # =========================================================================
        personal_status_text = "\n## 💎 A股天网级强算数据中心\n"
        
        calc_ma5 = calc_ma10 = calc_ma20 = calc_ma60 = calc_vr = 0.0
        vwap_60 = syn_profit_ratio = current_atr = poc_price = 0.0
        gap_str = ma5_trend = cci_status = obv_status = kdj_status = boll_status = macd_status = gene_str = cv_status = ma60_status = k_body_status = "未知"
        
        df_hist = pd.DataFrame()
        if 'history' in context and len(context['history']) > 0:
            try:
                # 使用近 120 天数据确保长周期指标 (MA60) 充足
                df_hist = pd.DataFrame(context['history']).tail(120)
                for col in ['close', 'high', 'low', 'open', 'volume', 'pct_chg']:
                    if col in df_hist.columns:
                        df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce').fillna(method='ffill').fillna(0)
                
                s_prices = df_hist['close']
                s_vol = df_hist['volume']
                
                if len(s_prices) > 0:
                    # 强算均线兜底
                    calc_ma5 = s_prices.rolling(5, min_periods=1).mean().iloc[-1]
                    calc_ma10 = s_prices.rolling(10, min_periods=1).mean().iloc[-1]
                    calc_ma20 = s_prices.rolling(20, min_periods=1).mean().iloc[-1]
                    calc_ma60 = s_prices.rolling(60, min_periods=1).mean().iloc[-1]
                    
                    # 强算量比兜底
                    if len(s_vol) >= 6:
                        vol_5d = s_vol.iloc[-6:-1].mean()
                        calc_vr = (s_vol.iloc[-1] / vol_5d) if vol_5d > 0 else 1.0
                    else:
                        calc_vr = 1.0
                        
                    # 日内K线实体多空判定 (防诱多假突破)
                    open_p = df_hist['open'].iloc[-1]
                    k_body_pct = ((cp_val - open_p) / open_p * 100) if open_p and cp_val else 0
                    k_body_status = "🔴实体大阳(日内做多坚决)" if k_body_pct > 2 else "🟢实体大阴或长上影(日内抛压极大)" if k_body_pct < -2 else "⚪实体较小(多空平衡)"
                    
                    # 筹码集中度变盘雷达 (波动率极小意味筹码集中面临变盘)
                    price_std_20 = s_prices.tail(20).std()
                    price_mean_20 = s_prices.tail(20).mean()
                    cv_20 = (price_std_20 / price_mean_20) * 100 if price_mean_20 > 0 else 0
                    cv_status = "🎯筹码高度集中(波动率极小，极易暴力变盘)" if cv_20 < 5 else "💥筹码极度发散(分歧巨大)" if cv_20 > 15 else "正常换手区间"

                    # MA60 牛熊线
                    ma60_status = "🐂站上牛熊分界线(MA60)" if cp_val and cp_val > calc_ma60 else "🐻跌破牛熊分界线(MA60，只抢反弹不重仓)"

                    # 强算 VWAP 筹码兜底
                    total_vol = s_vol.sum()
                    if total_vol > 0:
                        vwap_60 = (s_prices * s_vol).sum() / total_vol
                        if cp_val:
                            profit_vol = df_hist[df_hist['close'] <= cp_val]['volume'].sum()
                            syn_profit_ratio = (profit_vol / total_vol) * 100

                    # 妖股连板基因
                    df_hist_15 = df_hist.tail(15)
                    if 'pct_chg' in df_hist_15.columns:
                        zt_count = len(df_hist_15[df_hist_15['pct_chg'] >= 9.5])
                        gene_str = "🔥股性极度活跃(具备妖股基因)" if zt_count >= 2 else "🌟有异动基因(主力试盘)" if zt_count == 1 else "🧊股性沉闷(不适合接力)"

                    # 跳空缺口雷达
                    if len(df_hist) >= 2:
                        yesterday_high = df_hist['high'].iloc[-2]
                        yesterday_low = df_hist['low'].iloc[-2]
                        today_low = df_hist['low'].iloc[-1]
                        today_high = df_hist['high'].iloc[-1]
                        gap_str = "无明显跳空缺口"
                        if today_low > yesterday_high:
                            gap_str = f"🚀 向上跳空缺口 ({yesterday_high:.2f}-{today_low:.2f}) [极强进攻看多信号]"
                        elif today_high < yesterday_low:
                            gap_str = f"🕳️ 向下跳空缺口 ({today_high:.2f}-{yesterday_low:.2f}) [极度弱势破位信号]"

                    # 均线斜率探测
                    ma5_series = s_prices.rolling(5, min_periods=1).mean()
                    ma5_trend = "↗️ 拐头向上 (短线做多)" if len(ma5_series)>=2 and ma5_series.iloc[-1] > ma5_series.iloc[-2] else "↘️ 拐头向下 (短线承压)"

                    # CCI 妖股顺势雷达
                    if 'high' in df_hist.columns and 'low' in df_hist.columns:
                        tp = (df_hist['high'] + df_hist['low'] + s_prices) / 3
                        ma_tp = tp.rolling(14, min_periods=1).mean()
                        md = tp.rolling(14, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
                        cci = (tp - ma_tp) / (0.015 * md.replace(0, 1e-9))
                        cci_val = cci.iloc[-1]
                        cci_status = "🔥异动妖股主升浪(>100)" if cci_val > 100 else "🥶超跌错杀(<-100)" if cci_val < -100 else "震荡区"

                    # OBV 量能潮汐背离
                    df_hist['obv_sign'] = np.sign(s_prices.diff().fillna(0))
                    df_hist['OBV'] = (df_hist['obv_sign'] * df_hist['volume']).cumsum()
                    if len(df_hist) >= 5:
                        obv_trend = df_hist['OBV'].iloc[-1] - df_hist['OBV'].iloc[-5]
                        obv_status = "🌊资金真实吸筹(OBV向好)" if obv_trend > 0 else "🩸警惕放量滞涨诱多(OBV背离)"

                    # KDJ 极值护盾 (防除零)
                    low_min9 = s_prices.rolling(9, min_periods=1).min()
                    high_max9 = s_prices.rolling(9, min_periods=1).max()
                    denom = (high_max9 - low_min9).replace(0, 1e-9)
                    rsv = (s_prices - low_min9) / denom * 100
                    k = rsv.ewm(com=2, adjust=False).mean()
                    d = k.ewm(com=2, adjust=False).mean()
                    j = 3 * k - 2 * d
                    j_val = j.iloc[-1]
                    kdj_status = "💡黄金坑超卖 (J<0)" if j_val < 0 else ("⚠️严重超买钝化 (J>100)" if j_val > 100 else "安全区")

                    # BOLL 轨道
                    ma20 = s_prices.rolling(20, min_periods=1).mean()
                    std20 = s_prices.rolling(20, min_periods=1).std().fillna(0)
                    upper = ma20 + 2 * std20
                    lower = ma20 - 2 * std20
                    boll_status = "🚀突破上轨(强动能或极易被砸)" if cp_val and cp_val > upper.iloc[-1] else ("🕳️跌破下轨(弱势期)" if cp_val and cp_val < lower.iloc[-1] else "通道内运行")

                    # MACD 动能
                    exp1 = s_prices.ewm(span=12, adjust=False).mean()
                    exp2 = s_prices.ewm(span=26, adjust=False).mean()
                    macd = exp1 - exp2
                    signal = macd.ewm(span=9, adjust=False).mean()
                    hist = macd - signal
                    macd_status = "🔴死叉向下" if hist.iloc[-1] < 0 else "🟢金叉向上"

                    # ATR 真实波幅
                    highs = df_hist['high']
                    lows = df_hist['low']
                    tr = pd.concat([highs - lows, (highs - s_prices.shift()).abs(), (lows - s_prices.shift()).abs()], axis=1).max(axis=1)
                    atr14 = tr.rolling(14, min_periods=1).mean()
                    current_atr = atr14.iloc[-1]

                    # POC 筹码峰
                    if s_prices.nunique() > 1:
                        df_hist['price_bin'] = pd.cut(s_prices, bins=12, duplicates='drop')
                        vp = df_hist.groupby('price_bin', observed=False)['volume'].sum()
                        poc_price = vp.idxmax().mid
                    else:
                        poc_price = cp_val if cp_val else 0.0

            except Exception as e: logger.debug(f"量价底座计算轻微异常 (可能新股或停牌): {e}")
            
            # --- 确保传入 LLM 的数据绝对不为 N/A ---
            t_ma5 = today.get('ma5') if today.get('ma5') not in [None, 'N/A', ''] else f"{calc_ma5:.2f}"
            t_ma10 = today.get('ma10') if today.get('ma10') not in [None, 'N/A', ''] else f"{calc_ma10:.2f}"
            t_ma20 = today.get('ma20') if today.get('ma20') not in [None, 'N/A', ''] else f"{calc_ma20:.2f}"
            rt_vr = rt.get('volume_ratio') if rt.get('volume_ratio') not in [None, 'N/A', ''] else f"{calc_vr:.2f}"

            trend_info = context.get('trend_analysis', {})
            bias_ma5 = trend_info.get('bias_ma5')
            if bias_ma5 in [None, 'N/A', ''] and cp_val and calc_ma5 > 0:
                bias_ma5 = (cp_val - calc_ma5) / calc_ma5 * 100
            bias_ma5_str = f"{float(bias_ma5):+.2f}%" if bias_ma5 not in [None, 'N/A', ''] else "0.00%"

            # 【A股核武 1：大盘情绪一票否决】
            try:
                spot_df = ak.stock_zh_a_spot_em()
                if not spot_df.empty:
                    up_count = len(spot_df[spot_df['涨跌幅'] > 0])
                    down_count = len(spot_df[spot_df['涨跌幅'] < 0])
                    if (up_count + down_count) > 0:
                        up_ratio = up_count / (up_count + down_count) * 100
                        temp_str = "🌋 逼空贪婪" if up_ratio > 80 else "🥶 恐慌冰点" if up_ratio < 20 else "😐 情绪中性"
                        personal_status_text += f"### 📉 大盘宏观水温\n* **赚钱效应**：{up_ratio:.1f}% ({temp_str})\n"
            except: pass

            # 【A股核武 2：云端仓位算账】
            csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv"
            my_cost, my_shares = None, None
            try:
                req = urllib.request.Request(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=10) as res:
                    for row in csv.reader(io.StringIO(res.read().decode('utf-8-sig'))):
                        if len(row) >= 2 and row[0].strip() and ''.join(filter(str.isdigit, str(row[0]))).zfill(6) == code:
                            my_cost = float(str(row[1]).replace(',', '').strip())
                            my_shares = int(float(str(row[2]).replace(',', '').strip())) if len(row) >= 3 and row[2].strip() else 0
            except: pass

            if my_cost and cp_val:
                context['user_cost'] = my_cost
                context['user_shares'] = my_shares
                profit_pct = ((cp_val - (my_cost if my_cost>0 else 1e-9)) / (my_cost if my_cost>0 else 1e-9)) * 100
                personal_status_text += f"### 💰 机构持仓底牌\n* 成本价：{my_cost:.2f} 元 | 盈亏：{profit_pct:.2f}% ({'🔴套牢' if profit_pct < 0 else '🟢吃肉'})\n* 🚨 必须在 `personal_cost_review` 输出应对策略。\n\n"

            # 【A股核武 3：主力资金与龙虎榜】
            try:
                fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
                flow_desc = f"内资主力: {fund_flow.iloc[-1]['主力净流入-净额']/10000:.1f}万"
                try:
                    hk_funds = ak.stock_hsgt_stock_statistics_em()
                    my_hk = hk_funds[hk_funds['代码'] == code]
                    if not my_hk.empty:
                        flow_desc += f" | 北向资金: {'🟢流入' if my_hk.iloc[0]['今日增持估计-市值'] > 0 else '🔴砸盘流出'}"
                except: pass
                
                lhb_desc = "暂无数据"
                try:
                    end_date_str = datetime.now().strftime('%Y%m%d')
                    start_date_str = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
                    lhb_df = ak.stock_lhb_detail_em(start_date=start_date_str, end_date=end_date_str)
                    if not lhb_df.empty:
                        my_lhb = lhb_df[lhb_df['代码'] == code]
                        lhb_desc = f"🚨 近10日登榜 {len(my_lhb)} 次(有游资足迹)" if not my_lhb.empty else "🧊 未上榜跟风盘"
                except: pass
                personal_status_text += f"### 🌊 资金博弈与游资雷达\n* {flow_desc} | {lhb_desc}\n\n"
            except: pass

            # 组装强算特征树
            personal_status_text += f"""### 🎯 A股核心量化指标 (强算兜底)
* **K线日内多空**: {k_body_status}
* **MA60牛熊分界**: {ma60_status}
* **筹码变盘雷达**: {cv_status} (基于CV偏离值计算)
* **涨停基因(近15日)**: {gene_str}
* **缺口与均线雷达**: {gap_str} | MA5斜率: {ma5_trend}
* **CCI 顺势雷达**: {cci_status}
* **OBV 资金潮汐**: {obv_status} (警惕放量滞涨与长上影线)
* **KDJ 情绪极值**: {kdj_status}
* **BOLL 布林测压**: {boll_status}
* **ATR 真实波幅**: {current_atr:.2f}元 (网格T+0与防洗盘止损核心依据)
* **POC 历史筹码峰**: 约 {poc_price:.2f}元 (最强阻力位/支撑位)
"""

            # 【A股核武 6：RPS 超额收益】
            try:
                if 'history' in context and len(context['history']) >= 20:
                    df_h = pd.DataFrame(context['history'])
                    stock_c = (float(df_h['close'].iloc[-1]) - float(df_h['close'].iloc[-20])) / float(df_h['close'].iloc[-20]) * 100
                    sh_idx = ak.stock_zh_index_daily_em(symbol="sh000001")
                    if not sh_idx.empty and len(sh_idx) >= 20:
                        sh_c = (float(sh_idx['close'].iloc[-1]) - float(sh_idx['close'].iloc[-20])) / float(sh_idx['close'].iloc[-20]) * 100
                        alpha = stock_c - sh_c
                        personal_status_text += f"\n### 🚀 RPS 相对强弱跑赢率\n* **超额收益(Alpha)**: {alpha:.2f}% ({'🔥远超大盘' if alpha>10 else '🔴弱势跟风'})\n"
            except: pass

            # 【A股核武 7：AI 打脸回测雷达】
            try:
                report_files = glob.glob("reports/report_*.md")
                report_files.sort()
                if report_files:
                    with open(report_files[-1], 'r', encoding='utf-8') as f:
                        past_content = f.read()
                    stock_idx = past_content.find(f"({code})")
                    if stock_idx != -1:
                        snippet = past_content[stock_idx:stock_idx+400]
                        personal_status_text += f"\n### 🪞 机构打脸回测雷达\n```text\n{snippet}...\n```\n* 💥 打脸复盘硬指令：如果当前走势与你看多的预测相反，你必须在《风险提示》中认错！\n"
            except: pass

        except Exception as e:
            logger.error(f"底层特化引擎异常: {e}")
        # ========== [微创手术结束] ==========
        
        fallback_hints = ""
        if cp_val:
            fallback_hints = f"7. 防N/A硬指令：即便你对某些数据不确定，在 JSON 中的 `support_level` 必须填入具体价格 (如 `{cp_val * 0.95:.2f}`)，`resistance_level` (如 `{cp_val * 1.08:.2f}`)，`ma5/ma10/ma20` 必须输出具体的数值。绝对不准在数值型或价格字段输出 N/A！"

        # 筹码分布处理（东财API挂了就用VWAP）
        chip_info = context.get('chip', {})
        if chip_info and 'profit_ratio' in chip_info:
            profit_ratio = chip_info.get('profit_ratio', 0)
            avg_cost = chip_info.get('avg_cost', 'N/A')
            source_str = "官方API获取"
        else:
            profit_ratio = (syn_profit_ratio / 100) if 'syn_profit_ratio' in locals() else 0
            avg_cost = f"{vwap_60:.2f}" if 'vwap_60' in locals() else 'N/A'
            source_str = "量化自算兜底(VWAP)"

        prompt = f"""# A股顶级机构决策请求
{personal_status_text} 

## 📊 股票基础信息
| 股票代码 | 股票名称 | 分析日期 |
|------|------|------|
| {code} | {stock_name} | {context.get('date', '未知')} |

---

## 📈 技术面与量价数据 (防断网强算修复版)

### 今日行情
| 收盘价 | 涨跌幅 | 成交量 | 成交额 |
|------|------|------|------|
| {today.get('close', 'N/A')} 元 | {today.get('pct_chg', 'N/A')}% | {self._format_volume(today.get('volume'))} | {self._format_amount(today.get('amount'))} |

### 均线系统 
| MA5 | MA10 | MA20 | 均线形态 |
|------|------|------|------|
| {t_ma5} | {t_ma10} | {t_ma20} | {context.get('ma_status', '未知')} |

### 行情增强数据
- 量比: {rt_vr} | 换手率: {rt.get('turnover_rate', 'N/A')}%
- 筹码分布({source_str}): 获利比例 {profit_ratio:.1%} | 平均成本 {avg_cost} 元
- 乖离率(MA5): {bias_ma5_str}
"""
        
        # 【双新闻引擎排雷】
        prompt += """
---
## 📰 双引擎舆情情报 (排雷交叉验证)
"""
        if news_context or announcement_context:
            prompt += f"以下是 {stock_name}({code}) 近期的多源情报，请敏锐捕捉预期差与雷区：\n"
            if news_context: prompt += f"### 🔍 引擎一：全网搜索舆情\n```text\n{news_context}\n```\n"
            if announcement_context: prompt += f"### 📢 引擎二：官方权威公告\n```text\n{announcement_context}\n```\n"
        else:
            prompt += "未搜索到近期相关新闻或公告，本轮分析纯粹依靠量化技术面与资金博弈。\n"

        prompt += f"""
---
## ✅ 终极JSON生成任务

请为 {stock_name}({code}) 生成最终 JSON 格式的【决策仪表盘】。

### 必答核心检查点：
1. 妖股基因与变盘：结合筹码集中度(CV)、跳空缺口、龙虎榜判定是主流妖股还是边缘跟风盘。
2. 量价实体诱多防线：日内是否长上影线或大阴线？OBV 资金潮汐是否背离？跌破 MA60 必须减仓！
3. ATR 网格战法：必须根据提供的真实波幅，给出明确的 T+0 高抛低吸网格价差（在 `grid_trading_plan` 中）。
4. 量化仓位：必须给出一个百分比仓位数字（如 15% 或 0%）。
{fallback_hints}

> 注意：请只输出合法的 JSON 字符串，不要携带 Markdown 格式代码块围栏，也不要附加多余解释。不要输出任何 N/A 字符。
"""
        
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
            cleaned_text = response_text
            json_match = re.search(r'(\{.*\})', cleaned_text, re.DOTALL)
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
        json_str = re.sub(r'//.*?\n', '\n', json_str)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        json_str = json_str.replace('True', 'true').replace('False', 'false')
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
