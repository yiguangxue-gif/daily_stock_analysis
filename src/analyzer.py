# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层
===================================

职责：
1. 封装 Gemini API 调用逻辑
2. 利用 Google Search Grounding 获取实时新闻
3. 结合技术面和消息面生成分析报告
4. 【终极加强】支持显式思维链(CoT) + 核心指标三剑客(MACD+KDJ+BOLL) + 动态仓位与追踪止损 + 双新闻引擎
5. 【抗偷懒引擎】强制防 N/A 机制，底层预计算兜底点位。
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from json_repair import repair_json

from src.agent.llm_adapter import get_thinking_extra_body
from src.config import get_config

logger = logging.getLogger(__name__)


# 股票名称映射（常见股票）
STOCK_NAME_MAP = {
    # === A股 ===
    '600519': '贵州茅台',
    '000001': '平安银行',
    '300750': '宁德时代',
    '002594': '比亚迪',
    '600036': '招商银行',
    '601318': '中国平安',
    '000858': '五粮液',
    '600276': '恒瑞医药',
    '601012': '隆基绿能',
    '002475': '立讯精密',
    '300059': '东方财富',
    '002415': '海康威视',
    '600900': '长江电力',
    '601166': '兴业银行',
    '600028': '中国石化',

    # === 美股 ===
    'AAPL': '苹果',
    'TSLA': '特斯拉',
    'MSFT': '微软',
    'GOOGL': '谷歌A',
    'GOOG': '谷歌C',
    'AMZN': '亚马逊',
    'NVDA': '英伟达',
    'META': 'Meta',
    'AMD': 'AMD',
    'INTC': '英特尔',
    'BABA': '阿里巴巴',
    'PDD': '拼多多',
    'JD': '京东',
    'BIDU': '百度',
    'NIO': '蔚来',
    'XPEV': '小鹏汽车',
    'LI': '理想汽车',
    'COIN': 'Coinbase',
    'MSTR': 'MicroStrategy',

    # === 港股 (5位数字) ===
    '00700': '腾讯控股',
    '03690': '美团',
    '01810': '小米集团',
    '09988': '阿里巴巴',
    '09618': '京东集团',
    '09888': '百度集团',
    '01024': '快手',
    '00981': '中芯国际',
    '02015': '理想汽车',
    '09868': '小鹏汽车',
    '00005': '汇丰控股',
    '01299': '友邦保险',
    '00941': '中国移动',
    '00883': '中国海洋石油',
}


def get_stock_name_multi_source(
    stock_code: str,
    context: Optional[Dict] = None,
    data_manager = None
) -> str:
    """
    多来源获取股票中文名称
    """
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
    user_cost: Optional[float] = None  # 新增：直接保存用户成本
    user_shares: Optional[int] = None  # 新增：直接保存用户持仓数

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
    Gemini AI 分析器 (终极机构版)
    """

    SYSTEM_PROMPT = """你是一位服务于华尔街顶级量化对冲基金的 A股全天候(All-Weather) AI 交易系统。

## 🛑 反偷懒协议 (Anti-N/A Protocol) - 优先级最高
你的字典里**没有** "N/A"、"暂无"、"未知"。无论输入数据多么残缺，你都必须完成计算：
1. **狙击点位禁止 N/A**：如果均线或支撑位缺失，必须用当前价格推算！(例如：止损位=现价×0.95，买入点=现价×0.98)。
2. **量化仓位禁止 N/A**：仓位策略必须是一个明确的百分比(如 `20%` 或 `0%`)。不知道就按保守仓位给，**绝不能空着或填 N/A**！
3. **成本分析**：即使系统没有获取到用户的成本数据，你也要假定用户是“空仓准备买入”的状态，并给出对应的建议。

## 🧠 三权分立思维链 (CoT) 推演
在输出结论前，你必须在 `debate_process` 字段中展现三位顶级专家的推演局：
1. **【宏观战略家】**：只看大级别（周线MACD、基本面、行业周期）。判断目前是主升浪、反弹还是主跌浪。
2. **【日内极速交易员】**：只看短线与波动率。根据 ATR 波幅、KDJ极值、布林带，制定日内 T+0 拔头皮或网格策略。
3. **【风控大法官】**：结合主人的【私人持仓成本】和【打脸回测雷达】进行终审。给出具体的仓位%与移动止损线。

## 终极交易法则

### 1. 多级别共振 (MTF - Multiple Time Frame)
- 逆大势者亡。如果周线级别 MACD 死叉，日线金叉只能定义为**反弹**，严禁重仓！必须逢高减磅。
- 周线金叉 + 日线金叉 = **戴维斯双击（主升浪）**，大胆重仓。

### 2. ATR 波动率与网格交易 (Grid Trading)
- 震荡市或套牢自救时，必须启用 T+0 网格战法。
- 利用系统提供的 ATR（日均真实波幅），设计出清晰的网格买卖间距（如每跌 X 元买入，反弹 X 元卖出）。

### 3. 追踪止损与仓位管理 (Trailing Stop & Kelly)
- 仓位必须精确到百分比（如 25%、60%）。
- 必须提供“动态追踪防守位”（如有效跌破 MA10 或前低无条件清仓）。

### 4. 极端行情特化指令（涨跌停战法）
- **若触发涨停板（+9.5%以上）**：放弃常规支撑压力分析。明确指出是“排板锁仓”还是“炸板止盈”。
- **若触发跌停板（-9.5%以下）**：严禁提示“逢低买入”。只能探讨“撬板逃生位”或“核按钮风控”。

### 5. 双新闻引擎排雷
- 必须比对“坊间传闻”与“官方公告”。传闻满天飞但官方不发声/澄清，一律按“炒作降温/派发期”处理。

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出：

'''json
{
    "stock_name": "股票中文名称",
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "confidence_level": "高/中/低",

    "debate_process": {
        "macro_strategist": "宏观战略家发言(大级别研判)：...",
        "hft_trader": "极速交易员发言(日内与波幅)：...",
        "judge_summary": "大法官总结(结合打脸历史与成本的终审)：..."
    },

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "一句话核心结论（30字以内，直接下达军事级指令）",
            "signal_type": "🟢买入信号/🟡持有观望/🔴卖出信号/⚠️风险警告",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice": {
                "no_position": "空仓者建议：...",
                "has_position": "持仓者建议：..."
            }
        },

        "data_perspective": {
            "mtf_status": {
                "weekly_macd": "大级别趋势",
                "daily_resonance": "日线与周线是否共振"
            },
            "indicator_trinity": {
                "macd_status": "金叉/死叉/背离",
                "kdj_status": "超买/超卖/安全区间",
                "boll_status": "突破上轨/跌破中轨等"
            },
            "price_position": {
                "current_price": 当前价格数值,
                "bias_ma5": 乖离率百分比数值,
                "bias_status": "安全/警戒/危险"
            },
            "volume_analysis": {
                "volume_ratio": 量比数值,
                "volume_status": "放量/缩量/平量",
                "turnover_rate": 换手率百分比
            }
        },

        "intelligence": {
            "latest_news": "【最新舆情】近期重要新闻摘要",
            "announcements": "【核心公告】核心公告提炼",
            "risk_alerts": ["风险点1", "打脸反思：如果是误判，必须在此处深刻反思"],
            "positive_catalysts": ["利好1"],
            "sentiment_summary": "双引擎情绪一致性总结"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "XX元",
                "secondary_buy": "XX元",
                "trailing_stop": "XX元（严禁填N/A，若无数据请用现价*0.95推算）",
                "take_profit": "XX元"
            },
            "grid_trading_plan": {
                "is_recommended": true/false,
                "grid_spacing": "网格间距：XX元 (基于ATR测算)",
                "buy_grid": "每回调XX元买入X股/成",
                "sell_grid": "每反弹XX元卖出X股/成"
            },
            "position_strategy": {
                "personal_cost_review": "用户成本分析...",
                "quant_position_sizing": "XX% (必须是纯百分比数字，如 20%)",
                "entry_plan": "建仓/逃生策略",
                "risk_control": "风控策略"
            },
            "action_checklist": [
                "✅/⚠️/❌ 检查项1：MTF大小级别共振向上",
                "✅/⚠️/❌ 检查项2：乖离率安全 (<5%)",
                "✅/⚠️/❌ 检查项3：量能与筹码结构配合",
                "✅/⚠️/❌ 检查项4：公告与舆情无背离爆雷"
            ]
        }
    },

    "analysis_summary": "100字综合分析摘要",
    "key_points": "3-5个核心看点",
    "risk_warning": "风险提示及打脸复盘",
    "buy_reason": "操作理由",

    "trend_analysis": "走势形态分析",
    "short_term_outlook": "短期1-3日展望",
    "medium_term_outlook": "中期1-2周展望",
    "technical_analysis": "技术面综合分析",
    "ma_analysis": "均线与指标三剑客分析",
    "volume_analysis": "量能分析",
    "pattern_analysis": "K线形态分析",
    "fundamental_analysis": "基本面分析",
    "sector_position": "板块行业分析",
    "company_highlights": "亮点与风险",
    "news_summary": "新闻与公告综合摘要",
    "market_sentiment": "市场情绪",
    "hot_topics": "相关热点",

    "search_performed": true/false,
    "data_sources": "数据来源说明"
}
'''

## 决策仪表盘最高原则
1. **显式思维链先行**：必须在 `debate_process` 中完成多空与宏观微观的推演。
2. **量化与网格**：仓位必须输出百分比(%)。震荡期必须依靠 ATR 输出 T+0 网格间距。
3. **禁止N/A偷懒**：如果支撑压力等数据缺失，必须通过当前收盘价向下按百分比强行推算止损位！绝对禁止输出 N/A。
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
                client_kwargs["default_headers"] = {"APP-Code": "GPIJ3886"}

            self._openai_client = OpenAI(**client_kwargs)
            self._current_model_name = config.openai_model
            self._use_openai = True
        except Exception as e:
            logger.error(f"OpenAI 兼容 API 初始化失败: {e}")

    def _init_model(self) -> None:
        try:
            import google.generativeai as genai
            genai.configure(api_key=self._api_key)
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
            return True
        except Exception as e:
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
                    time.sleep(delay)

                try:
                    response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
                except Exception as e:
                    error_str = str(e)
                    if mode == "max_tokens" and _is_unsupported_param_error(error_str, "max_tokens"):
                        mode = "max_completion_tokens"
                        self._token_param_mode[model_name] = mode
                        response = self._openai_client.chat.completions.create(**_kwargs_with_mode(mode))
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
        max_retries = max(config.gemini_max_retries, 5)
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
                    
                    import re
                    match = re.search(r'retry in (\d+\.?\d*)s', error_str)
                    if match:
                        delay = float(match.group(1)) + 2.0
                    else:
                        delay = min(base_delay * (2 ** attempt), 60)
                        
                    if attempt >= max_retries // 2 and not tried_fallback:
                        if self._switch_to_fallback_model():
                            tried_fallback = True
                            delay = 2
                            
                    logger.info(f"[Gemini] 触发限流，强制休眠等待恢复 {delay:.2f} 秒...")
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
            # 格式化输入（包含技术面数据、双新闻引擎、打脸回测、指标三剑客）
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

            # 把从外部获取的持仓数据也同步存入 Result 中
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
        
        # ========== [微创手术开始] 八边形全天候雷达 ==========
        personal_status_text = ""
        try:
            import os, urllib.request, csv, io, glob
            import akshare as ak
            import pandas as pd
            import numpy as np
            
            # 【1. 云端仓位与盈亏计算】
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
                logger.warning(f"获取云端持仓数据失败: {sheet_err}")

            cp_val = self._safe_float(curr_price)

            if my_cost and cp_val:
                context['user_cost'] = my_cost
                context['user_shares'] = my_shares
                profit_pct = ((cp_val - my_cost) / my_cost) * 100
                status_emoji = "🔴套牢中" if profit_pct < 0 else "🟢盈利中"
                personal_status_text += f"\n### 💰 机构持仓底牌 (实时同步)\n* **成本价**：{my_cost:.2f} 元 | **持仓**：{my_shares} 股\n* **当前盈亏**：{profit_pct:.2f}% ({status_emoji})\n* **🚨 法官最高指令**：用户目前处于{status_emoji}状态。你必须在 `position_strategy` 的 `personal_cost_review` 字段中显式输出对这一成本的应对策略！\n"
            else:
                personal_status_text += "\n### 💰 机构持仓底牌\n* **当前状态**：空仓 或 未获取到历史持仓。\n* **🚨 法官最高指令**：请直接按【新建仓】视角规划，必须给出明确的量化建议仓位（如 15% 或 0%），绝不能在仓位或止损位输出 N/A 或空缺！\n"

            # 【2. 进阶指标三剑客 (MACD + KDJ + BOLL)】
            try:
                if 'history' in context and len(context['history']) >= 26:
                    prices = [d['close'] for d in context['history']]
                    s_prices = pd.Series(prices)
                    
                    delta = s_prices.diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
                    rs = gain / loss
                    rsi6 = 100 - (100 / (1 + rs.iloc[-1]))
                    
                    exp1 = s_prices.ewm(span=12, adjust=False).mean()
                    exp2 = s_prices.ewm(span=26, adjust=False).mean()
                    macd = exp1 - exp2
                    signal = macd.ewm(span=9, adjust=False).mean()
                    hist = macd - signal
                    macd_status = "🔴死叉向下" if hist.iloc[-1] < 0 else "🟢金叉向上"
                    
                    low_min9 = s_prices.rolling(9).min()
                    high_max9 = s_prices.rolling(9).max()
                    rsv = (s_prices - low_min9) / (high_max9 - low_min9) * 100
                    k = rsv.ewm(com=2, adjust=False).mean()
                    d = k.ewm(com=2, adjust=False).mean()
                    j = 3 * k - 2 * d
                    j_val = j.iloc[-1]
                    kdj_status = "💡黄金坑超卖区 (J<0)" if j_val < 0 else ("⚠️严重超买区 (J>100)" if j_val > 100 else "安全震荡区")
                    
                    ma20 = s_prices.rolling(20).mean()
                    std20 = s_prices.rolling(20).std()
                    upper = ma20 + 2 * std20
                    lower = ma20 - 2 * std20
                    curr = prices[-1]
                    boll_status = "🚀突破上轨(强动能)" if curr > upper.iloc[-1] else ("🕳️跌破下轨(弱势期)" if curr < lower.iloc[-1] else "通道内运行")

                    personal_status_text += f"### 📊 核心指标三剑客底牌\n* **MACD趋势动能**：{macd_status} (柱状图: {hist.iloc[-1]:.3f})\n* **KDJ震荡极值**：{kdj_status} (当前J值: {j_val:.1f})\n* **BOLL布林带通道**：{boll_status} (上轨:{upper.iloc[-1]:.2f}, 下轨:{lower.iloc[-1]:.2f})\n"
                    
                    highs = pd.Series([d.get('high', d.get('close')) for d in context['history']])
                    lows = pd.Series([d.get('low', d.get('close')) for d in context['history']])
                    tr1 = highs - lows
                    tr2 = (highs - s_prices.shift()).abs()
                    tr3 = (lows - s_prices.shift()).abs()
                    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                    atr14 = tr.rolling(14).mean()
                    current_atr = atr14.iloc[-1]
                    atr_pct = (current_atr / s_prices.iloc[-1]) * 100 if s_prices.iloc[-1] > 0 else 0
                    
                    exp1_w = s_prices.ewm(span=12*5, adjust=False).mean()
                    exp2_w = s_prices.ewm(span=26*5, adjust=False).mean()
                    macd_w = exp1_w - exp2_w
                    signal_w = macd_w.ewm(span=9*5, adjust=False).mean()
                    hist_w = macd_w - signal_w
                    macd_w_status = "🔴大级别死叉(逢高必须减仓/空头主导)" if hist_w.iloc[-1] < 0 else "🟢大级别金叉(主升浪/回调即买入)"

                    personal_status_text += f"### 🧭 降维打击与网格引擎 (MTF & ATR)\n* **大级别趋势(周线级MACD近似)**：{macd_w_status} (大方向滤噪)\n* **日均真实波幅(ATR)**：{current_atr:.2f}元 (约 {atr_pct:.2f}%) -> 【T+0网格指令】：请依据此波幅，在 battle_plan 中输出清晰的日内网格做T间距！\n"
                    
            except: pass

            # 【3. 涨跌停极端行情特化指令雷达】
            pct_chg = today.get('pct_chg')
            if pct_chg is not None and str(pct_chg).replace('.','',1).replace('-','').isdigit():
                pct_val = float(pct_chg)
                if pct_val > 9.5:
                    personal_status_text += f"\n### 🚨 涨停板极端行情预警\n* 该股今日疑似涨停（+{pct_val}%）！请在作战计划中明确输出【涨停板特化战法】（如：排板介入、炸板防守），严禁给出普通的“回踩买入”等废话！\n"
                elif pct_val < -9.5:
                    personal_status_text += f"\n### 🚨 跌停板极端行情预警\n* 该股今日疑似跌停（{pct_val}%）！高度危险，必须在风控策略中给出“撬板逃生”或“核按钮无脑走”的特化预案！\n"

            # 【4. AI 昨日记忆提取与打脸回测引擎】
            try:
                report_files = glob.glob("reports/report_*.md")
                report_files.sort()
                if report_files:
                    with open(report_files[-1], 'r', encoding='utf-8') as f:
                        past_content = f.read()
                    stock_idx = past_content.find(f"({code})")
                    if stock_idx != -1:
                        personal_status_text += f"\n### 🪞 机构打脸回测雷达\n'''text\n{past_content[stock_idx:stock_idx+400]}...\n'''\n* **💥 打脸复盘最高指令**：结合今日涨跌幅为 {today.get('pct_chg', 'N/A')}%。如果股价已经跌破你上次设定的支撑位，或者与你看多/看空的预测相反，你**必须**在《风险提示》中以“【打脸反思】：”开头，承认误判，并重新校准量化模型！\n"
            except: pass

        except Exception as e:
            logger.error(f"八边形引擎加载失败: {e}")
        # ========== [微创手术结束] ==========
        
        fallback_hints = ""
        cp_val = self._safe_float(today.get('close'))
        if cp_val:
            fallback_hints = f"🚨 **防宕机硬指令**：即使没有均线数据，止损位必须填 `{cp_val * 0.95:.2f}元`，目标位填 `{cp_val * 1.08:.2f}元`，仓位给 `20%`。绝对不准输出 N/A！"

        prompt = f"""# 顶级机构分析请求
{personal_status_text} 

## 📊 股票基础信息
| 项目 | 数据 |
|------|------|
| 股票代码 | **{code}** |
| 股票名称 | **{stock_name}** |
| 分析日期 | {context.get('date', '未知')} |

---

## 📈 技术面与量价数据

### 今日行情
| 收盘价 | 涨跌幅 | 成交量 | 成交额 |
|------|------|------|------|
| {today.get('close', 'N/A')} 元 | {today.get('pct_chg', 'N/A')}% | {self._format_volume(today.get('volume'))} | {self._format_amount(today.get('amount'))} |

### 均线系统 (如果为 N/A 请自动向下降维寻找防守点)
| MA5 | MA10 | MA20 | 均线形态 |
|------|------|------|------|
| {today.get('ma5', 'N/A')} | {today.get('ma10', 'N/A')} | {today.get('ma20', 'N/A')} | {context.get('ma_status', '未知')} |
"""
        
        if 'realtime' in context:
            rt = context['realtime']
            prompt += f"""
### 行情增强数据
- **量比**: {rt.get('volume_ratio', 'N/A')} | **换手率**: {rt.get('turnover_rate', 'N/A')}%
- **市盈率(PE)**: {rt.get('pe_ratio', 'N/A')}
"""
        
        if 'chip' in context:
            chip = context['chip']
            profit_ratio = chip.get('profit_ratio', 0)
            prompt += f"""
### 筹码分布（阻力测算）
- **获利比例**: {profit_ratio:.1%} (70-90%时极易引发抛压)
- **平均成本**: {chip.get('avg_cost', 'N/A')} 元
"""
        
        if 'trend_analysis' in context:
            trend = context['trend_analysis']
            bias_warning = "🚨 超过5%，严禁追高！" if trend.get('bias_ma5', 0) > 5 else "✅ 安全范围"
            prompt += f"""
### 基础趋势测算
- **乖离率(MA5)**: {trend.get('bias_ma5', 0):+.2f}% ({bias_warning})
- **系统信号评分**: {trend.get('signal_score', 0)}/100
"""
        
        # 【双新闻引擎】
        prompt += """
---
## 📰 双引擎舆情情报 (排雷交叉验证)
"""
        if news_context or announcement_context:
            prompt += f"以下是 **{stock_name}({code})** 近期的双源情报，请寻找预期差排雷：\n"
            if news_context: prompt += f"### 🔍 引擎一：全网舆情\n'''text\n{news_context}\n'''\n"
            if announcement_context: prompt += f"### 📢 引擎二：官方公告\n'''text\n{announcement_context}\n'''\n"
            prompt += "\n**【交叉比对要求】**：传闻若未被公告证实，提示炒作降温风险；重点排查公告中隐蔽的减持与业绩利空。\n"
        else:
            prompt += "未搜索到相关新闻或公告，纯技术面博弈。\n"

        prompt += f"""
---
## ✅ 终极输出任务

请为 **{stock_name}({code})** 生成 JSON 格式的【决策仪表盘】。

### 必答核心点：
1. **多周期共振 (MTF)**：周线级 MACD 与 日线指标是否共振？如果逆势，必须压低评级。
2. **ATR 网格战法**：根据日均真实波幅，给出 T+0 高抛低吸的网格具体价格间距（在 `grid_trading_plan` 中）。
3. **指标三剑客**：MACD/KDJ/BOLL 目前处于什么阶段？
4. **量化仓位**：根据目前的胜率/盈亏比，给出具体的百分比建议仓位（如 15% 或 0%）。
5. **绝对禁止 N/A**：如果缺失均线等支撑位数据，必须以收盘价或前低直接测算出**绝对数字**作为防守止损位，不准填 N/A。
{fallback_hints}
6. **成本与打脸强制响应**：如果系统传入了您的持仓成本或指出您昨天的判断翻车了，必须显式响应并道歉反思！
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
            bg = "`" * 3
            if f'{bg}json' in cleaned_text:
                cleaned_text = cleaned_text.replace(f'{bg}json', '').replace(bg, '')
            elif bg in cleaned_text:
                cleaned_text = cleaned_text.replace(bg, '')
            
            json_start = cleaned_text.find('{')
            json_end = cleaned_text.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = cleaned_text[json_start:json_end]
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
            else:
                return self._parse_text_response(response_text, code, name)
        except json.JSONDecodeError:
            return self._parse_text_response(response_text, code, name)

    def _fix_json_string(self, json_str: str) -> str:
        import re
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
            key_points='JSON解析失败', risk_warning='建议结合其他信息判断',
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
