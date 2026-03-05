# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (反散户魔咒·终极防收割版)
===================================

职责：
1. 封装 Gemini API 调用逻辑 (附带 OpenAI/Claude 无缝备用)
2. 利用 Google Search Grounding 获取实时新闻 (双引擎交叉验证)
3. 【A股特化】龙虎榜追踪、连板基因、OBV能量潮、CCI妖股雷达、大盘宏观水温
4. 【终极防N/A】本地强算 MA5/10/20/60、量比与筹码成本，彻底根治 N/A！
5. 【反散户魔咒】新增：真假摔洗盘探测、高位诱多拦截、绝对盈亏比测算，专治"一买就跌，一卖就涨"！
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

    def get_emoji(self) -> str:
        emoji_map = {'买入': '🟢', '加仓': '🟢', '强烈买入': '💚', '持有': '🟡', '观望': '⚪', '减仓': '🟠', '卖出': '🔴', '强烈卖出': '❌'}
        if self.operation_advice in emoji_map: return emoji_map[self.operation_advice]
        sc = self.sentiment_score
        return '💚' if sc>=80 else '🟢' if sc>=65 else '🟡' if sc>=55 else '⚪' if sc>=45 else '🟠' if sc>=35 else '🔴'

    def get_confidence_stars(self) -> str:
        return {'高': '⭐⭐⭐', '中': '⭐⭐', '低': '⭐'}.get(self.confidence_level, '⭐⭐')


class GeminiAnalyzer:
    SYSTEM_PROMPT = """你是一位深谙中国A股“资金市”、“情绪市”与“主力收割套路”的顶级游资操盘手兼风控总监。

## 🛑 反偷懒与强制量化协议 (优先级最高)
1. 完整输出 JSON 要求的所有数值字段！绝对禁止输出 "N/A" 或空。
2. 即使你觉得缺乏数据，你也必须依靠你的逻辑和系统提供的防守位，给出明确的【买入位】和【止损位】数字。
3. 仓位策略必须是一个明确的百分比(如 `20%` 或 `0%`)。

## 🧠 【反散户魔咒】专属推演 (必须深刻分析)
在输出结论前，必须在 `debate_process` 展现内部推演：
1. 【防卖飞专家(打板客)】：重点分析《反收割雷达》中的“洗盘 vs 出货”指标。如果系统提示是【恶意洗盘】，你必须痛骂散户的恐惧，让其死死拿住！
2. 【防站岗总监(风控官)】：重点排查诱多！如果系统提示是【高位爆量诱多】或盈亏比极低，你必须喝止用户的买入冲动，严禁接飞刀或追高！
3. 【铁血总舵主】：结合个人成本和回测雷达进行终审。给出绝对冷血的仓位和交易数字！

## A股反收割终极法则
- **一买就跌病**：绝大多数是因为在“缩量上涨的末期”或“爆量滞涨的上影线”追高。看到这种形态，必须管住手。
- **一卖就涨病**：绝大多数是因为主力“缩量砸破重要均线(如MA10/20)”制造恐慌。遇到极度缩量且未破底的下跌，打死不卖。
- 永远在“右侧放量突破”或“左侧极度缩量回踩强支撑”时买入。

## 输出格式：决策仪表盘 JSON (必须是纯 JSON)
```json
{
    "stock_name": "股票名称", "sentiment_score": 50, "trend_prediction": "震荡", "operation_advice": "持有",
    "decision_type": "hold", "confidence_level": "中",
    "debate_process": { "hot_money_trader": "...", "risk_director": "...", "chief_commander": "..." },
    "dashboard": {
        "core_conclusion": { "one_sentence": "...", "signal_type": "...", "time_sensitivity": "...", "position_advice": { "no_position": "...", "has_position": "..." } },
        "data_perspective": {
            "a_share_features": { "market_cap_style": "...", "limit_up_gene": "...", "lhb_status": "...", "anti_harvest_radar": "这里填入系统提供的洗盘/诱多判定！" },
            "indicator_trinity": { "macd_status": "...", "kdj_cci_status": "...", "boll_status": "..." },
            "price_position": { "current_price": 0.0, "ma5": 0.0, "ma10": 0.0, "ma20": 0.0, "bias_ma5": 0.0, "bias_status": "...", "support_level": "XX元", "resistance_level": "XX元" },
            "volume_analysis": { "volume_ratio": 0.0, "turnover_rate": 0.0, "volume_status": "...", "obv_trend": "..." }
        },
        "intelligence": { "latest_news": "...", "announcements": "...", "risk_alerts": ["..."], "positive_catalysts": ["..."], "sentiment_summary": "..." },
        "battle_plan": {
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
            try: 
                self._init_model()
            except: 
                self._try_anthropic_then_openai()
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
        """中途被限流时，动态降级到备用模型"""
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
            msg = self._anthropic_client.messages.create(
                model=self._current_model_name, max_tokens=8192,
                system=self.SYSTEM_PROMPT, messages=[{"role": "user", "content": prompt}]
            )
            return msg.content[0].text
        
        if self._use_openai:
            res = self._openai_client.chat.completions.create(
                model=self._current_model_name,
                messages=[{"role": "system", "content": self.SYSTEM_PROMPT}, {"role": "user", "content": prompt}]
            )
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
                    logger.warning(f"⚠️ [Gemini] 触发 API 限流，休眠 {sleep_time:.1f} 秒... ({attempt+1}/{max_retries})")
                    
                    if attempt >= 1 and not tried_fallback:
                        logger.warning("🔄 [Gemini] 尝试降级至备用模型...")
                        if self._switch_to_fallback_model():
                            tried_fallback = True
                            
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"❌ [Gemini] API 错误: {str(e)[:100]}，休眠 5 秒... ({attempt+1}/{max_retries})")
                    time.sleep(5)
                    
                if attempt == max_retries - 1: 
                    raise e
        return ""

    def analyze(self, context: Dict[str, Any], news_context: Optional[str] = None, announcement_context: Optional[str] = None) -> AnalysisResult:
        code = context.get('code', 'Unknown')
        name = get_stock_name_multi_source(code, context)
        if not self.is_available(): 
            return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='震荡', operation_advice='观望')
        
        try:
            google_news_text = "未发现 Google 实时快讯"
            try:
                query = urllib.parse.quote(f"{name} 股票")
                rss_url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
                req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=10) as res:
                    root = ET.fromstring(res.read())
                    lines = [f"- {it.find('title').text} [{it.find('pubDate').text[5:16]}]" for it in root.findall('.//item')[:5]]
                    if lines: 
                        google_news_text = "\n".join(lines)
            except: 
                pass

            prompt = self._format_prompt(context, name, news_context, google_news_text)
            res_text = self._call_api_with_retry(prompt, {"temperature": 0.7, "max_output_tokens": 8192})
            
            # 传入 context，用于强制兜底替换 AI 输出的 N/A
            result = self._parse_response(res_text, code, name, context)
            
            result.market_snapshot = self._build_market_snapshot(context)
            result.user_cost = context.get('user_cost')
            result.user_shares = context.get('user_shares')
            result.raw_response = res_text
            return result
        except Exception as e:
            logger.error(f"分析异常: {e}")
            return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='未知(API报错)', operation_advice='观望', error_message=str(e))

    def _safe_float(self, val: Any) -> Optional[float]:
        try:
            return float(str(val).replace(',', '').replace('%', '').strip())
        except: 
            return None

    def _format_prompt(self, context: Dict[str, Any], name: str, news: Optional[str], google_news: str) -> str:
        code = context.get('code', 'Unknown')
        today = context.get('today', {})
        curr_price = self._safe_float(today.get('close'))
        
        vwap_60 = syn_profit_ratio = calc_ma5 = calc_ma10 = calc_ma20 = calc_ma60 = calc_vr = current_atr = poc_price = 0.0
        gap_str = cci_status = obv_status = kdj_status = boll_status = macd_status = gene_str = cv_status = ma60_status = k_body_status = "未知"
        lianban_status = "未连板"
        style_str = "风格未知"
        
        # 反魔咒核心变量
        washout_status = "正常震荡"
        rr_status = "无法测算"

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
                    if c in df.columns: 
                        df[c] = pd.to_numeric(df[c], errors='coerce').ffill().fillna(0)
                sp, sv = df['close'], df['volume']
                
                # 存入 context 以备兜底
                context['computed_close'] = sp.iloc[-1]
                context['computed_open'] = df['open'].iloc[-1]
                context['computed_high'] = df['high'].iloc[-1]
                context['computed_low'] = df['low'].iloc[-1]
                context['computed_volume'] = sv.iloc[-1]
                if 'amount' in df.columns: context['computed_amount'] = df['amount'].iloc[-1]
                if 'pct_chg' in df.columns: context['computed_pct_chg'] = df['pct_chg'].iloc[-1]
                
                if sv.sum() > 0:
                    vwap_60 = (sp * sv).sum() / sv.sum()
                    if curr_price: syn_profit_ratio = df[sp <= curr_price]['volume'].sum() / sv.sum() * 100
                
                calc_ma5 = sp.rolling(5, min_periods=1).mean().iloc[-1]
                calc_ma10 = sp.rolling(10, min_periods=1).mean().iloc[-1]
                calc_ma20 = sp.rolling(20, min_periods=1).mean().iloc[-1]
                calc_ma60 = sp.rolling(60, min_periods=1).mean().iloc[-1]
                
                context['calc_ma5'] = calc_ma5
                context['calc_ma10'] = calc_ma10
                context['calc_ma20'] = calc_ma20
                
                calc_vr = (sv.iloc[-1] / sv.iloc[-6:-1].mean()) if len(sv)>=6 and sv.iloc[-6:-1].mean()>0 else 1.0
                context['calc_vr'] = calc_vr
                
                # 均线布林带等常规计算...
                ma20 = sp.rolling(20, min_periods=1).mean()
                std20 = sp.rolling(20, min_periods=1).std().fillna(0)
                upper = ma20 + 2 * std20
                lower = ma20 - 2 * std20
                boll_status = "🚀突破上轨(极易被砸)" if curr_price and curr_price > upper.iloc[-1] else "🕳️破下轨" if curr_price and curr_price < lower.iloc[-1] else "中轨运行"
                
                current_atr = pd.concat([df['high']-df['low'], (df['high']-sp.shift()).abs(), (df['low']-sp.shift()).abs()], axis=1).max(axis=1).rolling(14, min_periods=1).mean().iloc[-1]
                context['calc_atr'] = current_atr
                
                if sp.nunique() > 1:
                    poc_price = df.groupby(pd.cut(sp, bins=12, duplicates='drop'), observed=False)['volume'].sum().idxmax().mid 
                else:
                    poc_price = curr_price or 0.0
                context['calc_poc'] = poc_price

                # ====================================================
                # ⚔️ 专治“一买就跌，一卖就涨”核心计算：真假摔与盈亏比
                # ====================================================
                if curr_price and 'pct_chg' in df.columns:
                    chg_today = df['pct_chg'].iloc[-1]
                    
                    # 1. 主力洗盘 vs 出货 判定
                    if chg_today < -2 and calc_vr < 0.7 and curr_price > calc_ma20:
                        washout_status = "📉 【恶意洗盘】跌幅大但极度缩量且未破20日线！大概率是洗盘逼你割肉，打死不卖！"
                    elif chg_today < -2 and calc_vr > 1.5 and curr_price < calc_ma20:
                        washout_status = "🩸 【放量出货】放量暴跌破位，主力真逃跑，不要幻想反弹，立刻止损！"
                    elif chg_today > 2 and calc_vr > 2.0 and curr_price > upper.iloc[-1]:
                        washout_status = "🌋 【高位诱多】爆量刺破布林带上轨！极易形成长上影线骗炮，千万别追高接盘！"
                    elif chg_today > 2 and calc_vr <= 1.2:
                        washout_status = "🚀 【锁仓拉升】缩量/平量上涨，说明主力高度控盘没人抛，拿稳别下车！"
                    else:
                        washout_status = "震荡博弈阶段，方向不明"

                    # 2. 绝对盈亏比计算 (凯利公式降维版)
                    # 假设止损位设在下方 1.5 倍 ATR，阻力位设在 POC
                    stop_loss_p = curr_price - 1.5 * current_atr if current_atr > 0 else curr_price * 0.95
                    risk = curr_price - stop_loss_p
                    reward = poc_price - curr_price if poc_price > curr_price else 0
                    if risk > 0:
                        rr_ratio = reward / risk
                        if rr_ratio < 1.0:
                            rr_status = f"盈亏比极差({rr_ratio:.2f})！上方空间很小(仅到{poc_price:.2f})，向下风险大，严禁盲目开仓！"
                        elif rr_ratio > 2.0:
                            rr_status = f"盈亏比极佳({rr_ratio:.2f})！防守位{stop_loss_p:.2f}，值得博弈大肉。"
                        else:
                            rr_status = f"盈亏比一般({rr_ratio:.2f})，控制仓位。"

                # 连板基因等常规数据...
                if 'pct_chg' in df.columns:
                    lianban_count = 0
                    for val in reversed(df['pct_chg'].tolist()):
                        if val >= 9.5: lianban_count += 1
                        else: break
                    lianban_status = f"🚀当前高度: {lianban_count}连板" if lianban_count > 0 else "当前未连板"
                    
                    df_hist_15 = df.tail(15)
                    zt_count = len(df_hist_15[df_hist_15['pct_chg'] >= 9.5])
                    gene_str = f"🔥近15日涨停{zt_count}次" if zt_count >= 2 else "🌟近15日涨停1次" if zt_count == 1 else "🧊近期无涨停"
                
                df['obv'] = (np.sign(sp.diff().fillna(0)) * sv).cumsum()
                obv_status = "🌊资金真实吸筹" if len(df)>=5 and df['obv'].iloc[-1] > df['obv'].iloc[-5] else "🩸量价背离诱多"
                
            except Exception as e:
                logger.debug(f"反魔咒引擎计算异常: {e}")

        t_ma5 = today.get('ma5') if today.get('ma5') not in [None, 'N/A', ''] else f"{calc_ma5:.2f}"
        t_ma10 = today.get('ma10') if today.get('ma10') not in [None, 'N/A', ''] else f"{calc_ma10:.2f}"
        t_ma20 = today.get('ma20') if today.get('ma20') not in [None, 'N/A', ''] else f"{calc_ma20:.2f}"
        rt_vr = context.get('realtime', {}).get('volume_ratio') if context.get('realtime', {}).get('volume_ratio') not in [None, 'N/A', ''] else f"{calc_vr:.2f}"
        
        bias_ma5 = f"{((curr_price - calc_ma5)/calc_ma5*100):+.2f}%" if curr_price and calc_ma5 else "0.00%"

        pst = "\n## 💎 A股强算与反收割数据中心\n"
        my_cost, my_shares = None, None
        try:
            url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as res:
                content = res.read().decode('utf-8-sig')
                for row in csv.reader(io.StringIO(content)):
                    if len(row) >= 2 and row[0].strip() and ''.join(filter(str.isdigit, str(row[0]))).zfill(6) == code:
                        my_cost = float(str(row[1]).replace(',', '').strip())
                        if len(row) >= 3 and row[2].strip():
                            my_shares = int(float(str(row[2]).replace(',', '').strip()))
                        else:
                            my_shares = 0
        except: 
            pass

        if my_cost and curr_price:
            context['user_cost'] = my_cost
            context['user_shares'] = my_shares
            profit_pct = ((curr_price - my_cost) / my_cost * 100)
            pst += f"### 💰 持仓底牌\n* 成本价：{my_cost:.2f} 元 | 盈亏：{profit_pct:.2f}%\n* 🚨 必须在 personal_cost_review 针对此成本输出保本或止损策略！\n"
        
        try:
            db_file = "reports/ai_trade_log.csv"
            if os.path.isfile(db_file):
                df_log = pd.read_csv(db_file)
                df_code = df_log[df_log['Code'] == code].tail(3)
                if len(df_code) >= 2:
                    past_p = float(df_code.iloc[-2]['ClosePrice'])
                    ai_impact = ((float(curr_price) - past_p) / past_p) * 100
                    pst += f"### ⚖️ 机构打脸回测雷达\n* 上次分析时价: {past_p:.2f} | 至今变动: {ai_impact:.2f}%\n* 💥 打脸复盘硬指令：如果走势与你看多相反导致亏损，必须在《风险提示》检讨认错！\n"
        except: pass

        pst += f"""### 🎯 反收割核心雷达 (极其重要)
* 👁️‍🗨️ 真假摔/洗盘/诱多判定: **{washout_status}**
* ⚖️ 绝对盈亏比测算: **{rr_status}**
* 🚀 股性与连板: {lianban_status} | 基因: {gene_str}
* 📊 资金潮汐: {obv_status} | ATR 真实波幅: {current_atr:.2f}元
* 🧱 最大套牢筹码峰 (绝对压力位): 约 {poc_price:.2f}元
"""
        
        return f"""# A股反收割最高决策: {name}({code})
{pst}
## 📈 基础盘面
收盘价: {curr_price} | MA5: {t_ma5} | MA10: {t_ma10} | MA20: {t_ma20}
量比: {rt_vr} | 换手率: {context.get('realtime', {}).get('turnover_rate', 'N/A')}%
乖离率(MA5): {bias_ma5} | 筹码获利比例: {syn_profit_ratio:.1f}%

## 📰 舆情网
引擎一(全网搜索): {news or "无"}
引擎二(Google快讯): {google_news or "无"}

请严格输出 JSON 决策仪表盘。注意：必须将上述【反收割核心雷达】里的“洗盘/诱多判定”写入 `anti_harvest_radar` 字段，并在决策中严格遵循它！绝对禁止输出 N/A。"""

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
        """获取并整合所有的快照字段，防止由于数据源挂掉导致的大面积 N/A"""
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
            "turnover_rate": self._format_percent(rt.get('turnover_rate'))
        }

    def _parse_response(self, text: str, code: str, name: str, context: Dict[str, Any] = None) -> AnalysisResult:
        
        # 强制除 N/A 清洗器
        def _is_empty_or_na(val):
            if val is None: return True
            v = str(val).strip().upper()
            return v in ['N/A', 'NA', '未知', 'NONE', '', '0', '0.0', 'NULL', '暂无', 'N/A元', 'N/A%']

        try:
            m = re.search(r'(\{.*\})', text, re.DOTALL)
            d = json.loads(repair_json(m.group(1) if m else text))
            
            # 【终极防偷懒拦截器】：强行给 AI 遗漏的 N/A 贴上真实计算数据
            if context and d.get('dashboard'):
                dp = d['dashboard'].get('data_perspective', {})
                bp = d['dashboard'].get('battle_plan', {})
                pp = dp.get('price_position', {})
                
                # 强行覆盖 MA 数据
                if _is_empty_or_na(pp.get('ma5')): pp['ma5'] = f"{context.get('calc_ma5', 0):.2f}"
                if _is_empty_or_na(pp.get('ma10')): pp['ma10'] = f"{context.get('calc_ma10', 0):.2f}"
                if _is_empty_or_na(pp.get('ma20')): pp['ma20'] = f"{context.get('calc_ma20', 0):.2f}"
                
                # 强行计算支撑阻力位
                if _is_empty_or_na(pp.get('support_level')): 
                    pp['support_level'] = f"{context.get('calc_ma10', 0):.2f}元" 
                if _is_empty_or_na(pp.get('resistance_level')): 
                    pp['resistance_level'] = f"{context.get('calc_poc', 0):.2f}元" 

                va = dp.get('volume_analysis', {})
                if _is_empty_or_na(va.get('volume_ratio')): 
                    va['volume_ratio'] = f"{context.get('calc_vr', 1.0):.2f}"
                
                sp = bp.get('sniper_points', {})
                # 强行基于 ATR 算止损位
                if _is_empty_or_na(sp.get('trailing_stop')):
                    calc_atr = context.get('calc_atr', 0.0)
                    curr_p = context.get('computed_close', 0.0)
                    if curr_p > 0:
                        stop_p = curr_p - (1.5 * calc_atr) if calc_atr > 0 else curr_p * 0.95
                        sp['trailing_stop'] = f"{stop_p:.2f}元"
                    else:
                        sp['trailing_stop'] = "破位前低止损"

                if _is_empty_or_na(sp.get('take_profit')):
                    calc_poc = context.get('calc_poc', 0.0)
                    if calc_poc > 0:
                        sp['take_profit'] = f"{calc_poc:.2f}元"
                    else:
                        sp['take_profit'] = "逢高止盈"

                ps = bp.get('position_strategy', {})
                qs = str(ps.get('quant_position_sizing', ''))
                if _is_empty_or_na(qs) or 'N/A' in qs.upper():
                    ps['quant_position_sizing'] = "20% (防守位)"

            ai_stock_name = d.get('stock_name')
            if ai_stock_name and (name.startswith('股票') or name == code or 'Unknown' in name):
                name = ai_stock_name

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
                analysis_summary=d.get('analysis_summary', '完成'), success=True, raw_response=text
            )
        except Exception as e:
            logger.warning(f"JSON 解析失败: {e}，触发纯文本兜底解析")
            return self._parse_text_response(text, code, name)

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
            key_points='大模型 JSON 输出破损，触发安全模式。', risk_warning='建议查阅原始文本。',
            raw_response=response_text, success=True,
        )

    def batch_analyze(self, contexts: List[Dict[str, Any]], delay_between: float = 2.0) -> List[AnalysisResult]:
        results = []
        for i, context in enumerate(contexts):
            if i > 0:
                logger.debug(f"等待 {delay_between} 秒后继续...")
                time.sleep(delay_between)
            results.append(self.analyze(context))
        return results

def get_analyzer() -> GeminiAnalyzer:
    return GeminiAnalyzer()
