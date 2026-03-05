# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层 (A股超神特化·抗截断防报错版)
===================================

职责：
1. 封装 Gemini API 调用逻辑 (附带 OpenAI/Claude 无缝备用)
2. 利用 Google Search Grounding 获取实时新闻 (双引擎交叉验证)
3. 【A股特化】龙虎榜追踪、连板基因、OBV能量潮、CCI妖股雷达、大盘宏观水温
4. 【最新加强】跳空缺口、日内K线实体动能、MA60牛熊分界、筹码集中度变盘雷达
5. 【终极防N/A】本地强算 MA5/10/20/60、量比与筹码成本，彻底根治面板显示 N/A 问题！
6. 【抗断网引擎】自研 VWAP 筹码分布测算兜底算法，无视 API 频繁断网。
"""

import json, logging, time, re, os, csv, io, glob
import urllib.request, urllib.parse
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
        except: pass
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
    SYSTEM_PROMPT = """你是一位深谙中国A股“资金市”、“情绪市”与“龙头战法”的顶级游资操盘手兼风控总监。

## 🛑 反偷懒协议 (Anti-N/A Protocol)
1. 完整输出 JSON 要求的所有数值字段！绝对禁止输出 "N/A" 或空。
2. 仓位策略必须是一个明确的百分比(如 `20%` 或 `0%`)。
3. 如果支撑压力等数据缺失，必须通过当前收盘价向下按百分比强行推算止损位。

## 🧠 A股专属思维链 (CoT)
在输出结论前，必须在 `debate_process` 展现内部推演：
1. 【打板接力客】：查股性/筹码变盘/缺口/资金。
2. 【公募风控总监】：查MA60/日内实体/OBV背离/大盘。
3. 【铁血总舵主】：结合个人成本和回测雷达进行终审。如果套牢，严格按 ATR 给割肉位；若被打脸，必须认错！

## 输出格式：决策仪表盘 JSON (必须是纯 JSON)
```json
{
    "stock_name": "股票名称", "sentiment_score": 50, "trend_prediction": "震荡", "operation_advice": "持有",
    "decision_type": "hold", "confidence_level": "中",
    "debate_process": { "hot_money_trader": "...", "risk_director": "...", "chief_commander": "..." },
    "dashboard": {
        "core_conclusion": { "one_sentence": "...", "signal_type": "...", "time_sensitivity": "...", "position_advice": { "no_position": "...", "has_position": "..." } },
        "data_perspective": {
            "a_share_features": { "limit_up_gene": "...", "lhb_status": "...", "gap_and_trend": "...", "chip_concentration": "..." },
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

"""
def init(self, api_key: Optional[str] = None):
config = get_config()
self._api_key = api_key or config.gemini_api_key
self._model = self._openai_client = self._anthropic_client = None
self._current_model_name = None
self._use_openai = self._use_anthropic = self._using_fallback = False
if self._api_key and not self.api_key.startswith('your') and len(self._api_key) > 10:
try: self._init_model()
except: self._try_anthropic_then_openai()
else:
self._try_anthropic_then_openai()
def _try_anthropic_then_openai(self) -> None:
self._init_anthropic_fallback()
self._init_openai_fallback()
def init_anthropic_fallback(self) -> None:
cfg = get_config()
if cfg.anthropic_api_key and not cfg.anthropic_api_key.startswith('your'):
try:
from anthropic import Anthropic
self._anthropic_client = Anthropic(api_key=cfg.anthropic_api_key)
self._current_model_name = cfg.anthropic_model
self._use_anthropic = True
except: pass
def init_openai_fallback(self) -> None:
cfg = get_config()
if cfg.openai_api_key and not cfg.openai_api_key.startswith('your'):
try:
from openai import OpenAI
kw = {"api_key": cfg.openai_api_key}
if cfg.openai_base_url:
kw["base_url"] = cfg.openai_base_url
if "aihubmix.com" in cfg.openai_base_url: kw["default_headers"] = {"APP-Code": cfg.openai_api_key}
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
max_retries = max(get_config().gemini_max_retries, 5)
for attempt in range(max_retries):
try:
resp = self._model.generate_content(prompt, generation_config=gen_cfg, request_options={"timeout": 120})
if resp.text: return resp.text
except Exception as e:
err_str = str(e).lower()
if '429' in err_str or 'quota' in err_str:
time.sleep(15)
else:
time.sleep(5)
if attempt == max_retries - 1: raise e
return ""
def analyze(self, context: Dict[str, Any], news_context: Optional[str] = None, announcement_context: Optional[str] = None) -> AnalysisResult:
code = context.get('code', 'Unknown')
name = get_stock_name_multi_source(code, context)
if not self.is_available(): return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='震荡', operation_advice='观望')
try:
google_news_text = "未发现 Google 实时快讯"
try:
query = urllib.parse.quote(f"{name} 股票")
rss_url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, timeout=10) as res:
root = ET.fromstring(res.read())
lines = [f"- {it.find('title').text} [{it.find('pubDate').text[5:16]}]" for it in root.findall('.//item')[:5]]
if lines: google_news_text = "\n".join(lines)
except: pass
prompt = self._format_prompt(context, name, news_context, google_news_text)
res_text = self._call_api_with_retry(prompt, {"temperature": 0.7, "max_output_tokens": 8192})
result = self._parse_response(res_text, code, name)
result.market_snapshot = self._build_market_snapshot(context)
result.user_cost = context.get('user_cost')
result.user_shares = context.get('user_shares')
result.raw_response = res_text
return result
except Exception as e:
return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='未知(API报错)', operation_advice='观望', error_message=str(e))
def _safe_float(self, val: Any) -> Optional[float]:
try:
return float(str(val).replace(',', '').replace('%', '').strip())
except: return None
def _format_prompt(self, context: Dict[str, Any], name: str, news: Optional[str], google_news: str) -> str:
code, today = context.get('code', 'Unknown'), context.get('today', {})
curr_price = self._safe_float(today.get('close'))
vwap_60 = syn_profit_ratio = calc_ma5 = calc_ma10 = calc_ma20 = calc_ma60 = calc_vr = current_atr = poc_price = 0.0
gap_str = ma5_trend = cci_status = obv_status = kdj_status = boll_status = macd_status = gene_str = cv_status = ma60_status = k_body_status = "未知"
if 'history' in context and len(context['history']) > 0:
try:
df = pd.DataFrame(context['history']).tail(120)
for c in ['close', 'high', 'low', 'open', 'volume', 'pct_chg']:
if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').ffill().fillna(0)
sp, sv = df['close'], df['volume']
if sv.sum() > 0:
vwap_60 = (sp * sv).sum() / sv.sum()
if curr_price: syn_profit_ratio = df[sp <= curr_price]['volume'].sum() / sv.sum() * 100
calc_ma5, calc_ma10, calc_ma20, calc_ma60 = sp.rolling(5, min_periods=1).mean().iloc[-1], sp.rolling(10, min_periods=1).mean().iloc[-1], sp.rolling(20, min_periods=1).mean().iloc[-1], sp.rolling(60, min_periods=1).mean().iloc[-1]
calc_vr = (sv.iloc[-1] / sv.iloc[-6:-1].mean()) if len(sv)>=6 and sv.iloc[-6:-1].mean()>0 else 1.0
k_body_pct = (curr_price - df['open'].iloc[-1]) / df['open'].iloc[-1] * 100 if curr_price and df['open'].iloc[-1] else 0
k_body_status = "🔴大阳做多" if k_body_pct > 2 else "🟢大阴/长上影抛压" if k_body_pct < -2 else "⚪多空平衡"
cv_20 = (sp.tail(20).std() / sp.tail(20).mean() * 100) if sp.tail(20).mean() > 0 else 0
cv_status = "🎯筹码高度集中(极易变盘)" if cv_20 < 5 else "💥筹码极度发散" if cv_20 > 15 else "正常"
ma60_status = "🐂站上牛熊线" if curr_price and curr_price > calc_ma60 else "🐻跌破牛熊线(只抢反弹)"
gene_str = "🔥活跃妖股基因" if len(df.tail(15)[df.tail(15).get('pct_chg', 0) >= 9.5]) >= 2 else "🧊股性沉闷"
if len(df) >= 2:
y_h, y_l, t_l, t_h = df['high'].iloc[-2], df['low'].iloc[-2], df['low'].iloc[-1], df['high'].iloc[-1]
if t_l > y_h: gap_str = f"🚀向上跳空极强看多"
elif t_h < y_l: gap_str = f"🕳️向下跳空破位危险"
ma5_trend = "↗️向上" if len(sp)>=2 and sp.rolling(5).mean().iloc[-1] > sp.rolling(5).mean().iloc[-2] else "↘️向下"
tp = (df['high'] + df['low'] + sp) / 3
md = tp.rolling(14, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
cci_val = ((tp - tp.rolling(14, min_periods=1).mean()) / (0.015 * md.replace(0, 1e-9))).iloc[-1]
cci_status = "🔥超买主升" if cci_val > 100 else "🥶超跌错杀" if cci_val < -100 else "震荡"
df['obv'] = (np.sign(sp.diff().fillna(0)) * sv).cumsum()
obv_status = "🌊资金吸筹(OBV向好)" if len(df)>=5 and df['obv'].iloc[-1] > df['obv'].iloc[-5] else "🩸诱多派发(OBV背离)"
l9, h9 = sp.rolling(9, min_periods=1).min(), sp.rolling(9, min_periods=1).max()
j_val = (3 * ((sp - l9) / (h9 - l9).replace(0, 1e-9) * 100).ewm(com=2).mean() - 2 * ((sp - l9) / (h9 - l9).replace(0, 1e-9) * 100).ewm(com=2).mean().ewm(com=2).mean()).iloc[-1]
kdj_status = "💡超卖" if j_val < 0 else "⚠️超买钝化" if j_val > 100 else "安全"
ma20, std20 = sp.rolling(20, min_periods=1).mean(), sp.rolling(20, min_periods=1).std().fillna(0)
boll_status = "🚀破上轨(防砸)" if curr_price and curr_price > (ma20 + 2std20).iloc[-1] else "🕳️破下轨" if curr_price and curr_price < (ma20 - 2std20).iloc[-1] else "中轨运行"
macd = sp.ewm(span=12).mean() - sp.ewm(span=26).mean()
macd_status = "🔴死叉" if (macd - macd.ewm(span=9).mean()).iloc[-1] < 0 else "🟢金叉"
current_atr = pd.concat([df['high']-df['low'], (df['high']-sp.shift()).abs(), (df['low']-sp.shift()).abs()], axis=1).max(axis=1).rolling(14, min_periods=1).mean().iloc[-1]
poc_price = df.groupby(pd.cut(sp, bins=12, duplicates='drop'), observed=False)['volume'].sum().idxmax().mid if sp.nunique() > 1 else (curr_price or 0.0)
except: pass
t_ma5 = today.get('ma5') if today.get('ma5') not in [None, 'N/A', ''] else f"{calc_ma5:.2f}"
t_ma10 = today.get('ma10') if today.get('ma10') not in [None, 'N/A', ''] else f"{calc_ma10:.2f}"
t_ma20 = today.get('ma20') if today.get('ma20') not in [None, 'N/A', ''] else f"{calc_ma20:.2f}"
rt_vr = context.get('realtime', {}).get('volume_ratio') if context.get('realtime', {}).get('volume_ratio') not in [None, 'N/A', ''] else f"{calc_vr:.2f}"
bias_ma5 = f"{((curr_price - calc_ma5)/calc_ma5*100):+.2f}%" if curr_price and calc_ma5 else "0.00%"
pst = "\n## 💎 A股强算引擎数据中心\n"
my_cost, my_shares = None, None
try:
with urllib.request.urlopen(urllib.request.Request("https://docs.google.com/spreadsheets/d/e/2PACX-1vTxwkN9w5AOtcE__HmRKJU7iN088oyEYLdPnWkU6568HzzpIsnhN7x7Z7h5HSKysrkq0s3KKkHirfsO/pub?gid=0&single=true&output=csv", headers={'User-Agent': 'Mozilla/5.0'}), timeout=10) as res:
for row in csv.reader(io.StringIO(res.read().decode('utf-8-sig'))):
if len(row) >= 2 and row[0].strip() and ''.join(filter(str.isdigit, str(row[0]))).zfill(6) == code:
my_cost, my_shares = float(str(row[1]).replace(',', '').strip()), int(float(str(row[2]).replace(',', '').strip())) if len(row) >= 3 and row[2].strip() else 0
except: pass
if my_cost and curr_price:
context['user_cost'], context['user_shares'] = my_cost, my_shares
pst += f"### 💰 持仓底牌\n* 成本价：{my_cost:.2f} 元 | 盈亏：{((curr_price - my_cost)/my_cost100):.2f}%\n 🚨 必须在 personal_cost_review 针对此成本输出策略！\n"
try:
fund_flow = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith('6') else "sz")
flow_desc = f"东方财富内资净流入: {fund_flow.iloc[-1]['主力净流入-净额']/10000:.1f}万"
try:
hk_funds = ak.stock_hsgt_stock_statistics_em()
my_hk = hk_funds[hk_funds['代码'] == code]
if not my_hk.empty:
flow_desc += f" | 北向资金: {'🟢流入' if my_hk.iloc[0]['今日增持估计-市值'] > 0 else '🔴出逃'} {abs(my_hk.iloc[0]['今日增持估计-市值'])/10000:.1f}万"
except: pass
lhb_desc = "暂无数据"
try:
end_date_str = datetime.now().strftime('%Y%m%d')
start_date_str = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
lhb_df = ak.stock_lhb_detail_em(start_date=start_date_str, end_date=end_date_str)
if not lhb_df.empty:
my_lhb = lhb_df[lhb_df['代码'] == code]
lhb_desc = f"🚨 近10日登榜 {len(my_lhb)} 次(有顶级游资运作)" if not my_lhb.empty else "🧊 未上龙虎榜"
except: pass
pst += f"### 🌊 资金与游资雷达\n* {flow_desc}\n* {lhb_desc}\n"
except: pass
try:
db_file = "reports/ai_trade_log.csv"
if os.path.isfile(db_file):
df_log = pd.read_csv(db_file)
df_code = df_log[df_log['Code'] == code].tail(3)
if len(df_code) >= 2:
past_p = float(df_code.iloc[-2]['ClosePrice'])
ai_impact = ((float(curr_price) - past_p) / past_p) * 100
pst += f"### ⚖️ 机构打脸回测雷达\n* 上次分析时价: {past_p:.2f} | 至今变动: {ai_impact:.2f}%\n* 💥 打脸复盘硬指令：如果走势与你看多相反导致亏损，必须在《风险提示》认错！\n"
except: pass
pst += f"""### 🎯 量化三剑客与特化指标
 * K线日内多空: {k_body_status} | MA60牛熊分界: {ma60_status}
 * 筹码变盘雷达: {cv_status} | 涨停基因: {gene_str}
 * 缺口雷达: {gap_str} | MA5斜率: {ma5_trend}
 * CCI 妖股雷达: {cci_status} | OBV 资金潮汐: {obv_status}
 * KDJ: {kdj_status} | BOLL: {boll_status} | MACD: {macd_status}
 * ATR 真实波幅: {current_atr:.2f}元 (网格T+0核心) | POC 历史筹码峰: 约 {poc_price:.2f}元
   """
   return f"""# A股顶级机构决策: {name}({code})
   {pst}
📈 基础盘面
收盘价: {curr_price} | MA5: {t_ma5} | MA10: {t_ma10} | MA20: {t_ma20}
量比: {rt_vr} | 换手率: {context.get('realtime', {}).get('turnover_rate', 'N/A')}%
乖离率(MA5): {bias_ma5} | 筹码获利比例: {syn_profit_ratio:.1f}%
📰 舆情网
引擎一(全网搜索): {news or "无"}
引擎二(Google快讯): {google_news or "无"}
请严格输出 JSON 决策仪表盘。包含所有必需的数值字段，绝对禁止输出 N/A。"""
def _build_market_snapshot(self, context: Dict[str, Any]) -> Dict[str, Any]:
return {"date": context.get('date', '未知'), "close": context.get('today', {}).get('close')}
def _parse_response(self, text: str, code: str, name: str) -> AnalysisResult:
try:
m = re.search(r'({.*})', text, re.DOTALL)
d = json.loads(repair_json(m.group(1) if m else text))
return AnalysisResult(
code=code, name=d.get('stock_name', name), sentiment_score=int(d.get('sentiment_score', 50)),
trend_prediction=d.get('trend_prediction', '震荡'), operation_advice=d.get('operation_advice', '持有'),
decision_type=d.get('decision_type', 'hold'), confidence_level=d.get('confidence_level', '中'),
debate_process=d.get('debate_process'), dashboard=d.get('dashboard'),
analysis_summary=d.get('analysis_summary', '完成'), success=True, raw_response=text
)
except:
return AnalysisResult(code=code, name=name, sentiment_score=50, trend_prediction='未知', operation_advice='观望', analysis_summary="大模型JSON错乱", success=True)
def get_analyzer() -> GeminiAnalyzer:
return GeminiAnalyzer()

