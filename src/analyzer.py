# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 大盘复盘分析层
===================================

职责：
1. 抓取大盘核心指数和涨跌停数据
2. 爬取全网最新大盘宏观新闻
3. 结合 Gemini 模型生成宏观复盘报告
4. 【抗限流】加入了动态激进降级和 Google 429 智能休眠机制
"""

import json
import logging
import time
import re
from datetime import datetime
from typing import Dict, Any, Optional

from src.config import get_config
from src.agent.llm_adapter import get_thinking_extra_body

logger = logging.getLogger(__name__)

class MarketAnalyzer:
    """大盘复盘分析器"""

    SYSTEM_PROMPT = """你是一位国内顶尖的宏观策略分析师兼 A股游资总舵主。
你的任务是根据提供的【大盘指数】、【涨跌统计】、【板块异动】和【市场新闻】，写一份犀利、深刻、接地气的大盘复盘报告。

报告风格要求：
1. 语言要极度犀利，一针见血，敢于下判断（例如：今天是主升浪、还是诱多骗炮、还是极致冰点）。
2. 严禁说废话，不准用“可能”、“也许”等模糊词汇。
3. 必须包含具体的【宏观水温】研判，并给出明天【散户的实战操作建议】（例如：建议管住手、或者建议猛干科技股）。

输出格式（请直接输出 Markdown 格式的报告）：

# 📊 A股全景复盘报告 (YYYY-MM-DD)

## 🌡️ 市场水温与核心定调
(用一句话定调今天的大盘情绪，并说明赚钱效应在哪)

## 📈 盘面深度透视
(结合涨跌停家数、主力资金、缩放量情况，剖析主力的真实意图)

## 🌪️ 龙虎风口与板块轮动
(解读今天领涨/领跌板块背后的政策或资金逻辑)

## ⚔️ 明日实战军规
(给出极其具体的仓位建议和防守/进攻方向)
"""

    def __init__(self, api_key: Optional[str] = None):
        self.config = get_config()
        self._api_key = api_key or self.config.gemini_api_key
        self.model = None
        self._current_model_name = None
        self._using_fallback = False

        if self._api_key and not self._api_key.startswith('your_'):
            try:
                import google.generativeai as genai
                genai.configure(api_key=self._api_key)
                self.model = genai.GenerativeModel(
                    model_name=self.config.gemini_model,
                    system_instruction=self.SYSTEM_PROMPT
                )
                self._current_model_name = self.config.gemini_model
            except Exception as e:
                logger.error(f"大盘复盘 Gemini 初始化失败: {e}")
                self.model = None

    def _switch_to_fallback_model(self) -> bool:
        """中途被限流时，动态降级到备用模型"""
        try:
            import google.generativeai as genai
            fallback_model = self.config.gemini_model_fallback
            self.model = genai.GenerativeModel(
                model_name=fallback_model,
                system_instruction=self.SYSTEM_PROMPT
            )
            self._current_model_name = fallback_model
            self._using_fallback = True
            logger.warning(f"🔄 [大盘复盘] 触发防宕机降级机制，成功切换到备选模型: {fallback_model}")
            return True
        except Exception as e:
            logger.error(f"❌ [大盘复盘] 切换备选模型失败: {e}")
            return False

    def generate_review(self, market_data: Dict[str, Any], news_text: str) -> Optional[str]:
        if not self.model:
            logger.error("[大盘复盘] 模型未初始化，无法生成报告")
            return None

        prompt = f"""请根据以下真实市场数据生成今日复盘报告。

### 数据时间: {market_data.get('date', datetime.now().strftime('%Y-%m-%d'))}

### 大盘核心指数:
{json.dumps(market_data.get('indices', {}), ensure_ascii=False, indent=2)}

### 市场涨跌统计 (情绪冰点与高潮指标):
- 上涨家数: {market_data.get('stats', {}).get('up_count', '未知')}
- 下跌家数: {market_data.get('stats', {}).get('down_count', '未知')}
- 涨停家数: {market_data.get('stats', {}).get('limit_up_count', '未知')}
- 跌停家数: {market_data.get('stats', {}).get('limit_down_count', '未知')}
- 两市总成交额: {market_data.get('stats', {}).get('total_volume', '未知')} 亿

### 板块风向标:
- 领涨板块: {', '.join(market_data.get('sectors', {}).get('top_up', []))}
- 领跌板块: {', '.join(market_data.get('sectors', {}).get('top_down', []))}

### 全网重大宏观与行业新闻:
{news_text}

请像一个没有感情的游资机器一样，严格按照系统指令的 Markdown 格式输出你的研判！"""

        logger.info("[大盘] 开始调用大模型生成复盘报告...")
        
        # ====== 终极防限流与自动降级引擎 ======
        max_retries = max(self.config.gemini_max_retries, 8)
        tried_fallback = self._using_fallback
        
        for attempt in range(max_retries):
            try:
                response = self.model.generate_content(
                    prompt,
                    generation_config={"temperature": self.config.gemini_temperature},
                    request_options={"timeout": 120}
                )
                if response and response.text:
                    logger.info("[大盘] 报告生成成功！")
                    return response.text
            except Exception as api_e:
                err_str = str(api_e).lower()
                # 捕获 429 额度限制或请求过多
                if '429' in err_str or 'quota' in err_str or 'rate' in err_str:
                    logger.warning(f"⚠️ [大盘复盘] 触发限流 (429)，准备执行自救方案... ({attempt+1}/{max_retries})")
                    
                    # 激进降级策略：第二次被限流就直接切模型，不等了
                    if attempt >= 1 and not tried_fallback:
                        logger.warning("🔄 [大盘复盘] 尝试降级至备用模型 (Gemini-2.5-Flash) 以突破封锁...")
                        if self._switch_to_fallback_model():
                            tried_fallback = True
                            time.sleep(3) # 切换后小憩3秒马上接着干
                            continue
                    
                    # 听从 Google 指令动态休眠
                    match = re.search(r'retry in (\d+\.?\d*)s', err_str)
                    sleep_time = float(match.group(1)) + 5.0 if match else 30.0
                    logger.warning(f"⏳ [大盘复盘] 强制休眠等待 API 额度恢复 {sleep_time:.2f} 秒...")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"❌ [大盘复盘] API 错误: {str(api_e)[:100]}，休眠 5 秒... ({attempt+1}/{max_retries})")
                    time.sleep(5)
                
                if attempt == max_retries - 1:
                    logger.error("[大盘复盘] 达到最大重试次数，彻底失败。")
                    return None
                    
        return None
