#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ticker智能补全模块
使用SEC Edgar API或LLM根据公司名称/CIK查询ticker
"""

import asyncio
import json
import re
from pathlib import Path
from typing import Optional, Dict
import httpx
from openai import AsyncOpenAI
from config import OPENAI_CONFIG


class TickerResolver:
    """Ticker解析器"""
    
    def __init__(self, cache_file: str = None):
        """
        初始化ticker解析器
        
        Args:
            cache_file: 缓存文件路径，用于避免重复查询
        """
        self.cache_file = cache_file or "/root/liujie/nianbao-v2/results/ticker_cache.json"
        self.cache = self._load_cache()
        self.client = AsyncOpenAI(
            base_url=OPENAI_CONFIG["base_url"],
            api_key=OPENAI_CONFIG["api_key"],
            http_client=httpx.AsyncClient(
                base_url=OPENAI_CONFIG["base_url"],
                follow_redirects=True,
            ),
        )
    
    def _load_cache(self) -> Dict:
        """加载缓存"""
        cache_path = Path(self.cache_file)
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载ticker缓存失败: {e}")
                return {}
        return {}
    
    def _save_cache(self):
        """保存缓存"""
        try:
            cache_path = Path(self.cache_file)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 保存ticker缓存失败: {e}")
    
    def _normalize_company_name(self, company_name: str) -> str:
        """标准化公司名称用于缓存键"""
        # 转小写，移除标点和多余空格
        normalized = company_name.lower().strip()
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized
    
    async def resolve_ticker_via_llm(self, company_name: str, cik: str) -> Optional[str]:
        """
        使用LLM推断ticker
        
        Args:
            company_name: 公司名称
            cik: CIK编号
            
        Returns:
            ticker或None
        """
        prompt = f"""Given the following company information, please provide the stock ticker symbol (exchange symbol).

Company Name: {company_name}
CIK: {cik}

Requirements:
1. Return ONLY the ticker symbol (e.g., "AAPL", "GOOGL", "MSFT")
2. If you cannot determine the ticker with high confidence, return "UNKNOWN"
3. Common cases:
   - "Google Inc." or "Alphabet Inc." → "GOOGL" or "GOOG"
   - "Apple Inc." → "AAPL"
   - "Microsoft Corporation" → "MSFT"
   - "General Motors" → "GM"
   
Return format: Just the ticker symbol, nothing else.
"""
        
        try:
            response = await self.client.chat.completions.create(
                model=OPENAI_CONFIG["model"],
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=0.1,  # 低温度以获得确定性结果
                max_tokens=10
            )
            
            ticker = response.choices[0].message.content.strip().upper()
            
            # 验证ticker格式（通常是1-5个字母）
            if ticker and ticker != "UNKNOWN" and re.match(r'^[A-Z]{1,5}$', ticker):
                return ticker
            
        except Exception as e:
            print(f"  ⚠️ LLM查询ticker失败: {e}")
        
        return None
    
    async def resolve_ticker(self, company_name: str, cik: str) -> Optional[str]:
        """
        解析ticker（先查缓存，再用LLM）
        
        Args:
            company_name: 公司名称
            cik: CIK编号
            
        Returns:
            ticker或None
        """
        # 标准化公司名称作为缓存键
        cache_key = self._normalize_company_name(company_name)
        
        # 检查缓存
        if cache_key in self.cache:
            cached_ticker = self.cache[cache_key]
            if cached_ticker and cached_ticker != "UNKNOWN":
                return cached_ticker
        
        # 也尝试用CIK作为键
        if cik and cik in self.cache:
            cached_ticker = self.cache[cik]
            if cached_ticker and cached_ticker != "UNKNOWN":
                return cached_ticker
        
        # 使用LLM查询
        print(f"  🔍 使用LLM查询ticker: {company_name} (CIK: {cik})")
        ticker = await self.resolve_ticker_via_llm(company_name, cik)
        
        if ticker:
            print(f"  ✓ 找到ticker: {ticker}")
            # 保存到缓存
            self.cache[cache_key] = ticker
            if cik:
                self.cache[cik] = ticker
            self._save_cache()
            return ticker
        
        # 如果都失败，标记为UNKNOWN并缓存
        print(f"  ⚠️ 无法解析ticker，将使用 UNKNOWN_{cik}")
        self.cache[cache_key] = "UNKNOWN"
        if cik:
            self.cache[cik] = "UNKNOWN"
        self._save_cache()
        
        return None


# 全局实例（单例模式）
_resolver_instance = None


def get_ticker_resolver() -> TickerResolver:
    """获取全局ticker解析器实例"""
    global _resolver_instance
    if _resolver_instance is None:
        _resolver_instance = TickerResolver()
    return _resolver_instance


async def resolve_ticker(company_name: str, cik: str) -> Optional[str]:
    """
    便捷函数：解析ticker
    
    Args:
        company_name: 公司名称
        cik: CIK编号
        
    Returns:
        ticker或None
    """
    resolver = get_ticker_resolver()
    return await resolver.resolve_ticker(company_name, cik)


async def main():
    """测试函数"""
    test_cases = [
        ("Google Inc.", "1288776"),
        ("Apple Inc.", "0320193"),
        ("General Motors Co", "1467858"),
        ("Unknown Company XYZ", "9999999"),
    ]
    
    for company_name, cik in test_cases:
        print(f"\n测试: {company_name} (CIK: {cik})")
        ticker = await resolve_ticker(company_name, cik)
        print(f"结果: {ticker or 'None'}")


if __name__ == "__main__":
    asyncio.run(main())


