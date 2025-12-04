#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关键词提取器 - 按年份提取（并发版本）
"""

import asyncio
import json
import logging
from typing import List, Dict
from pathlib import Path
from datetime import datetime
import httpx
from openai import AsyncOpenAI

from database_manager_v2 import DatabaseManagerV2
from config import OPENAI_CONFIG, KEYWORD_EXTRACTION_PROMPT, QUANTITATIVE_CONFIG

# 配置日志
log_dir = Path("/root/liujie/nianbao-v2/logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"keyword_extractor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class KeywordExtractor:
    """关键词提取器 - 按年份独立提取（并发版本）"""
    
    def __init__(self, db_path: str):
        self.db = DatabaseManagerV2(db_path)
        self.client = AsyncOpenAI(
            base_url=OPENAI_CONFIG["base_url"],
            api_key=OPENAI_CONFIG["api_key"],
            http_client=httpx.AsyncClient(
                base_url=OPENAI_CONFIG["base_url"],
                follow_redirects=True,
            ),
        )
        self.batch_size = QUANTITATIVE_CONFIG["batch_size_keywords"]
        self.max_concurrent = QUANTITATIVE_CONFIG.get("max_concurrent", 5)
        logger.info(f"初始化关键词提取器: batch_size={self.batch_size}, max_concurrent={self.max_concurrent}")
    
    async def llm_extract_keywords(self, texts: List[str], batch_id: int = 0, max_retries: int = 3) -> List[Dict]:
        """使用LLM提取关键词（带重试）"""
        corpus_text = "\n\n---\n\n".join(texts[:50])
        
        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=OPENAI_CONFIG["model"],
                    messages=[{"role": "user", "content": KEYWORD_EXTRACTION_PROMPT.format(corpus_texts=corpus_text)}],
                    temperature=OPENAI_CONFIG["temperature"],
                    response_format={"type": "json_object"}
                )
                keywords = json.loads(response.choices[0].message.content).get('keywords', [])
                
                if not keywords and attempt < max_retries - 1:
                    logger.warning(f"批次{batch_id}: 空结果，重试 {attempt + 1}/{max_retries}")
                    print(f"      ⚠️ 批次{batch_id}: 空结果，重试 {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2)
                    continue
                
                logger.info(f"批次{batch_id}: 成功提取 {len(keywords)} 个关键词")
                return keywords
            except Exception as e:
                logger.error(f"批次{batch_id}: 错误 (尝试 {attempt + 1}/{max_retries}): {type(e).__name__}: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"      ⚠️ 批次{batch_id}: 错误，重试 {attempt + 1}/{max_retries}: {type(e).__name__}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    print(f"      ❌ 批次{batch_id}: 最终失败: {e}")
                    return []
    
    async def extract_keywords_for_year(self, data_year: int) -> Dict[str, Dict]:
        """为指定年份提取关键词（并发版本）"""
        logger.info(f"开始提取 {data_year} 年关键词")
        print(f"\n  【{data_year}年】提取关键词...")
        
        corpus_data = self.db.get_corpus_texts_by_year(data_year)
        texts = [item['text'] for item in corpus_data if item['text']]
        
        if not texts:
            logger.warning(f"{data_year}年无语料库数据")
            print(f"    ⚠️ 无语料库数据")
            return {}
        
        logger.info(f"{data_year}年语料库: {len(texts)} 段文本")
        print(f"    语料库: {len(texts)} 段")
        
        # 分批提取
        batches = [texts[i:i+self.batch_size] for i in range(0, len(texts), self.batch_size)]
        logger.info(f"{data_year}年分成 {len(batches)} 个批次，并发数: {self.max_concurrent}")
        print(f"    分成 {len(batches)} 个批次（并发数: {self.max_concurrent}）")
        
        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def extract_with_semaphore(batch_id: int, batch: List[str]):
            async with semaphore:
                return await self.llm_extract_keywords(batch, batch_id)
        
        # 并发处理所有批次
        tasks = [extract_with_semaphore(i+1, batch) for i, batch in enumerate(batches)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 合并所有关键词
        all_keywords = {}
        success_count = 0
        
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                logger.error(f"批次{i}异常: {type(result).__name__}: {str(result)}")
                print(f"      ❌ 批次{i}异常")
                continue
            
            if not result:
                continue
            
            success_count += 1
            for kw in result:
                kw_text = kw.get('keyword', '').strip().lower()
                if kw_text and kw_text not in all_keywords:
                    all_keywords[kw_text] = {
                        'keyword': kw_text,
                        'category': kw.get('category', 'unknown'),
                        'context': kw.get('context', ''),
                        'method': 'llm'
                    }
        
        # 统计
        categories = {}
        for kw in all_keywords.values():
            cat = kw['category']
            categories[cat] = categories.get(cat, 0) + 1
        
        logger.info(f"{data_year}年: 成功批次 {success_count}/{len(batches)}, 提取 {len(all_keywords)} 个关键词")
        print(f"    ✓ 成功批次: {success_count}/{len(batches)}")
        print(f"    ✓ 提取 {len(all_keywords)} 个关键词")
        for cat, count in sorted(categories.items()):
            print(f"      - {cat}: {count}")
        
        return all_keywords
    
    async def extract_keywords_from_corpus(self) -> Dict[int, Dict[str, Dict]]:
        """按年份提取所有关键词（并发版本）"""
        logger.info("="*80)
        logger.info("关键词提取器启动 - 按年份独立提取（并发版本）")
        logger.info(f"日志文件: {log_file}")
        logger.info("="*80)
        
        print(f"\n{'='*80}")
        print(f"关键词提取器 - 按年份独立提取（并发版本）")
        print(f"{'='*80}")
        print(f"📝 日志文件: {log_file}")
        
        years = self.db.get_available_years()
        logger.info(f"可用年份: {years}")
        print(f"\n可用年份: {years}")
        
        if not years:
            logger.warning("无可用数据")
            print("⚠️ 无数据")
            return {}
        
        all_years_keywords = {}
        
        for year in years:
            year_keywords = await self.extract_keywords_for_year(year)
            all_years_keywords[year] = year_keywords
            
            # 保存到数据库
            if year_keywords:
                logger.info(f"{year}年: 保存 {len(year_keywords)} 个关键词到数据库")
                self.db.save_keywords(list(year_keywords.values()))
            else:
                logger.warning(f"{year}年: 无关键词可保存")
        
        total_keywords = sum(len(kws) for kws in all_years_keywords.values())
        logger.info("="*80)
        logger.info(f"完成 - 共 {len(years)} 年，{total_keywords} 个关键词")
        logger.info("="*80)
        
        print(f"\n{'='*80}")
        print(f"✅ 完成 - 共{len(years)}年，{total_keywords}个关键词")
        print(f"📝 详细日志: {log_file}")
        print(f"{'='*80}")
        
        return all_years_keywords


def main():
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    extractor = KeywordExtractor(db_path)
    asyncio.run(extractor.extract_keywords_from_corpus())


if __name__ == "__main__":
    main()
