#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
语料库构建器
读取年报HTML，用LLM提取五维度数据和文本段落
"""

import asyncio
import json
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup
import httpx
from openai import AsyncOpenAI

from database_manager_v2 import DatabaseManagerV2
from config import OPENAI_CONFIG, CORPUS_EXTRACTION_PROMPT, QUANTITATIVE_CONFIG
from ticker_resolver import resolve_ticker

# 配置日志
log_dir = Path("/root/liujie/nianbao-v2/logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"corpus_builder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CorpusBuilder:
    """语料库构建器"""
    
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
        self.max_concurrent = QUANTITATIVE_CONFIG["max_concurrent"]
    
    def parse_html(self, html_file: Path) -> str:
        """解析HTML文件，提取文本内容"""
        with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # 移除script和style标签
        for tag in soup(['script', 'style', 'meta', 'link']):
            tag.decompose()
        
        # 提取文本
        text = soup.get_text(separator='\n', strip=True)
        
        # 清理文本
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        return text
    
    def extract_metadata(self, html_file: Path) -> Dict[str, Any]:
        """从文件名提取元数据"""
        filename = html_file.stem
        parts = filename.split('_')
        
        if len(parts) >= 6:
            # 文件名格式: cik_fiscal_year_end_ticker_company_name_parts..._form_type_report_date
            # 公司名可能包含多个下划线分隔的部分（原本是空格）
            # parts[0]: cik
            # parts[1]: fiscal_year_end
            # parts[2]: ticker
            # parts[3:-2]: company_name (多个部分用空格连接)
            # parts[-2]: form_type (如 10-K)
            # parts[-1]: report_date
            
            company_name_parts = parts[3:-2]
            company_name = ' '.join(company_name_parts)
            
            return {
                'cik': parts[0],
                'fiscal_year_end': parts[1],
                'ticker': parts[2],
                'company_name': company_name,
                'report_date': parts[-1],
                'data_year': int(parts[1].split('-')[0]),
                'needs_ticker_resolution': parts[2].lower() in ['none', 'unknown', ''] or not parts[2]
            }
        
        return {
            'cik': '',
            'ticker': filename[:10],
            'company_name': 'Unknown',
            'fiscal_year_end': '',
            'report_date': '',
            'data_year': 2025,
            'needs_ticker_resolution': True
        }
    
    def estimate_tokens(self, text: str) -> int:
        """粗略估算token数（1 token ≈ 4 字符）"""
        return len(text) // 4
    
    def split_text_into_chunks(self, text: str, max_tokens: int = 100000) -> list:
        """
        将长文本分块，保持段落完整性
        
        Args:
            text: 要分块的文本
            max_tokens: 每块最大token数（实际按max_chars=max_tokens*4计算）
        """
        max_chars = max_tokens * 4  # token到字符的转换
        
        if len(text) <= max_chars:
            return [text]
        
        chunks = []
        current_chunk = ""
        paragraphs = text.split('\n\n')
        
        for para in paragraphs:
            # 如果单个段落就超过max_chars，强制分割
            if len(para) > max_chars:
                # 先保存当前chunk
                if current_chunk:
                    chunks.append(current_chunk.strip())
                    current_chunk = ""
                
                # 将超长段落按字符强制分割
                for i in range(0, len(para), max_chars):
                    chunks.append(para[i:i+max_chars])
                continue
            
            # 正常的段落分块逻辑
            if len(current_chunk) + len(para) + 2 <= max_chars:
                current_chunk += para + '\n\n'
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = para + '\n\n'
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    async def llm_extract(self, text: str, max_retries: int = 3, ticker: str = "UNKNOWN") -> Dict:
        """使用LLM提取结构化数据和文本段落（带重试）"""
        # 检查文本长度
        estimated_tokens = self.estimate_tokens(text)
        max_tokens = 100000  # GPT-4-turbo 支持 128K，留一些余量
        
        # 如果文本过长，分块处理
        if estimated_tokens > max_tokens:
            logger.warning(f"[{ticker}] 文本过长 (估计 {estimated_tokens:,} tokens)，启用分块处理...")
            print(f"  ⚠️ 文本过长 (估计 {estimated_tokens:,} tokens)，启用分块处理...")
            return await self.llm_extract_chunked(text, ticker)
        
        # 带重试的正常处理
        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=OPENAI_CONFIG["model"],
                    messages=[{
                        "role": "user",
                        "content": CORPUS_EXTRACTION_PROMPT.format(text=text)
                    }],
                    temperature=OPENAI_CONFIG["temperature"],
                    response_format={"type": "json_object"}
                )
                
                content = response.choices[0].message.content
                result = json.loads(content)
                
                # 验证结果
                if not result.get("text_segments"):
                    logger.warning(f"[{ticker}] 提取结果为空 (尝试 {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        print(f"  ⚠️ 提取结果为空，重试 {attempt + 1}/{max_retries}")
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.error(f"[{ticker}] 所有重试后仍然返回空结果")
                
                logger.info(f"[{ticker}] 成功提取 {len(result.get('text_segments', []))} 个段落")
                return result
                
            except json.JSONDecodeError as e:
                logger.error(f"[{ticker}] JSON解析错误 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"  ⚠️ JSON解析错误，重试 {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2)
                    continue
                else:
                    print(f"  ❌ JSON最终解析失败: {e}")
                    logger.error(f"[{ticker}] JSON解析最终失败: {e}")
                    return {"structured_data": {}, "text_segments": []}
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[{ticker}] LLM调用错误 (尝试 {attempt + 1}/{max_retries}): {type(e).__name__}: {error_msg}")
                
                # 检查是否是token限制错误
                if 'maximum context length' in error_msg.lower() or 'token' in error_msg.lower():
                    print(f"  ⚠️ Token限制错误，切换到分块处理...")
                    return await self.llm_extract_chunked(text, ticker)
                
                if attempt < max_retries - 1:
                    print(f"  ⚠️ LLM错误 (尝试 {attempt + 1}/{max_retries}): {type(e).__name__}")
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                    continue
                else:
                    print(f"  ❌ LLM最终失败: {type(e).__name__}: {str(e)}")
                    print(f"     模型: {OPENAI_CONFIG['model']}")
                    logger.error(f"[{ticker}] LLM最终失败: {type(e).__name__}: {error_msg}, 模型: {OPENAI_CONFIG['model']}")
                    return {"structured_data": {}, "text_segments": []}
    
    async def llm_extract_chunked(self, text: str, ticker: str = "UNKNOWN") -> Dict:
        """分块处理长文本"""
        chunks = self.split_text_into_chunks(text, max_tokens=100000)
        logger.info(f"[{ticker}] 分成 {len(chunks)} 块处理...")
        print(f"  📦 分成 {len(chunks)} 块处理...")
        
        all_structured_data = {}
        all_text_segments = []
        
        for i, chunk in enumerate(chunks, 1):
            print(f"     处理第 {i}/{len(chunks)} 块...")
            logger.info(f"[{ticker}] 处理第 {i}/{len(chunks)} 块...")
            # 使用带重试的方法
            result = await self._llm_extract_single_chunk(chunk, ticker)
            
            # 合并结构化数据
            structured = result.get('structured_data', {})
            for key, value in structured.items():
                if key not in all_structured_data:
                    all_structured_data[key] = value if isinstance(value, list) else [value]
                elif isinstance(value, list):
                    all_structured_data[key].extend(value)
                else:
                    if isinstance(all_structured_data[key], list):
                        all_structured_data[key].append(value)
                    else:
                        all_structured_data[key] = [all_structured_data[key], value]
            
            # 合并文本段落
            segments = result.get('text_segments', [])
            all_text_segments.extend(segments)
        
        logger.info(f"[{ticker}] 分块处理完成：合并了 {len(chunks)} 块数据，共 {len(all_text_segments)} 个段落")
        print(f"  ✓ 分块处理完成：合并了 {len(chunks)} 块数据")
        
        return {
            "structured_data": all_structured_data,
            "text_segments": all_text_segments
        }
    
    async def _llm_extract_single_chunk(self, text: str, ticker: str = "UNKNOWN", max_retries: int = 3) -> Dict:
        """单个块的LLM提取（带重试）"""
        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=OPENAI_CONFIG["model"],
                    messages=[{
                        "role": "user",
                        "content": CORPUS_EXTRACTION_PROMPT.format(text=text)
                    }],
                    temperature=OPENAI_CONFIG["temperature"],
                    response_format={"type": "json_object"}
                )
                
                content = response.choices[0].message.content
                result = json.loads(content)
                
                # 验证结果
                if not result.get("text_segments") and attempt < max_retries - 1:
                    logger.warning(f"[{ticker}] 块提取结果为空 (尝试 {attempt + 1}/{max_retries})")
                    print(f"        ⚠️ 空结果，重试 {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2)
                    continue
                
                return result
                
            except json.JSONDecodeError as e:
                logger.error(f"[{ticker}] 块JSON解析错误 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"        ⚠️ JSON错误，重试 {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2)
                    continue
                return {"structured_data": {}, "text_segments": []}
                    
            except Exception as e:
                logger.error(f"[{ticker}] 块提取错误 (尝试 {attempt + 1}/{max_retries}): {type(e).__name__}: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"        ⚠️ {type(e).__name__}，重试 {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                print(f"        ❌ 最终失败: {type(e).__name__}")
                return {"structured_data": {}, "text_segments": []}
    
    async def build_corpus_for_report(self, html_file: Path):
        """为单个年报构建语料库"""
        logger.info(f"开始处理: {html_file.name}")
        print(f"\n处理: {html_file.name}")
        
        try:
            # 1. 提取元数据
            metadata = self.extract_metadata(html_file)
            ticker = metadata['ticker']
            data_year = metadata['data_year']
            
            # 2. Ticker智能补全
            if metadata.get('needs_ticker_resolution', False):
                logger.info(f"[{ticker}] 检测到ticker为None/Unknown，启动智能补全...")
                print(f"  检测到ticker为None/Unknown，启动智能补全...")
                resolved_ticker = await resolve_ticker(metadata['company_name'], metadata['cik'])
                if resolved_ticker:
                    ticker = resolved_ticker
                    logger.info(f"[{ticker}] Ticker补全成功: {ticker}")
                    print(f"  ✓ Ticker补全成功: {ticker}")
                else:
                    ticker = f"UNKNOWN_{metadata['cik']}"
                    logger.warning(f"[{ticker}] 无法解析ticker，使用: {ticker}")
                    print(f"  ⚠️ 无法解析ticker，使用: {ticker}")
            
            logger.info(f"[{ticker}] 公司: {metadata['company_name']}, 年份: {data_year}")
            print(f"  公司: {metadata['company_name']} ({ticker})")
            print(f"  年份: {data_year}")
            
            # 2. 解析HTML
            print(f"  解析HTML...")
            text = self.parse_html(html_file)
            estimated_tokens = self.estimate_tokens(text)
            logger.info(f"[{ticker}] 提取文本: {len(text):,} 字符 (估计 ~{estimated_tokens:,} tokens)")
            print(f"  提取文本: {len(text):,} 字符 (估计 ~{estimated_tokens:,} tokens)")
            
            # 3. LLM分析
            print(f"  LLM分析中...")
            logger.info(f"[{ticker}] 开始LLM分析...")
            result = await self.llm_extract(text, ticker=ticker)
            
            structured_data = result.get('structured_data', {})
            text_segments = result.get('text_segments', [])
            
            logger.info(f"[{ticker}] 结构化数据: {len(structured_data)} 个维度, 文本段落: {len(text_segments)} 个")
            print(f"  ✓ 结构化数据: {len(structured_data)} 个维度")
            print(f"  ✓ 文本段落: {len(text_segments)} 个")
            
            # 检查是否提取失败
            if len(text_segments) == 0:
                logger.error(f"[{ticker}] ❌ 提取失败：没有文本段落")
                print(f"  ❌ 警告：没有提取到文本段落")
            
            # 4. 保存到数据库（按年份独立表）
            self.db.save_company(
                ticker=ticker,
                data_year=data_year,
                cik=metadata['cik'],
                company_name=metadata['company_name'],
                fiscal_year_end=metadata['fiscal_year_end'],
                report_date=metadata['report_date'],
                file_path=str(html_file)
            )
            
            self.db.save_structured_data(ticker, data_year, structured_data)
            self.db.save_text_segments(ticker, data_year, text_segments)
            
            logger.info(f"[{ticker}] ✅ 处理完成")
            
            return {
                'ticker': ticker,
                'company_name': metadata['company_name'],
                'segments_count': len(text_segments)
            }
            
        except Exception as e:
            logger.error(f"处理文件失败 {html_file.name}: {type(e).__name__}: {str(e)}", exc_info=True)
            print(f"  ❌ 处理失败: {type(e).__name__}: {str(e)}")
            raise
    
    async def build_corpus_batch(self, html_files: list):
        """批量处理多个年报（并发）"""
        logger.info(f"{'='*80}")
        logger.info(f"语料库构建器 - 批量处理开始")
        logger.info(f"文件数量: {len(html_files)}, 并发数: {self.max_concurrent}")
        logger.info(f"日志文件: {log_file}")
        logger.info(f"{'='*80}")
        
        print(f"\n{'='*80}")
        print(f"语料库构建器 - 批量处理")
        print(f"{'='*80}")
        print(f"文件数量: {len(html_files)}")
        print(f"并发数: {self.max_concurrent}")
        print(f"📝 日志文件: {log_file}")
        
        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_with_semaphore(file):
            async with semaphore:
                return await self.build_corpus_for_report(file)
        
        # 并发处理
        tasks = [process_with_semaphore(f) for f in html_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计结果
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get('segments_count', 0) > 0)
        failed_count = sum(1 for r in results if isinstance(r, dict) and r.get('segments_count', 0) == 0)
        error_count = sum(1 for r in results if not isinstance(r, dict))
        
        logger.info(f"{'='*80}")
        logger.info(f"批量处理完成: 成功={success_count}, 数据为空={failed_count}, 异常={error_count}")
        logger.info(f"{'='*80}")
        
        print(f"\n{'='*80}")
        print(f"✅ 批量处理完成")
        print(f"  ✅ 成功（有数据）: {success_count}/{len(html_files)}")
        print(f"  ⚠️  数据为空: {failed_count}")
        print(f"  ❌ 异常错误: {error_count}")
        print(f"  📝 详细日志: {log_file}")
        print(f"{'='*80}")
        
        # 显示失败的文件
        if failed_count > 0 or error_count > 0:
            print(f"\n失败的文件:")
            for i, (result, file) in enumerate(zip(results, html_files)):
                if isinstance(result, dict):
                    if result.get('segments_count', 0) == 0:
                        print(f"  ⚠️  {result.get('ticker', 'UNKNOWN')}: {file.name} (数据为空)")
                elif isinstance(result, Exception):
                    print(f"  ❌ {file.name}: {type(result).__name__}: {str(result)}")
        
        return results


def main():
    """测试函数"""
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python corpus_builder.py <data_dir> [limit]")
        return
    
    data_dir = Path(sys.argv[1])
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    builder = CorpusBuilder(db_path)
    
    # 获取HTML文件
    html_files = sorted(data_dir.glob("*.html"))
    if limit:
        html_files = html_files[:limit]
    
    # 运行批量处理
    asyncio.run(builder.build_corpus_batch(html_files))


if __name__ == "__main__":
    main()

