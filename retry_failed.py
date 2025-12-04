#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重试失败的年报处理
只处理之前提取失败的文件（有公司记录但没有corpus数据）
"""

import asyncio
import sqlite3
from pathlib import Path
import sys
from corpus_builder import CorpusBuilder

def get_failed_companies(db_path: str, year: int):
    """获取提取失败的公司列表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取所有在companies表但没有corpus数据的公司
    cursor.execute(f"""
        SELECT c.ticker, c.file_path 
        FROM companies_{year} c
        WHERE c.ticker NOT IN (
            SELECT DISTINCT ticker 
            FROM corpus_{year} 
            WHERE text_segment IS NOT NULL AND LENGTH(text_segment) > 0
        )
        AND c.file_path IS NOT NULL
    """)
    
    failed = cursor.fetchall()
    conn.close()
    
    return [(ticker, Path(file_path)) for ticker, file_path in failed if file_path]


async def retry_year(db_path: str, year: int, limit: int = None):
    """重试指定年份的失败文件"""
    print(f"\n{'='*80}")
    print(f"重试失败文件 - {year}年")
    print(f"{'='*80}")
    
    # 获取失败的文件
    failed_companies = get_failed_companies(db_path, year)
    
    if not failed_companies:
        print(f"✅ {year}年没有需要重试的文件")
        return
    
    print(f"❌ 发现 {len(failed_companies)} 个失败的公司")
    
    # 应用limit
    if limit:
        failed_companies = failed_companies[:limit]
        print(f"📝 本次处理前 {limit} 个")
    
    # 提取文件路径
    failed_files = [file_path for _, file_path in failed_companies if file_path.exists()]
    
    if not failed_files:
        print("❌ 没有找到有效的文件路径")
        return
    
    print(f"\n开始重试处理...")
    print(f"文件数量: {len(failed_files)}")
    
    # 显示将要处理的文件
    print("\n将要处理的公司:")
    for ticker, file_path in failed_companies[:10]:
        print(f"  - {ticker}: {file_path.name}")
    if len(failed_companies) > 10:
        print(f"  ... 还有 {len(failed_companies) - 10} 个")
    
    # 创建builder并处理
    builder = CorpusBuilder(db_path)
    
    # 注意：不需要删除旧记录，因为 save_company 使用 INSERT OR REPLACE
    # 只是之前的记录没有corpus数据而已
    
    # 批量处理
    results = await builder.build_corpus_batch(failed_files)
    
    # 统计结果
    success_count = sum(1 for r in results if isinstance(r, dict) and r.get('segments_count', 0) > 0)
    still_failed = len(failed_files) - success_count
    
    print(f"\n{'='*80}")
    print(f"重试结果:")
    print(f"  ✅ 成功: {success_count}/{len(failed_files)}")
    print(f"  ❌ 仍然失败: {still_failed}")
    if still_failed > 0:
        print(f"\n建议:")
        print(f"  1. 检查 LLM API 配置和余额")
        print(f"  2. 再次运行重试：python3 retry_failed.py {year}")
        print(f"  3. 检查失败文件的HTML质量")
    print(f"{'='*80}\n")


async def retry_all_years(db_path: str, limit_per_year: int = None):
    """重试所有年份的失败文件"""
    for year in range(2014, 2025):
        await retry_year(db_path, year, limit_per_year)


if __name__ == "__main__":
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    
    if len(sys.argv) > 1:
        year = int(sys.argv[1])
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
        asyncio.run(retry_year(db_path, year, limit))
    else:
        print("用法:")
        print("  python3 retry_failed.py 2024         # 重试2024年所有失败的文件")
        print("  python3 retry_failed.py 2024 10      # 重试2024年前10个失败的文件")
        print("  python3 retry_failed.py all          # 重试所有年份")

