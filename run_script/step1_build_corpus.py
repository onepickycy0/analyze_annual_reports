#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤1: 构建语料库
读取年报HTML文件，提取结构化数据和文本段落
"""

import asyncio
import argparse
from pathlib import Path
from corpus_builder import CorpusBuilder
from config import QUANTITATIVE_CONFIG


async def main():
    parser = argparse.ArgumentParser(description='步骤1: 构建语料库')
    parser.add_argument('--years', nargs='+', type=int, help='要处理的年份，如: 2020 2021 2022')
    parser.add_argument('--limit', type=int, help='每年处理的文件数限制')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    args = parser.parse_args()
    
    print("="*80)
    print("步骤1: 构建语料库")
    print("="*80)
    print(f"数据库: {args.db}")
    
    # 扫描年份目录
    base_reports_dir = Path("/root/liujie/nianbao-v2results/reports")
    all_year_dirs = sorted([d for d in base_reports_dir.iterdir() 
                            if d.is_dir() and d.name.startswith('20') and len(d.name) == 4])
    
    # 筛选要处理的年份
    if args.years:
        year_strs = [str(y) for y in args.years]
        dirs_to_process = [d for d in all_year_dirs if d.name in year_strs]
    else:
        dirs_to_process = all_year_dirs
    
    # 收集HTML文件
    all_html_files = []
    for year_dir in dirs_to_process:
        html_files = sorted(year_dir.glob("*.html"))
        if args.limit:
            html_files = html_files[:args.limit]
        all_html_files.extend(html_files)
        print(f"  {year_dir.name}: {len(html_files)} 个文件")
    
    print(f"\n总计: {len(all_html_files)} 个文件（跨{len(dirs_to_process)}年）")
    
    if not all_html_files:
        print("⚠️ 未找到HTML文件")
        return
    
    # 构建语料库
    builder = CorpusBuilder(args.db)
    await builder.build_corpus_batch(all_html_files)
    
    print("\n" + "="*80)
    print("✅ 步骤1完成: 语料库构建")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())

