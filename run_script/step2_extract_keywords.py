#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤2: 提取关键词
从语料库中提取关键词（按年份独立提取）
"""

import asyncio
import argparse
from keyword_extractor import KeywordExtractor
from config import QUANTITATIVE_CONFIG


async def main():
    parser = argparse.ArgumentParser(description='步骤2: 提取关键词（并发版本）')
    parser.add_argument('--years', nargs='+', type=int, help='要处理的年份，如: 2020 2021 2022')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    args = parser.parse_args()
    
    print("="*80)
    print("步骤2: 提取关键词（并发版本）")
    print("="*80)
    print(f"数据库: {args.db}")
    
    extractor = KeywordExtractor(args.db)
    
    # 获取可用年份
    available_years = extractor.db.get_available_years()
    print(f"可用年份: {available_years}")
    
    # 筛选要处理的年份
    if args.years:
        years_to_process = [y for y in args.years if y in available_years]
        if not years_to_process:
            print(f"⚠️ 指定的年份 {args.years} 都没有数据")
            print(f"   可用年份: {available_years}")
            return
        print(f"将处理年份: {years_to_process}")
    else:
        years_to_process = available_years
        print(f"将处理所有年份: {years_to_process}")
    
    # 按年份提取关键词（每个年份内部并发处理批次）
    total_keywords = 0
    for year in years_to_process:
        year_keywords = await extractor.extract_keywords_for_year(year)
        if year_keywords:
            extractor.db.save_keywords(list(year_keywords.values()))
            total_keywords += len(year_keywords)
    
    print("\n" + "="*80)
    print(f"✅ 步骤2完成: 关键词提取")
    print(f"   处理年份: {len(years_to_process)} 个")
    print(f"   提取关键词: {total_keywords} 个")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())


