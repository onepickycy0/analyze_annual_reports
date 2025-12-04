#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤3: 计算TF-IDF
按年份独立计算TF-IDF得分
"""

import argparse
from tfidf_calculator import TFIDFCalculator
from config import QUANTITATIVE_CONFIG


def main():
    parser = argparse.ArgumentParser(description='步骤3: 计算TF-IDF')
    parser.add_argument('--years', nargs='+', type=int, help='要处理的年份，如: 2020 2021 2022')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    args = parser.parse_args()
    
    print("="*80)
    print("步骤3: 计算TF-IDF")
    print("="*80)
    print(f"数据库: {args.db}")
    
    calculator = TFIDFCalculator(args.db)
    
    # 获取可用年份和关键词
    available_years = calculator.db.get_available_years()
    keywords = calculator.db.get_all_keywords()
    
    if not keywords:
        print("⚠️ 无关键词数据，请先运行步骤2")
        return
    
    # 筛选要处理的年份
    if args.years:
        years_to_process = [y for y in args.years if y in available_years]
        if not years_to_process:
            print(f"⚠️ 指定的年份 {args.years} 都没有数据")
            print(f"   可用年份: {available_years}")
            return
    else:
        years_to_process = available_years
    
    print(f"处理年份: {years_to_process}")
    print(f"关键词数: {len(keywords)}")
    
    # 按年份计算TF-IDF
    for year in years_to_process:
        calculator.calculate_tfidf_for_year(year, keywords)
    
    print("\n" + "="*80)
    print("✅ 步骤3完成: TF-IDF计算")
    print("="*80)


if __name__ == "__main__":
    main()


