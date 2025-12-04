#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤4: 计算量化得分
使用NSS密度校正方法，按年份独立计算
"""

import argparse
from score_calculator import ScoreCalculator
from config import QUANTITATIVE_CONFIG


def main():
    parser = argparse.ArgumentParser(description='步骤4: 计算量化得分')
    parser.add_argument('--years', nargs='+', type=int, help='要处理的年份，如: 2020 2021 2022')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    args = parser.parse_args()
    
    print("="*80)
    print("步骤4: 计算量化得分 (NSS密度校正)")
    print("="*80)
    print(f"数据库: {args.db}")
    
    calculator = ScoreCalculator(args.db)
    
    # 获取可用年份
    available_years = calculator.db.get_available_years()
    
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
    print(f"参数: k={calculator.k}, eps={calculator.eps}")
    
    # 按年份计算得分
    for year in years_to_process:
        calculator.calculate_scores_for_year(year)
    
    print("\n" + "="*80)
    print("✅ 步骤4完成: 量化得分计算")
    print("="*80)


if __name__ == "__main__":
    main()

