#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤5: 生成Excel报告
按年份生成独立的sheets
"""

import argparse
from datetime import datetime
from report_generator_v2 import ReportGeneratorV2
from config import QUANTITATIVE_CONFIG


def main():
    parser = argparse.ArgumentParser(description='步骤5: 生成Excel报告')
    parser.add_argument('--output', type=str, help='输出文件路径')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    args = parser.parse_args()
    
    # 默认输出文件名
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{QUANTITATIVE_CONFIG['output_dir']}/annual_reports_analysis_{timestamp}.xlsx"
    
    print("="*80)
    print("步骤5: 生成Excel报告")
    print("="*80)
    print(f"数据库: {args.db}")
    print(f"输出文件: {output_file}")
    
    generator = ReportGeneratorV2(args.db)
    generator.generate_full_report(output_file)
    
    print("\n" + "="*80)
    print("✅ 步骤5完成: Excel报告生成")
    print("="*80)


if __name__ == "__main__":
    main()


