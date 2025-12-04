#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
导出量化得分到CSV文件
"""

import argparse
import pandas as pd
from datetime import datetime
from database_manager_v2 import DatabaseManagerV2
from config import QUANTITATIVE_CONFIG


def export_scores_to_csv(db_path: str, output_file: str = None):
    """导出所有年份的得分数据到CSV文件"""
    
    print("="*80)
    print("导出量化得分到CSV")
    print("="*80)
    print(f"数据库: {db_path}")
    
    db = DatabaseManagerV2(db_path)
    
    # 获取所有有数据的年份
    years = db.get_available_years()
    print(f"可用年份: {years}")
    
    if not years:
        print("⚠️ 没有数据可导出")
        return
    
    # 收集所有数据
    all_data = []
    
    for year in years:
        print(f"\n处理 {year} 年数据...")
        
        cursor = db.conn.cursor()
        cursor.execute(f'''
        SELECT 
            c.ticker,
            c.company_name,
            q.investment_attitude_score,
            q.expansion_score,
            q.contraction_score,
            q.china_investment_score,
            q.china_positive_score,
            q.china_negative_score,
            q.non_china_investment_score,
            q.non_china_raw_score,
            q.china_investment_density,
            q.non_china_investment_density,
            q.china_investment_density_normalized,
            q.non_china_investment_density_normalized,
            q.total_keywords_count,
            q.calculation_date
        FROM companies_{year} c
        LEFT JOIN quantitative_scores_{year} q ON c.ticker = q.ticker
        WHERE q.ticker IS NOT NULL
        ORDER BY c.ticker
        ''')
        
        rows = cursor.fetchall()
        print(f"  找到 {len(rows)} 家公司")
        
        for row in rows:
            all_data.append({
                'year': year,
                'ticker': row[0],
                'company_name': row[1],
                'investment_attitude_score': row[2],
                'expansion_score': row[3],
                'contraction_score': row[4],
                'china_investment_score': row[5],
                'china_positive_score': row[6],
                'china_negative_score': row[7],
                'non_china_investment_score': row[8],
                'non_china_raw_score': row[9],
                'china_investment_density': row[10],
                'non_china_investment_density': row[11],
                'china_investment_density_normalized': row[12],
                'non_china_investment_density_normalized': row[13],
                'total_keywords_count': row[14],
                'calculation_date': row[15]
            })
    
    db.close()
    
    if not all_data:
        print("\n⚠️ 没有得分数据可导出")
        return
    
    # 转换为DataFrame
    df = pd.DataFrame(all_data)
    
    # 生成输出文件名
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{QUANTITATIVE_CONFIG['output_dir']}/quantitative_scores_{timestamp}.csv"
    
    # 导出到CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n{'='*80}")
    print(f"✅ 成功导出 {len(all_data)} 条记录")
    print(f"输出文件: {output_file}")
    print(f"{'='*80}")
    
    # 显示统计信息
    print("\n数据统计:")
    print(f"  年份: {df['year'].min()} - {df['year'].max()}")
    print(f"  公司数: {df['ticker'].nunique()}")
    print(f"  总记录数: {len(df)}")
    
    # 显示新指标的统计
    if 'china_investment_density' in df.columns:
        print("\n原始密度指标统计:")
        print(f"  中国投资密度 (平均): {df['china_investment_density'].mean():.2f}%")
        print(f"  中国投资密度 (范围): {df['china_investment_density'].min():.2f}% - {df['china_investment_density'].max():.2f}%")
        print(f"  非中国投资密度 (平均): {df['non_china_investment_density'].mean():.2f}%")
        print(f"  非中国投资密度 (范围): {df['non_china_investment_density'].min():.2f}% - {df['non_china_investment_density'].max():.2f}%")
    
    if 'china_investment_density_normalized' in df.columns:
        print("\n归一化密度指标统计 (0-100，相对于两者最大值):")
        print(f"  中国投资密度归一化 (平均): {df['china_investment_density_normalized'].mean():.2f}")
        print(f"  中国投资密度归一化 (范围): {df['china_investment_density_normalized'].min():.2f} - {df['china_investment_density_normalized'].max():.2f}")
        print(f"  非中国投资密度归一化 (平均): {df['non_china_investment_density_normalized'].mean():.2f}")
        print(f"  非中国投资密度归一化 (范围): {df['non_china_investment_density_normalized'].min():.2f} - {df['non_china_investment_density_normalized'].max():.2f}")
        
        # 统计偏好情况
        china_preferred = (df['china_investment_density_normalized'] > df['non_china_investment_density_normalized']).sum()
        non_china_preferred = (df['china_investment_density_normalized'] < df['non_china_investment_density_normalized']).sum()
        neutral = (df['china_investment_density_normalized'] == df['non_china_investment_density_normalized']).sum()
        
        print(f"\n投资偏好分布 (基于归一化指标):")
        print(f"  更倾向中国投资: {china_preferred} ({china_preferred/len(df)*100:.1f}%)")
        print(f"  更倾向非中国投资: {non_china_preferred} ({non_china_preferred/len(df)*100:.1f}%)")
        print(f"  均衡: {neutral} ({neutral/len(df)*100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='导出量化得分到CSV')
    parser.add_argument('--db', type=str, 
                       default=f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}",
                       help='数据库路径')
    parser.add_argument('--output', type=str, help='输出CSV文件路径（可选）')
    args = parser.parse_args()
    
    export_scores_to_csv(args.db, args.output)


if __name__ == "__main__":
    main()

