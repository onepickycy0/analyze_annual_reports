#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告生成器 v3.0 - 按年份生成独立Sheets
"""

import sqlite3
import pandas as pd
from datetime import datetime


class ReportGeneratorV2:
    """报告生成器 - 按年份独立Sheets"""
    
    YEARS = list(range(2002, 2025))
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def generate_year_summary(self, year: int) -> pd.DataFrame:
        """生成指定年份的摘要"""
        query = f"""
        SELECT 
            COUNT(*) as company_count,
            AVG(investment_attitude_score) as avg_attitude,
            AVG(china_investment_score) as avg_china,
            AVG(non_china_investment_score) as avg_non_china,
            MIN(investment_attitude_score) as min_attitude,
            MAX(investment_attitude_score) as max_attitude
        FROM quantitative_scores_{year}
        """
        df = pd.read_sql_query(query, self.conn)
        
        if df.empty or df['company_count'].iloc[0] == 0:
            return pd.DataFrame({'指标': [f'{year}年无数据'], '值': ['']})
        
        return pd.DataFrame({
            '指标': ['公司数', '平均投资态度', '平均中国投资', '平均非中国投资', '投资态度范围'],
            '值': [
                int(df['company_count'].iloc[0]),
                f"{df['avg_attitude'].iloc[0]:.2f}",
                f"{df['avg_china'].iloc[0]:.2f}",
                f"{df['avg_non_china'].iloc[0]:.2f}",
                f"{df['min_attitude'].iloc[0]:.2f} - {df['max_attitude'].iloc[0]:.2f}"
            ]
        })
    
    def generate_year_scores(self, year: int) -> pd.DataFrame:
        """生成指定年份的公司得分"""
        query = f"""
        SELECT 
            c.ticker as 股票代码,
            c.company_name as 公司名称,
            qs.investment_attitude_score as 投资态度,
            qs.china_investment_score as 中国投资,
            qs.non_china_investment_score as 非中国投资,
            qs.expansion_score as 扩张得分,
            qs.contraction_score as 收缩得分
        FROM quantitative_scores_{year} qs
        JOIN companies_{year} c ON qs.ticker = c.ticker
        ORDER BY qs.investment_attitude_score DESC
        """
        return pd.read_sql_query(query, self.conn)
    
    def generate_year_keywords(self, year: int) -> pd.DataFrame:
        """生成指定年份的关键词分析"""
        query = f"""
        SELECT 
            keyword as 关键词,
            keyword_category as 类别,
            COUNT(DISTINCT ticker) as 出现公司数,
            AVG(tfidf) as 平均TFIDF,
            MAX(tfidf) as 最大TFIDF
        FROM tfidf_scores_{year}
        GROUP BY keyword, keyword_category
        ORDER BY AVG(tfidf) DESC
        LIMIT 200
        """
        return pd.read_sql_query(query, self.conn)
    
    def generate_year_raw_data(self, year: int) -> pd.DataFrame:
        """生成指定年份的原始数据"""
        query = f"""
        SELECT 
            c.ticker as 代码,
            c.company_name as 公司,
            qs.expansion_score as 扩张原始,
            qs.contraction_score as 收缩原始,
            qs.china_positive_score as 中国正面,
            qs.china_negative_score as 中国负面,
            qs.non_china_raw_score as 非中国原始,
            qs.total_keywords_count as 关键词数
        FROM quantitative_scores_{year} qs
        JOIN companies_{year} c ON qs.ticker = c.ticker
        ORDER BY c.ticker
        """
        return pd.read_sql_query(query, self.conn)
    
    def generate_full_report(self, output_file: str):
        """生成完整报告 - 按年份分sheets"""
        print(f"\n{'='*80}")
        print(f"报告生成器 v3.0 - 按年份独立Sheets")
        print(f"{'='*80}\n")
        
        # 检查哪些年份有数据
        cursor = self.conn.cursor()
        years_with_data = []
        for year in self.YEARS:
            cursor.execute(f'SELECT COUNT(*) FROM companies_{year}')
            if cursor.fetchone()[0] > 0:
                years_with_data.append(year)
        
        if not years_with_data:
            print("⚠️ 无数据")
            return
        
        print(f"有数据年份: {years_with_data}\n")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 全局摘要
            print("  生成: 全局摘要")
            summary_data = []
            for year in years_with_data:
                year_summary = self.generate_year_summary(year)
                if not year_summary.empty and '无数据' not in str(year_summary.iloc[0, 0]):
                    summary_data.append(pd.DataFrame({'年份': [year]}))
            pd.concat(summary_data, ignore_index=True).to_excel(writer, sheet_name='总览', index=False)
            
            # 为每年生成独立的sheets
            for year in years_with_data:
                print(f"  生成: {year}年数据")
                
                # 摘要
                summary = self.generate_year_summary(year)
                if not summary.empty:
                    summary.to_excel(writer, sheet_name=f'{year}_摘要', index=False)
                
                # 公司得分
                scores = self.generate_year_scores(year)
                if not scores.empty:
                    scores.to_excel(writer, sheet_name=f'{year}_得分', index=False)
                
                # 关键词
                keywords = self.generate_year_keywords(year)
                if not keywords.empty:
                    keywords.to_excel(writer, sheet_name=f'{year}_关键词', index=False)
                
                # 原始数据
                raw = self.generate_year_raw_data(year)
                if not raw.empty:
                    raw.to_excel(writer, sheet_name=f'{year}_原始', index=False)
        
        print(f"\n{'='*80}")
        print(f"✅ 完成: {output_file}")
        print(f"   共{len(years_with_data)}年 × 4个sheets/年")
        print(f"{'='*80}")
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()


def main():
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    output_file = f"/root/liujie/nianbao-v2results/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    generator = ReportGeneratorV2(db_path)
    generator.generate_full_report(output_file)


if __name__ == "__main__":
    main()
