#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关键词频率分析器
统计指定关键词在每个公司年报中的出现频率和次数
"""

import sqlite3
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
from bs4 import BeautifulSoup


class KeywordFrequencyAnalyzer:
    """关键词频率分析器"""
    
    # 定义要搜索的关键词（支持变体）
    KEYWORDS = {
        'cost': ['cost', 'costs'],
        'efficiency': ['efficiency', 'efficient', 'efficiently'],
        'profit': ['profit', 'profits', 'profitability', 'profitable'],
        'resilience': ['resilience', 'resilient'],
        'security': ['security', 'secure'],
        'risk': ['risk', 'risks', 'risky'],
        'uncertainty': ['uncertainty', 'uncertain', 'uncertainties'],
        'diversification': ['diversification', 'diversify', 'diversified'],
        'localization': ['localization', 'localize', 'localized', 'localisation'],
        'flexibility': ['flexibility', 'flexible'],
        'continuity': ['continuity', 'continuous'],
        'critical_inputs': ['critical input', 'critical inputs', 'critical material', 'critical materials'],
        'reconfiguration': ['reconfiguration', 'reconfigure', 'reconfigured'],
        'de_risking': ['de-risking', 'de-risk', 'derisking', 'derisk'],
        'realignment': ['realignment', 'realign', 'realigned'],
        'volatility': ['volatility', 'volatile']
    }
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def get_all_companies_by_year(self, year: int) -> List[Tuple[str, str, str]]:
        """获取指定年份的所有公司（包括文件路径）"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT ticker, company_name, file_path
            FROM companies_{year}
            ORDER BY ticker
        ''')
        return cursor.fetchall()
    
    def parse_html_file(self, html_path: str) -> str:
        """解析HTML文件，提取全文"""
        try:
            html_file = Path(html_path)
            if not html_file.exists():
                return ''
            
            with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            # 移除script和style标签
            for tag in soup(['script', 'style', 'meta', 'link']):
                tag.decompose()
            
            # 提取文本
            text = soup.get_text(separator=' ', strip=True)
            
            # 清理文本
            text = re.sub(r'\s+', ' ', text)
            
            return text
        except Exception as e:
            print(f"      ⚠️  解析HTML失败 {html_path}: {e}")
            return ''
    
    def get_company_full_text(self, file_path: str) -> str:
        """获取公司年报的全文"""
        if not file_path:
            return ''
        return self.parse_html_file(file_path)
    
    def count_keyword_variants(self, text: str, variants: List[str]) -> Dict[str, int]:
        """统计关键词及其变体的出现次数"""
        text_lower = text.lower()
        
        counts = {}
        total = 0
        
        for variant in variants:
            # 使用单词边界匹配，确保完整单词
            pattern = r'\b' + re.escape(variant.lower()) + r'\b'
            count = len(re.findall(pattern, text_lower))
            counts[variant] = count
            total += count
        
        counts['total'] = total
        return counts
    
    def analyze_year(self, year: int) -> List[Dict]:
        """分析指定年份的所有公司（基于全文）"""
        print(f"\n分析 {year} 年...")
        
        companies = self.get_all_companies_by_year(year)
        if not companies:
            print(f"  ⚠️  {year}年无数据")
            return []
        
        print(f"  公司数量: {len(companies)}")
        
        results = []
        for i, (ticker, company_name, file_path) in enumerate(companies, 1):
            if i % 20 == 0 or i == 1:
                print(f"    处理: {i}/{len(companies)} - {ticker}")
            
            # 获取年报全文
            text = self.get_company_full_text(file_path)
            if not text:
                print(f"      ⚠️  无法获取 {ticker} 的全文")
                continue
            
            # 计算全文词数
            word_count = len(text.split())
            
            # 统计每个关键词
            result = {
                'year': year,
                'ticker': ticker,
                'company_name': company_name,
                'total_words': word_count
            }
            
            for keyword, variants in self.KEYWORDS.items():
                counts = self.count_keyword_variants(text, variants)
                result[f'{keyword}_count'] = counts['total']
                # 计算频率（每千词）
                result[f'{keyword}_freq'] = round(counts['total'] / word_count * 1000, 2) if word_count > 0 else 0
            
            results.append(result)
        
        print(f"  ✓ 完成 {len(results)} 个公司")
        return results
    
    def analyze_all_years(self, years: List[int] = None) -> List[Dict]:
        """分析所有年份或指定年份"""
        if years is None:
            # 获取所有可用年份
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'companies_%'")
            years = sorted([int(table[0].split('_')[1]) for table in cursor.fetchall()])
        
        print(f"\n{'='*80}")
        print(f"关键词频率分析器")
        print(f"{'='*80}")
        print(f"分析年份: {years}")
        print(f"关键词数量: {len(self.KEYWORDS)}")
        
        all_results = []
        for year in years:
            year_results = self.analyze_year(year)
            all_results.extend(year_results)
        
        print(f"\n{'='*80}")
        print(f"✅ 完成所有分析")
        print(f"  总记录数: {len(all_results)}")
        print(f"{'='*80}")
        
        return all_results
    
    def export_to_csv(self, results: List[Dict], output_file: str):
        """导出结果到CSV文件"""
        if not results:
            print("⚠️  无数据可导出")
            return
        
        print(f"\n导出到CSV: {output_file}")
        
        # 定义列顺序
        fieldnames = ['year', 'ticker', 'company_name', 'total_words']
        
        # 添加关键词列（次数 + 频率）
        for keyword in self.KEYWORDS.keys():
            fieldnames.append(f'{keyword}_count')
            fieldnames.append(f'{keyword}_freq')
        
        # 写入CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"✓ 已导出 {len(results)} 条记录")
        print(f"  文件: {output_file}")
    
    def export_summary_to_csv(self, results: List[Dict], output_file: str):
        """导出汇总统计到CSV"""
        if not results:
            return
        
        print(f"\n生成汇总统计: {output_file}")
        
        # 按年份汇总
        year_stats = defaultdict(lambda: defaultdict(list))
        
        for record in results:
            year = record['year']
            for keyword in self.KEYWORDS.keys():
                year_stats[year][keyword].append(record[f'{keyword}_count'])
        
        # 计算统计指标
        summary = []
        for year in sorted(year_stats.keys()):
            row = {'year': year}
            for keyword in self.KEYWORDS.keys():
                counts = year_stats[year][keyword]
                row[f'{keyword}_mean'] = round(sum(counts) / len(counts), 2) if counts else 0
                row[f'{keyword}_median'] = round(sorted(counts)[len(counts)//2], 2) if counts else 0
                row[f'{keyword}_max'] = max(counts) if counts else 0
                row[f'{keyword}_total'] = sum(counts)
            row['companies'] = len(year_stats[year][list(self.KEYWORDS.keys())[0]])
            summary.append(row)
        
        # 写入CSV
        fieldnames = ['year', 'companies']
        for keyword in self.KEYWORDS.keys():
            fieldnames.extend([
                f'{keyword}_mean',
                f'{keyword}_median', 
                f'{keyword}_max',
                f'{keyword}_total'
            ])
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary)
        
        print(f"✓ 已导出汇总统计")
    
    def close(self):
        """关闭数据库连接"""
        self.conn.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='关键词频率分析器')
    parser.add_argument('--years', nargs='+', type=int, help='要分析的年份，如: 2020 2021 2022')
    parser.add_argument('--db', type=str, 
                       default='/root/liujie/nianbao-v2/results/annual_reports_quantitative.db',
                       help='数据库路径')
    parser.add_argument('--output', type=str,
                       default='/root/liujie/nianbao-v2/results/keyword_frequency_analysis.csv',
                       help='输出CSV文件路径')
    args = parser.parse_args()
    
    # 创建分析器
    analyzer = KeywordFrequencyAnalyzer(args.db)
    
    # 分析
    results = analyzer.analyze_all_years(args.years)
    
    # 导出详细数据
    analyzer.export_to_csv(results, args.output)
    
    # 导出汇总统计
    summary_file = args.output.replace('.csv', '_summary.csv')
    analyzer.export_summary_to_csv(results, summary_file)
    
    # 关闭
    analyzer.close()
    
    print(f"\n{'='*80}")
    print(f"✅ 全部完成")
    print(f"  详细数据: {args.output}")
    print(f"  汇总统计: {summary_file}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()


