#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断脚本：检查哪些年报提取失败
"""

import sqlite3
from pathlib import Path
import sys

def diagnose_year(db_path: str, reports_dir: str, year: int):
    """诊断指定年份的提取情况"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取所有HTML文件
    reports_path = Path(reports_dir) / str(year)
    if not reports_path.exists():
        print(f"❌ 目录不存在: {reports_path}")
        return
    
    html_files = sorted(reports_path.glob("*.html"))
    print(f"\n{'='*80}")
    print(f"诊断报告 - {year}年")
    print(f"{'='*80}")
    print(f"📁 HTML文件总数: {len(html_files)}")
    
    # 获取数据库中的记录
    cursor.execute(f"SELECT COUNT(*) FROM companies_{year}")
    company_count = cursor.fetchone()[0]
    print(f"📊 数据库公司记录: {company_count}")
    
    # 获取有语料库数据的公司
    cursor.execute(f"""
        SELECT DISTINCT ticker 
        FROM corpus_{year} 
        WHERE text_segment IS NOT NULL AND LENGTH(text_segment) > 0
    """)
    successful_tickers = set(row[0] for row in cursor.fetchall())
    print(f"✅ 成功提取数据: {len(successful_tickers)} 个公司")
    print(f"❌ 提取失败: {company_count - len(successful_tickers)} 个公司")
    
    # 获取失败的公司列表
    cursor.execute(f"SELECT ticker, company_name FROM companies_{year}")
    all_companies = cursor.fetchall()
    
    failed_companies = [(t, n) for t, n in all_companies if t not in successful_tickers]
    
    if failed_companies:
        print(f"\n失败的公司列表 (前20个):")
        for ticker, name in failed_companies[:20]:
            print(f"  - {ticker}: {name}")
        
        if len(failed_companies) > 20:
            print(f"  ... 还有 {len(failed_companies) - 20} 个")
    
    # 检查是否有文件没有被处理
    processed_files = set()
    cursor.execute(f"SELECT file_path FROM companies_{year}")
    for row in cursor.fetchall():
        if row[0]:
            processed_files.add(Path(row[0]).name)
    
    unprocessed = []
    for html_file in html_files:
        if html_file.name not in processed_files:
            unprocessed.append(html_file.name)
    
    if unprocessed:
        print(f"\n⚠️  完全未处理的文件: {len(unprocessed)} 个")
        for fname in unprocessed[:10]:
            print(f"  - {fname}")
        if len(unprocessed) > 10:
            print(f"  ... 还有 {len(unprocessed) - 10} 个")
    
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"建议:")
    print(f"1. 运行 python retry_failed.py {year} 重新处理失败的文件")
    print(f"2. 检查API配置和网络连接")
    print(f"3. 查看错误日志以了解失败原因")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    reports_dir = "/root/liujie/nianbao-v2results/reports"
    
    if len(sys.argv) > 1:
        year = int(sys.argv[1])
        diagnose_year(db_path, reports_dir, year)
    else:
        # 诊断所有年份
        for year in range(2014, 2025):
            diagnose_year(db_path, reports_dir, year)

