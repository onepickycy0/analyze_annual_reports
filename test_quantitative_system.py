#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化评分系统 - 快速测试脚本
验证所有模块是否正常工作
"""

import sys
from pathlib import Path


def test_imports():
    """测试模块导入"""
    print("测试1: 模块导入...")
    
    try:
        from database_manager_v2 import DatabaseManagerV2
        print("  ✓ database_manager_v2")
        
        from corpus_builder import CorpusBuilder
        print("  ✓ corpus_builder")
        
        from keyword_extractor import KeywordExtractor
        print("  ✓ keyword_extractor")
        
        from tfidf_calculator import TFIDFCalculator
        print("  ✓ tfidf_calculator")
        
        from score_calculator import ScoreCalculator
        print("  ✓ score_calculator")
        
        from report_generator_v2 import ReportGeneratorV2
        print("  ✓ report_generator_v2")
        
        from main_quantitative_analysis import QuantitativeAnalysisSystem
        print("  ✓ main_quantitative_analysis")
        
        return True
    except Exception as e:
        print(f"  ✗ 导入失败: {e}")
        return False


def test_database():
    """测试数据库创建"""
    print("\n测试2: 数据库初始化...")
    
    try:
        from database_manager_v2 import DatabaseManagerV2
        
        test_db = "/tmp/test_quantitative.db"
        db = DatabaseManagerV2(test_db)
        print("  ✓ 数据库创建成功")
        
        # 测试保存公司
        db.save_company(
            ticker="TEST",
            cik="0000001",
            company_name="Test Company",
            fiscal_year_end="2025-12-31",
            report_date="2025-03-01",
            data_year=2025,
            file_path="/test/path.html"
        )
        print("  ✓ 公司数据保存成功")
        
        # 测试读取公司
        companies = db.get_all_companies()
        if companies:
            print(f"  ✓ 公司数据读取成功 ({len(companies)} 家)")
        
        db.close()
        
        # 清理测试文件
        Path(test_db).unlink(missing_ok=True)
        
        return True
    except Exception as e:
        print(f"  ✗ 数据库测试失败: {e}")
        return False


def test_config():
    """测试配置文件"""
    print("\n测试3: 配置文件...")
    
    try:
        from config import (
            QUANTITATIVE_CONFIG,
            CORPUS_EXTRACTION_PROMPT,
            KEYWORD_EXTRACTION_PROMPT
        )
        
        print("  ✓ QUANTITATIVE_CONFIG")
        print(f"    - 数据库名: {QUANTITATIVE_CONFIG['v2_db_name']}")
        print(f"    - 并发数: {QUANTITATIVE_CONFIG['max_concurrent']}")
        
        print("  ✓ CORPUS_EXTRACTION_PROMPT")
        print(f"    - 长度: {len(CORPUS_EXTRACTION_PROMPT)} 字符")
        
        print("  ✓ KEYWORD_EXTRACTION_PROMPT")
        print(f"    - 长度: {len(KEYWORD_EXTRACTION_PROMPT)} 字符")
        
        return True
    except Exception as e:
        print(f"  ✗ 配置测试失败: {e}")
        return False


def test_data_directory():
    """测试数据目录"""
    print("\n测试4: 数据目录...")
    
    data_dir = Path("/root/liujie/nianbao-v2/2025")
    
    if not data_dir.exists():
        print(f"  ✗ 数据目录不存在: {data_dir}")
        return False
    
    print(f"  ✓ 数据目录存在: {data_dir}")
    
    html_files = list(data_dir.glob("*.html"))
    print(f"  ✓ 找到 {len(html_files)} 个HTML文件")
    
    if html_files:
        print(f"    示例: {html_files[0].name}")
    
    return len(html_files) > 0


def main():
    """主测试函数"""
    print("="*80)
    print("量化评分系统 - 快速测试")
    print("="*80)
    
    results = []
    
    # 运行测试
    results.append(("模块导入", test_imports()))
    results.append(("数据库", test_database()))
    results.append(("配置文件", test_config()))
    results.append(("数据目录", test_data_directory()))
    
    # 总结
    print("\n" + "="*80)
    print("测试总结")
    print("="*80)
    
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"{status} - {name}")
    
    all_passed = all(r[1] for r in results)
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ 所有测试通过！系统已就绪。")
        print("\n运行系统:")
        print("  ./run_quantitative_analysis.sh 5")
    else:
        print("❌ 部分测试失败，请检查错误信息。")
    print("="*80)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())




