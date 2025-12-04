#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化评分系统 - 主程序
整合所有模块，运行完整流程
"""

import asyncio
from pathlib import Path
from datetime import datetime
import sys
import argparse

from corpus_builder import CorpusBuilder
from keyword_extractor import KeywordExtractor
from tfidf_calculator import TFIDFCalculator
from score_calculator import ScoreCalculator
from report_generator_v2 import ReportGeneratorV2
from config import QUANTITATIVE_CONFIG


class QuantitativeAnalysisSystem:
    """量化分析系统"""
    
    def __init__(self, data_dirs: list = None, db_path: str = None):
        """
        初始化量化分析系统
        
        Args:
            data_dirs: 数据目录列表，如果为None则自动扫描所有年份
            db_path: 数据库路径，如果为None则使用默认路径
        """
        # 默认数据库路径
        if db_path is None:
            db_path = f"{QUANTITATIVE_CONFIG['output_dir']}/{QUANTITATIVE_CONFIG['v2_db_name']}"
        
        self.db_path = db_path
        
        # 处理数据目录
        if data_dirs is None:
            # 自动扫描所有年份目录（只处理20*格式的文件夹）
            base_reports_dir = Path("/root/liujie/nianbao-v2results/reports")
            if base_reports_dir.exists():
                self.data_dirs = [d for d in base_reports_dir.iterdir() 
                                 if d.is_dir() and d.name.startswith('20') and len(d.name) == 4]
                self.data_dirs.sort()  # 按年份排序
            else:
                self.data_dirs = []
        else:
            self.data_dirs = [Path(d) for d in data_dirs]
        
        # 初始化共享数据库连接
        self.db = DatabaseManagerV2(db_path)
        
        # 初始化各模块（共享数据库连接）
        self.corpus_builder = CorpusBuilder(db_path)
        self.keyword_extractor = KeywordExtractor(db_path)
        self.tfidf_calculator = TFIDFCalculator(db_path)
        self.score_calculator = ScoreCalculator(db_path)
        self.report_generator = ReportGeneratorV2(db_path)
        
        # 让所有模块共享同一个数据库连接
        self.corpus_builder.db = self.db
        self.keyword_extractor.db = self.db
        self.tfidf_calculator.db = self.db
        self.score_calculator.db = self.db
    
    async def run_full_pipeline(self, limit: int = None, years: list = None):
        """
        运行完整流程
        
        Args:
            limit: 每个年份处理的文件数限制，None表示全部
            years: 要处理的年份列表，None表示全部
        """
        print("="*80)
        print("量化评分系统 - 完整流程（多年份版本）")
        print("="*80)
        print(f"数据库: {self.db_path}")
        print(f"处理限制: 每年份{limit}个文件" if limit else "处理限制: 全部文件")
        print("="*80)
        
        # 筛选要处理的目录
        dirs_to_process = self.data_dirs
        if years:
            year_strs = [str(y) for y in years]
            dirs_to_process = [d for d in self.data_dirs if d.name in year_strs]
        
        # 收集所有HTML文件
        all_html_files = []
        for data_dir in dirs_to_process:
            html_files = sorted(data_dir.glob("*.html"))
            if limit:
                html_files = html_files[:limit]
            all_html_files.extend(html_files)
            print(f"  {data_dir.name}: {len(html_files)} 个文件")
        
        print(f"\n总计找到 {len(all_html_files)} 个年报文件（跨{len(dirs_to_process)}个年份）")
        
        if not all_html_files:
            print("⚠️ 未找到HTML文件")
            return
        
        # 步骤1: 构建语料库
        print(f"\n" + "="*80)
        print(f"步骤1: 构建语料库（多年份数据）")
        print(f"="*80)
        await self.corpus_builder.build_corpus_batch(all_html_files)
        
        # 步骤2: 提取关键词
        print(f"\n" + "="*80)
        print(f"步骤2: 智能提取关键词")
        print(f"="*80)
        await self.keyword_extractor.extract_keywords_from_corpus()
        
        # 步骤3: 计算TF-IDF
        print(f"\n" + "="*80)
        print(f"步骤3: 计算TF-IDF")
        print(f"="*80)
        self.tfidf_calculator.calculate_tfidf_for_all_companies()
        
        # 步骤4: 计算量化得分
        print(f"\n" + "="*80)
        print(f"步骤4: 计算量化得分")
        print(f"="*80)
        self.score_calculator.calculate_scores_for_all_companies()
        
        # 步骤5: 生成报告
        print(f"\n" + "="*80)
        print(f"步骤5: 生成分析报告")
        print(f"="*80)
        
        output_file = f"{QUANTITATIVE_CONFIG['output_dir']}/annual_reports_quantitative_analysis.xlsx"
        self.report_generator.generate_full_report(output_file)
        
        # 完成
        print(f"\n" + "="*80)
        print(f"✅ 完整流程执行完毕!")
        print(f"="*80)
        print(f"\n输出文件:")
        print(f"  数据库: {self.db_path}")
        print(f"  报告: {output_file}")
        print(f"\n" + "="*80)


def main():
    """主函数"""
    # 创建参数解析器
    parser = argparse.ArgumentParser(
        description='量化评分系统 - 年报分析与量化评分（支持多年份）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 自动处理所有年份（2014-2024）
  python main_quantitative_analysis.py
  
  # 处理特定年份
  python main_quantitative_analysis.py --years 2020 2021 2022
  
  # 每个年份只处理前10个文件（快速测试）
  python main_quantitative_analysis.py --limit 10
  
  # 处理特定目录
  python main_quantitative_analysis.py --data-dirs /path/to/2020 /path/to/2021
        """
    )
    
    parser.add_argument(
        '--data-dirs',
        type=str,
        nargs='+',
        default=None,
        help='HTML文件所在目录列表 (默认: 自动扫描/root/liujie/nianbao-v2results/reports/下所有年份)'
    )
    
    parser.add_argument(
        '--years', '-y',
        type=int,
        nargs='+',
        default=None,
        help='要处理的年份列表，如: --years 2020 2021 2022 (默认: 全部年份)'
    )
    
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='每个年份处理的文件数量限制 (默认: 处理全部)'
    )
    
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help='数据库文件路径 (默认: /root/liujie/nianbao-v2results/annual_reports_quantitative.db)'
    )
    
    # 解析参数
    args = parser.parse_args()
    
    # 创建系统实例
    system = QuantitativeAnalysisSystem(
        data_dirs=args.data_dirs,
        db_path=args.db_path
    )
    
    # 运行完整流程
    asyncio.run(system.run_full_pipeline(limit=args.limit, years=args.years))


if __name__ == "__main__":
    main()

