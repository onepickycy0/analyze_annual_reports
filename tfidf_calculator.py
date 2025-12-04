#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TF-IDF计算器 - 按年份独立计算
"""

import math
from typing import List, Dict
from database_manager_v2 import DatabaseManagerV2


class TFIDFCalculator:
    """TF-IDF计算器 - 按年份独立计算"""
    
    def __init__(self, db_path: str):
        self.db = DatabaseManagerV2(db_path)
    
    def calculate_tfidf_for_year(self, data_year: int, keywords: List[Dict]):
        """为指定年份计算TF-IDF"""
        print(f"\n  【{data_year}年】计算TF-IDF...")
        
        companies = self.db.get_companies_by_year(data_year)
        corpus_data = self.db.get_corpus_texts_by_year(data_year)
        all_texts = [item['text'] for item in corpus_data if item['text']]
        
        if not all_texts or not companies:
            print(f"    ⚠️ 无数据")
            return
        
        print(f"    公司: {len(companies)}, 语料: {len(all_texts)}段")
        
        # 计算全局TF-IDF
        full_corpus = " ".join(all_texts).lower()
        total_words = len(full_corpus.split())
        total_segments = len(all_texts)
        
        year_tfidf = {}
        for keyword in keywords:
            kw_text = keyword['keyword'].lower()
            category = keyword['category']
            
            kw_count = full_corpus.count(kw_text)
            tf = kw_count / total_words if total_words > 0 else 0
            
            segment_count = sum(1 for text in all_texts if kw_text in text.lower())
            idf = math.log(total_segments / segment_count) if segment_count > 0 else 0
            
            tfidf = tf * idf
            if tfidf > 0:
                year_tfidf[kw_text] = {
                    'keyword': kw_text,
                    'category': category,
                    'tf': tf,
                    'idf': idf,
                    'tfidf': tfidf
                }
        
        print(f"    ✓ 计算{len(year_tfidf)}个关键词的TF-IDF")
        
        # 为每家公司标记关键词
        all_scores = []
        for company in companies:
            ticker = company['ticker']
            company_texts = self.db.get_company_corpus_texts(ticker, data_year)
            company_text = " ".join(company_texts).lower()
            
            for kw_text, tfidf_data in year_tfidf.items():
                if kw_text in company_text:
                    all_scores.append({
                        'ticker': ticker,
                        'keyword': kw_text,
                        'keyword_category': tfidf_data['category'],
                        'tf': tfidf_data['tf'],
                        'idf': tfidf_data['idf'],
                        'tfidf': tfidf_data['tfidf']
                    })
        
        self.db.save_tfidf_scores(data_year, all_scores)
        print(f"    ✓ 保存{len(all_scores)}条记录")
    
    def calculate_tfidf_for_all_companies(self):
        """按年份分别计算TF-IDF"""
        print(f"\n{'='*80}")
        print(f"TF-IDF计算器 - 按年份独立计算")
        print(f"{'='*80}")
        
        years = self.db.get_available_years()
        keywords = self.db.get_all_keywords()
        
        print(f"\n可用年份: {years}")
        print(f"关键词数: {len(keywords)}")
        
        if not keywords or not years:
            print("⚠️ 无数据")
            return
        
        for year in years:
            self.calculate_tfidf_for_year(year, keywords)
        
        print(f"\n{'='*80}")
        print(f"✅ 完成 - 共{len(years)}年")
        print(f"{'='*80}")


def main():
    db_path = "/root/liujie/nianbao-v2/results/annual_reports_quantitative.db"
    calculator = TFIDFCalculator(db_path)
    calculator.calculate_tfidf_for_all_companies()


if __name__ == "__main__":
    main()
