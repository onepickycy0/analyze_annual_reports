#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
得分计算器 - NSS密度校正方法，按年份独立计算
"""

import numpy as np
from typing import List, Dict
from database_manager_v2 import DatabaseManagerV2


class ScoreCalculator:
    """得分计算器 - NSS密度校正，按年份独立"""
    
    def __init__(self, db_path: str):
        self.db = DatabaseManagerV2(db_path)
        self.eps = 1e-9
        self.k = 5.0  # 密度权重系数
    
    def calculate_company_scores(self, tfidf_data: List[Dict]) -> Dict:
        """计算公司原始得分"""
        expansion = sum(row['tfidf'] for row in tfidf_data if row['keyword_category'] == 'expansion')
        contraction = sum(row['tfidf'] for row in tfidf_data if row['keyword_category'] == 'contraction')
        china_positive = sum(row['tfidf'] for row in tfidf_data if row['keyword_category'] == 'china_positive')
        china_negative = sum(row['tfidf'] for row in tfidf_data if row['keyword_category'] == 'china_negative')
        non_china = sum(row['tfidf'] for row in tfidf_data if row['keyword_category'] == 'non_china_regions')
        
        total_tfidf = expansion + contraction + china_positive + china_negative + non_china + self.eps
        
        return {
            'expansion_score': expansion,
            'contraction_score': contraction,
            'china_positive_score': china_positive,
            'china_negative_score': china_negative,
            'non_china_raw_score': non_china,
            'total_tfidf': total_tfidf,
            'total_keywords_count': len(tfidf_data)
        }
    
    def calculate_scores_for_year(self, data_year: int):
        """为指定年份计算得分 - NSS密度校正"""
        print(f"\n  【{data_year}年】计算得分...")
        
        companies = self.db.get_companies_by_year(data_year)
        
        if not companies:
            print(f"    ⚠️ 无数据")
            return
        
        # 收集原始得分
        raw_scores = []
        for company in companies:
            ticker = company['ticker']
            tfidf_data = self.db.get_tfidf_scores(ticker, data_year)
            if not tfidf_data:
                continue
            
            scores = self.calculate_company_scores(tfidf_data)
            scores['ticker'] = ticker
            raw_scores.append(scores)
        
        if not raw_scores:
            print(f"    ⚠️ 无有效得分")
            return
        
        print(f"    有效公司: {len(raw_scores)}")
        
        # NSS密度校正（年内）
        # 计算所有公司的非中国投资密度
        non_china_densities = []
        for s in raw_scores:
            nc_d = s['non_china_raw_score'] / s['total_tfidf']
            non_china_densities.append(nc_d)
        
        nc_d_mean = np.mean(non_china_densities)
        nc_d_std = np.std(non_china_densities) + self.eps
        
        # 计算最终得分
        for scores in raw_scores:
            ticker = scores['ticker']
            total_tfidf = scores['total_tfidf']
            
            # 投资态度（扩张 vs 收缩）
            exp_d = scores['expansion_score'] / total_tfidf
            con_d = scores['contraction_score'] / total_tfidf
            NSS = (exp_d - con_d) / (exp_d + con_d + self.eps)
            attitude_NSS = (NSS + 1) * 50
            density_factor = np.log1p(self.k * total_tfidf)
            attitude_score = max(0, min(100, 50 + (attitude_NSS - 50) * density_factor))
            
            # 中国投资态度（正面 vs 负面）
            pos_d = scores['china_positive_score'] / total_tfidf
            neg_d = scores['china_negative_score'] / total_tfidf
            NSS_c = (pos_d - neg_d) / (pos_d + neg_d + self.eps)
            china_NSS = (NSS_c + 1) * 50
            china_score = max(0, min(100, 50 + (china_NSS - 50) * density_factor))
            
            # 非中国投资（关注强度）- 使用标准z-score
            nc_d = scores['non_china_raw_score'] / total_tfidf
            nc_z = (nc_d - nc_d_mean) / nc_d_std
            non_china_score = max(0, min(100, nc_z * 10 + 50))
            
            # 投资地理密度得分（绝对占比）
            china_investment_density = (scores['china_positive_score'] / total_tfidf) * 100
            non_china_investment_density = (scores['non_china_raw_score'] / total_tfidf) * 100
            
            # 联合归一化：相对于两者最大值归一到0-100，50为中性
            max_density = max(china_investment_density, non_china_investment_density)
            if max_density > 0:
                china_investment_density_normalized = (china_investment_density / max_density) * 100
                non_china_investment_density_normalized = (non_china_investment_density / max_density) * 100
            else:
                # 两个都是0的情况
                china_investment_density_normalized = 50.0
                non_china_investment_density_normalized = 50.0
            
            # 保存
            self.db.save_quantitative_scores(
                ticker=ticker,
                data_year=data_year,
                investment_attitude_score=attitude_score,
                expansion_score=scores['expansion_score'],
                contraction_score=scores['contraction_score'],
                china_investment_score=china_score,
                china_positive_score=scores['china_positive_score'],
                china_negative_score=scores['china_negative_score'],
                non_china_investment_score=non_china_score,
                non_china_raw_score=scores['non_china_raw_score'],
                china_investment_density=china_investment_density,
                non_china_investment_density=non_china_investment_density,
                china_investment_density_normalized=china_investment_density_normalized,
                non_china_investment_density_normalized=non_china_investment_density_normalized,
                total_keywords_count=scores['total_keywords_count']
            )
        
        print(f"    ✓ 完成{len(raw_scores)}家公司")
    
    def calculate_scores_for_all_companies(self):
        """按年份分别计算所有得分"""
        print(f"\n{'='*80}")
        print(f"得分计算器 - NSS密度校正（按年份）")
        print(f"{'='*80}")
        
        years = self.db.get_available_years()
        print(f"\n可用年份: {years}")
        
        if not years:
            print("⚠️ 无数据")
            return
        
        for year in years:
            self.calculate_scores_for_year(year)
        
        print(f"\n{'='*80}")
        print(f"✅ 完成 - 共{len(years)}年，k={self.k}")
        print(f"{'='*80}")


def main():
    db_path = "/root/liujie/nianbao-v2results/annual_reports_quantitative.db"
    calculator = ScoreCalculator(db_path)
    calculator.calculate_scores_for_all_companies()


if __name__ == "__main__":
    main()
