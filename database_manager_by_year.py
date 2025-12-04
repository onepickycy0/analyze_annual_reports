#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理器 v3.0 - 按年份独立表格
每个年份的数据存储在独立的表中
"""

import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class DatabaseManagerByYear:
    """数据库管理器 - 按年份独立表格版本"""
    
    YEARS = list(range(2002, 2025))  # 2002-2024
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.init_database()
    
    def init_database(self):
        """初始化所有年份的表结构"""
        cursor = self.conn.cursor()
        
        # 关键词表（所有年份共享）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT UNIQUE,
            keyword_category TEXT,
            extraction_method TEXT
        )
        ''')
        
        # 为每个年份创建独立的表
        for year in self.YEARS:
            self._create_tables_for_year(cursor, year)
        
        self.conn.commit()
    
    def _create_tables_for_year(self, cursor, year: int):
        """为指定年份创建所有表"""
        
        # 公司表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS companies_{year} (
            ticker TEXT PRIMARY KEY,
            cik TEXT,
            company_name TEXT,
            fiscal_year_end TEXT,
            report_date TEXT,
            file_path TEXT,
            processed_date TEXT
        )
        ''')
        
        # 语料库表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS corpus_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            text_segment TEXT,
            segment_type TEXT,
            segment_category TEXT,
            source_section TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # TF-IDF得分表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS tfidf_scores_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            keyword TEXT,
            keyword_category TEXT,
            tf REAL,
            idf REAL,
            tfidf REAL,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker),
            FOREIGN KEY (keyword) REFERENCES keywords(keyword)
        )
        ''')
        
        # 量化得分表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS quantitative_scores_{year} (
            ticker TEXT PRIMARY KEY,
            investment_attitude_score REAL,
            expansion_score REAL,
            contraction_score REAL,
            china_investment_score REAL,
            china_positive_score REAL,
            china_negative_score REAL,
            non_china_investment_score REAL,
            non_china_raw_score REAL,
            total_keywords_count INTEGER,
            calculation_date TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 对外投资表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS foreign_investments_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            investment_type TEXT,
            target_country TEXT,
            target_region TEXT,
            investment_amount REAL,
            investment_purpose TEXT,
            description TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 全球贸易表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS global_trade_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            total_revenue REAL,
            international_revenue REAL,
            international_revenue_pct REAL,
            major_markets TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 地理分布表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS geographic_segments_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            region TEXT,
            country TEXT,
            revenue REAL,
            revenue_pct REAL,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 供应链表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS supply_chain_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            supplier_countries TEXT,
            manufacturing_locations TEXT,
            distribution_centers TEXT,
            sourcing_strategy TEXT,
            risk_factors TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 政策影响表
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS policy_impacts_{year} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            policy_type TEXT,
            policy_description TEXT,
            impact_description TEXT,
            mentioned_countries TEXT,
            decoupling_indicators TEXT,
            FOREIGN KEY (ticker) REFERENCES companies_{year}(ticker)
        )
        ''')
        
        # 创建索引
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_corpus_{year}_ticker ON corpus_{year}(ticker)')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_tfidf_{year}_ticker ON tfidf_scores_{year}(ticker)')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_tfidf_{year}_keyword ON tfidf_scores_{year}(keyword)')
    
    # ==================== 公司管理 ====================
    
    def save_company(self, ticker: str, data_year: int, **kwargs):
        """保存公司信息到指定年份的表"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        INSERT OR REPLACE INTO companies_{data_year}
        (ticker, cik, company_name, fiscal_year_end, report_date, file_path, processed_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (ticker, kwargs.get('cik'), kwargs.get('company_name'),
              kwargs.get('fiscal_year_end'), kwargs.get('report_date'),
              kwargs.get('file_path'), datetime.now().isoformat()))
        self.conn.commit()
    
    def get_companies_by_year(self, data_year: int) -> List[Dict]:
        """获取指定年份的所有公司"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        SELECT ticker, company_name, cik
        FROM companies_{data_year}
        ORDER BY ticker
        ''')
        return [{'ticker': row[0], 'company_name': row[1], 'cik': row[2], 'data_year': data_year} 
                for row in cursor.fetchall()]
    
    def get_available_years(self) -> List[int]:
        """获取有数据的年份列表"""
        cursor = self.conn.cursor()
        years_with_data = []
        for year in self.YEARS:
            cursor.execute(f'SELECT COUNT(*) FROM companies_{year}')
            if cursor.fetchone()[0] > 0:
                years_with_data.append(year)
        return years_with_data
    
    # ==================== 语料库管理 ====================
    
    def save_text_segments(self, ticker: str, data_year: int, segments: List[Dict]):
        """保存文本段落到指定年份的表"""
        cursor = self.conn.cursor()
        for seg in segments:
            cursor.execute(f'''
            INSERT INTO corpus_{data_year} 
            (ticker, text_segment, segment_type, segment_category, source_section)
            VALUES (?, ?, ?, ?, ?)
            ''', (ticker, seg.get('text', ''), seg.get('type', ''),
                  seg.get('category', ''), seg.get('source_section', '')))
        self.conn.commit()
    
    def get_corpus_texts_by_year(self, data_year: int) -> List[Dict]:
        """获取指定年份的所有语料库文本"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        SELECT ticker, text_segment, segment_category 
        FROM corpus_{data_year}
        ''')
        return [{'ticker': row[0], 'text': row[1], 'category': row[2], 'year': data_year} 
                for row in cursor.fetchall()]
    
    def get_company_corpus_texts(self, ticker: str, data_year: int) -> List[str]:
        """获取指定公司和年份的语料库文本"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        SELECT text_segment 
        FROM corpus_{data_year}
        WHERE ticker = ?
        ''', (ticker,))
        return [row[0] for row in cursor.fetchall()]
    
    # ==================== 结构化数据管理 ====================
    
    def save_structured_data(self, ticker: str, data_year: int, data: Dict):
        """保存五个维度的结构化数据到指定年份的表"""
        cursor = self.conn.cursor()
        
        # 对外投资
        for item in data.get('foreign_investments', []):
            cursor.execute(f'''
            INSERT INTO foreign_investments_{data_year}
            (ticker, investment_type, target_country, target_region, 
             investment_amount, investment_purpose, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (ticker, item.get('investment_type'), item.get('target_country'),
                  item.get('target_region'), item.get('investment_amount'),
                  item.get('investment_purpose'), item.get('description')))
        
        # 全球贸易
        trade = data.get('global_trade', {})
        if trade:
            cursor.execute(f'''
            INSERT INTO global_trade_{data_year}
            (ticker, total_revenue, international_revenue, 
             international_revenue_pct, major_markets)
            VALUES (?, ?, ?, ?, ?)
            ''', (ticker, trade.get('total_revenue'),
                  trade.get('international_revenue'), trade.get('international_revenue_pct'),
                  json.dumps(trade.get('major_markets', []))))
        
        # 地理分布
        for seg in data.get('geographic_segments', []):
            cursor.execute(f'''
            INSERT INTO geographic_segments_{data_year}
            (ticker, region, country, revenue, revenue_pct)
            VALUES (?, ?, ?, ?, ?)
            ''', (ticker, seg.get('region'), seg.get('country'),
                  seg.get('revenue'), seg.get('revenue_pct')))
        
        # 供应链
        sc = data.get('supply_chain', {})
        if sc:
            cursor.execute(f'''
            INSERT INTO supply_chain_{data_year}
            (ticker, supplier_countries, manufacturing_locations,
             distribution_centers, sourcing_strategy, risk_factors)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (ticker, json.dumps(sc.get('supplier_countries', [])),
                  json.dumps(sc.get('manufacturing_locations', [])),
                  json.dumps(sc.get('distribution_centers', [])),
                  sc.get('sourcing_strategy'), sc.get('risk_factors')))
        
        # 政策影响
        for policy in data.get('policy_impacts', []):
            cursor.execute(f'''
            INSERT INTO policy_impacts_{data_year}
            (ticker, policy_type, policy_description, impact_description,
             mentioned_countries, decoupling_indicators)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (ticker, policy.get('policy_type'),
                  policy.get('policy_description'), policy.get('impact_description'),
                  json.dumps(policy.get('mentioned_countries', [])),
                  policy.get('decoupling_indicators')))
        
        self.conn.commit()
    
    # ==================== 关键词管理 ====================
    
    def save_keywords(self, keywords: List[Dict]):
        """保存关键词（所有年份共享）"""
        cursor = self.conn.cursor()
        for kw in keywords:
            cursor.execute('''
            INSERT OR IGNORE INTO keywords (keyword, keyword_category, extraction_method)
            VALUES (?, ?, ?)
            ''', (kw['keyword'], kw['category'], kw.get('method', 'llm')))
        self.conn.commit()
    
    def get_all_keywords(self) -> List[Dict]:
        """获取所有关键词"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT keyword, keyword_category FROM keywords')
        return [{'keyword': row[0], 'category': row[1]} for row in cursor.fetchall()]
    
    # ==================== TF-IDF管理 ====================
    
    def save_tfidf_scores(self, data_year: int, scores: List[Dict]):
        """保存TF-IDF得分到指定年份的表"""
        cursor = self.conn.cursor()
        for score in scores:
            cursor.execute(f'''
            INSERT INTO tfidf_scores_{data_year}
            (ticker, keyword, keyword_category, tf, idf, tfidf)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (score['ticker'], score['keyword'],
                  score['keyword_category'], score['tf'], score['idf'], score['tfidf']))
        self.conn.commit()
    
    def get_tfidf_scores(self, ticker: str, data_year: int) -> List[Dict]:
        """获取指定公司和年份的TF-IDF得分"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        SELECT keyword, keyword_category, tf, idf, tfidf 
        FROM tfidf_scores_{data_year}
        WHERE ticker = ?
        ''', (ticker,))
        return [{'keyword': row[0], 'keyword_category': row[1], 'tf': row[2],
                'idf': row[3], 'tfidf': row[4]} for row in cursor.fetchall()]
    
    # ==================== 量化得分管理 ====================
    
    def save_quantitative_scores(self, ticker: str, data_year: int, **scores):
        """保存量化得分到指定年份的表"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
        INSERT OR REPLACE INTO quantitative_scores_{data_year}
        (ticker, investment_attitude_score, expansion_score, contraction_score,
         china_investment_score, china_positive_score, china_negative_score,
         non_china_investment_score, non_china_raw_score, total_keywords_count, calculation_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ticker, scores.get('investment_attitude_score'), scores.get('expansion_score'),
              scores.get('contraction_score'), scores.get('china_investment_score'),
              scores.get('china_positive_score'), scores.get('china_negative_score'),
              scores.get('non_china_investment_score'),
              scores.get('non_china_raw_score'), scores.get('total_keywords_count'),
              datetime.now().isoformat()))
        self.conn.commit()
    
    def close(self):
        """关闭数据库连接"""
        self.conn.close()
    
    def __del__(self):
        """析构函数"""
        if hasattr(self, 'conn'):
            self.conn.close()


