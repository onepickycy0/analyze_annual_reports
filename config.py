# -*- coding: utf-8 -*-
"""
配置文件
"""

# OpenAI API配置
OPENAI_CONFIG = {
    "base_url": "https://api.xty.app/v1",
    "api_key": "sk-HVA4cR9kk5xkzzu957F0C213320f4475848032FdC221F2Ba",
    "model": "gpt-4.1-mini-2025-04-14",
    "temperature": 0.3,
}

# 数据目录配置
DATA_DIRS = {
    "2002": "/root/liujie/nianbao-v2/gpt-5/reports/2002",
    "2003": "/root/liujie/nianbao-v2/gpt-5/reports/2003",
    "2004": "/root/liujie/nianbao-v2/gpt-5/reports/2004",
    "2005": "/root/liujie/nianbao-v2/gpt-5/reports/2005",
    "2006": "/root/liujie/nianbao-v2/gpt-5/reports/2006",
    "2007": "/root/liujie/nianbao-v2/gpt-5/reports/2007",
    "2008": "/root/liujie/nianbao-v2/gpt-5/reports/2008",
    "2009": "/root/liujie/nianbao-v2/gpt-5/reports/2009",
    "2010": "/root/liujie/nianbao-v2/gpt-5/reports/2010",
    "2011": "/root/liujie/nianbao-v2/gpt-5/reports/2011",
    "2012": "/root/liujie/nianbao-v2/gpt-5/reports/2012",
    "2013": "/root/liujie/nianbao-v2/gpt-5/reports/2013",
    "2014": "/root/liujie/nianbao-v2/gpt-5/reports/2014",
    "2015": "/root/liujie/nianbao-v2/gpt-5/reports/2015",
    "2016": "/root/liujie/nianbao-v2/gpt-5/reports/2016",
    "2017": "/root/liujie/nianbao-v2/gpt-5/reports/2017",
    "2018": "/root/liujie/nianbao-v2/gpt-5/reports/2018",
    "2019": "/root/liujie/nianbao-v2/gpt-5/reports/2019",
    "2020": "/root/liujie/nianbao-v2/gpt-5/reports/2020",
    "2021": "/root/liujie/nianbao-v2/gpt-5/reports/2021",
    "2022": "/root/liujie/nianbao-v2/gpt-5/reports/2022",
    "2023": "/root/liujie/nianbao-v2/gpt-5/reports/2023",
    "2024": "/root/liujie/nianbao-v2/gpt-5/reports/2024",
}

# 数据库配置
DATABASE_CONFIG = {
    "base_path": "/root/liujie/nianbao-v2/gpt-5",
    "db_name": "annual_reports_quantitative.db",  # 统一数据库存储所有年份
}

# 分析配置
ANALYSIS_CONFIG = {
    # 文本长度限制（字符数）
    "max_text_length": 30000,
    
    # 批量处理时的延迟（秒）
    "batch_delay": 2,
    
    # 是否使用流式输出
    "use_stream": True,
    
    # 默认处理文件数量限制（None表示全部）
    "default_limit": None,  # 处理所有文件
}

# 提示词模板
PROMPT_TEMPLATES = {
    "investment": """
请从以下10-K年报文本中提取对外投资相关信息，以JSON格式返回：
{{
    "foreign_investments": [
        {{
            "investment_type": "类型（如：并购、新建、合资等）",
            "target_country": "目标国家",
            "target_region": "目标地区",
            "investment_amount": "投资金额（数值，单位百万美元）",
            "investment_purpose": "投资目的",
            "description": "详细描述"
        }}
    ]
}}

如果没有找到相关信息，返回空数组。

年报文本：
""",

    "trade": """
请从以下10-K年报文本中提取全球贸易相关信息，以JSON格式返回：
{{
    "global_trade": {{
        "total_revenue": "总营收（百万美元）",
        "international_revenue": "国际营收（百万美元）",
        "international_revenue_pct": "国际营收占比（百分比）",
        "major_markets": ["主要市场国家/地区"],
        "geographic_segments": [
            {{
                "region": "地区名称",
                "country": "国家",
                "revenue": "营收（百万美元）",
                "revenue_pct": "营收占比",
                "description": "描述"
            }}
        ]
    }}
}}

年报文本：
""",

    "supply_chain": """
请从以下10-K年报文本中提取供应链和生产布局相关信息，以JSON格式返回：
{{
    "supply_chain": {{
        "supplier_countries": ["供应商所在国家"],
        "manufacturing_locations": ["生产设施所在国家/地区"],
        "distribution_centers": ["分销中心所在地"],
        "sourcing_strategy": "采购策略描述",
        "risk_factors": "供应链风险因素"
    }}
}}

年报文本：
""",

    "policy": """
请从以下10-K年报文本中提取政策影响和脱钩相关信息，以JSON格式返回：
{{
    "policy_impacts": [
        {{
            "policy_type": "政策类型（如：贸易政策、关税、制裁等）",
            "policy_description": "政策描述",
            "impact_description": "对公司的影响",
            "mentioned_countries": ["涉及的国家"],
            "decoupling_indicators": "是否提及脱钩、供应链转移等（是/否/不确定）及相关描述"
        }}
    ]
}}

特别关注：
1. 中美贸易关系相关内容
2. 供应链多元化或转移的表述
3. 对中国市场的依赖度变化
4. 政府政策对投资决策的影响

年报文本：
"""
}

# 关键词配置
KEYWORDS = {
    # 脱钩相关关键词
    "decoupling": [
        "decouple", "decoupling", "脱钩",
        "supply chain diversification", "供应链多元化",
        "reshoring", "回流",
        "nearshoring", "近岸外包",
        "alternative sourcing", "替代采购",
        "supply chain relocation", "供应链迁移"
    ],
    
    # 中国相关关键词
    "china": [
        "China", "中国",
        "Chinese", "中国的",
        "PRC", "People's Republic of China"
    ],
    
    # 风险因素关键词
    "risk": [
        "risk", "风险",
        "uncertainty", "不确定性",
        "challenge", "挑战",
        "threat", "威胁"
    ]
}

# 输出配置
OUTPUT_CONFIG = {
    # Excel文件配置
    "excel": {
        "single_year_template": "annual_report_analysis_{year}.xlsx",
        "multi_year_template": "multi_year_analysis_report.xlsx",
    },
    
    # 可视化配置
    "visualization": {
        "output_dir": "visualizations",
        "figure_size": (12, 6),
        "dpi": 300,
        "style": "seaborn",
    }
}

# 日志配置
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "analysis.log"
}

# 量化分析配置
QUANTITATIVE_CONFIG = {
    "v2_db_name": "annual_reports_quantitative.db",
    "batch_size_keywords": 50,
    "normalization_method": "nss_density",  # NSS密度校正方法
    "max_concurrent": 8,
    "output_dir": "/root/liujie/nianbao-v2/gpt-5",  # 输出目录
}

# 语料库构建提示词
CORPUS_EXTRACTION_PROMPT = """
请分析以下10-K年报文本，完成两项任务：

任务1：提取五个维度的结构化数据（JSON格式）
1. 对外投资 (foreign_investments) - 数组
2. 全球贸易 (global_trade) - 对象
3. 地理分布 (geographic_segments) - 数组
4. 供应链 (supply_chain) - 对象
5. 政策影响 (policy_impacts) - 数组

任务2：提取关键文本段落并分类
- 提取所有与投资、扩张、收缩、供应链相关的**原始完整段落**
- **重要**：必须保留原文！不要改写、不要摘要、不要缩减！直接复制原文中的完整段落！
- 每个段落应该是200-50000字符的完整段落，包含完整的上下文和细节
- 提取所有相关的段落（不是句子，是完整段落）
- 为每个段落分类：
  * expansion: 描述扩张、投资、增长、进入新市场的段落
  * contraction: 描述收缩、撤资、削减、退出市场的段落
  * neutral: 中性描述现状的段落

返回JSON格式：
{{
    "structured_data": {{
        "foreign_investments": [
            {{
                "investment_type": "类型",
                "target_country": "目标国家",
                "target_region": "目标地区",
                "investment_amount": 金额数值（统一转换为百万美元，必须是纯数字或null），
                "investment_amount_original": "原文中的金额表述（含单位）",
                "investment_purpose": "目的",
                "description": "描述"
            }}
        ],
        "global_trade": {{
            "total_revenue": 总营收数值（统一转换为百万美元，必须是纯数字或null）,
            "total_revenue_original": "原文中的营收表述（含单位）",
            "international_revenue": 国际营收数值（统一转换为百万美元）,
            "international_revenue_original": "原文中的国际营收表述（含单位）",
            "international_revenue_pct": 国际营收占比（百分比数值）,
            "major_markets": ["市场1", "市场2"]
        }},
        "geographic_segments": [
            {{
                "region": "地区",
                "country": "国家",
                "revenue": 营收数值（统一转换为百万美元）,
                "revenue_original": "原文中的营收表述（含单位）",
                "revenue_pct": 占比（百分比数值）
            }}
        ],
        "supply_chain": {{
            "supplier_countries": ["国家1", "国家2"],
            "manufacturing_locations": ["地点1", "地点2"],
            "distribution_centers": ["中心1", "中心2"],
            "sourcing_strategy": "采购策略",
            "risk_factors": "风险因素"
        }},
        "policy_impacts": [
            {{
                "policy_type": "政策类型",
                "policy_description": "政策描述",
                "impact_description": "影响描述",
                "mentioned_countries": ["国家1", "国家2"],
                "decoupling_indicators": "脱钩指标描述"
            }}
        ]
    }},
    "text_segments": [
        {{
            "text": "【必须是原文完整段落，200-5000字符，保留所有细节】",
            "category": "expansion/contraction/neutral",
            "type": "investment/trade/supply_chain/policy",
            "source_section": "Item 1/Item 7/etc"
        }}
    ]
}}

**关键要求**：
1. text字段必须是原文段落，不能改写或总结
2. 每个段落应该是完整的、连贯的、有上下文的
3. 优先提取包含具体数字、国家名、投资金额、扩张计划的段落
4. 确保提取15-30个段落（不是句子）

**金额单位统一规则（极其重要）**：
1. 所有金额必须识别原文单位（million/百万、billion/十亿、thousand/千等）
2. 统一转换为百万美元（million USD）：
   - 原文 "5 billion" → investment_amount: 5000, investment_amount_original: "5 billion"
   - 原文 "$120 million" → investment_amount: 120, investment_amount_original: "$120 million"
   - 原文 "2.5千万美元" → investment_amount: 25, investment_amount_original: "2.5千万美元"
   - 原文 "500 thousand" → investment_amount: 0.5, investment_amount_original: "500 thousand"
3. 如果原文没有明确金额，设置为 null
4. 同时保留原文表述在 *_original 字段中，方便审核
5. 百分比保留为数值（如 "35%" → 35）

年报文本：
{text}
"""

# 关键词提取提示词
KEYWORD_EXTRACTION_PROMPT = """
请分析以下语料库中的文本段落，提取关键词并分类。

要求：
1. 识别所有表示投资、扩张、收缩、地理转移的关键词和短语
2. 不要局限于固定词表，要理解语义
3. 提取单词、短语、专业术语

分类标准：
- expansion: 扩张、投资、增长相关
  例如: expand, investment, new facility, capacity increase, market entry
  
- contraction: 收缩、撤资、削减相关
  例如: exit, closure, divest, reduce capacity, layoff
  
- china_positive: 对中国市场的正面表述
  例如: china expansion, invest in china, growth in china, important market
  
- china_negative: 对中国市场的负面表述或脱钩迹象
  例如: reduce dependence on china, diversify from china, china risk, exit china
  
- non_china_regions: 非中国地区投资
  例如: vietnam expansion, india manufacturing, mexico facility, nearshoring

语料库文本段落：
{corpus_texts}

返回JSON格式：
{{
    "keywords": [
        {{
            "keyword": "关键词或短语",
            "category": "expansion/contraction/china_positive/china_negative/non_china_regions",
            "context": "出现的上下文示例"
        }}
    ]
}}
"""



