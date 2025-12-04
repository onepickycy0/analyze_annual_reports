#!/bin/bash
# 完整流程运行脚本
# 用法：
#   ./run_full_pipeline.sh                    # 处理所有年份
#   ./run_full_pipeline.sh 2020 2021 2022    # 处理指定年份
#   ./run_full_pipeline.sh --limit 2          # 每年限制2个文件

set -e  # 遇到错误立即停止

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 解析参数
YEARS=""
LIMIT=""
DB="/root/liujie/nianbao-v2results/annual_reports_quantitative.db"

while [[ $# -gt 0 ]]; do
    case $1 in
        --limit)
            LIMIT="--limit $2"
            shift 2
            ;;
        --db)
            DB="$2"
            shift 2
            ;;
        --help|-h)
            echo "用法: $0 [选项] [年份列表]"
            echo ""
            echo "选项:"
            echo "  --limit N     每年处理N个文件"
            echo "  --db PATH     指定数据库路径"
            echo "  --help        显示此帮助信息"
            echo ""
            echo "示例:"
            echo "  $0                        # 处理所有年份"
            echo "  $0 2020 2021 2022         # 处理指定年份"
            echo "  $0 --limit 2              # 每年限制2个文件"
            echo "  $0 --limit 2 2023 2024    # 2023和2024年各处理2个文件"
            exit 0
            ;;
        *)
            YEARS="$YEARS $1"
            shift
            ;;
    esac
done

# 切换到项目目录
cd /root/liujie/nianbao-v2

print_info "======================================================================"
print_info "年报量化分析系统 - 完整流程"
print_info "======================================================================"
print_info "数据库: $DB"
[[ -n "$YEARS" ]] && print_info "年份: $YEARS" || print_info "年份: 全部"
[[ -n "$LIMIT" ]] && print_info "限制: $LIMIT"
print_info "======================================================================"
echo ""

# 开始计时
START_TIME=$(date +%s)

# 步骤1: 构建语料库
print_info "步骤1/5: 构建语料库..."
if python3 step1_build_corpus.py --db "$DB" $LIMIT --years$YEARS; then
    print_success "步骤1完成 ✓"
else
    print_error "步骤1失败 ✗"
    exit 1
fi
echo ""

# 步骤2: 提取关键词
print_info "步骤2/5: 提取关键词..."
if python3 step2_extract_keywords.py --db "$DB" --years $YEARS; then
    print_success "步骤2完成 ✓"
else
    print_error "步骤2失败 ✗"
    exit 1
fi
echo ""

# 步骤3: 计算TF-IDF
print_info "步骤3/5: 计算TF-IDF..."
if python3 step3_calculate_tfidf.py --db "$DB" --years $YEARS; then
    print_success "步骤3完成 ✓"
else
    print_error "步骤3失败 ✗"
    exit 1
fi
echo ""

# 步骤4: 计算量化得分
print_info "步骤4/5: 计算量化得分..."
if python3 step4_calculate_scores.py --db "$DB" --years $YEARS; then
    print_success "步骤4完成 ✓"
else
    print_error "步骤4失败 ✗"
    exit 1
fi
echo ""

# 步骤5: 生成Excel报告
print_info "步骤5/5: 生成Excel报告..."
OUTPUT_FILE="/root/liujie/nianbao-v2results/annual_reports_quantitative_analysis.xlsx"
if python3 step5_generate_report.py --db "$DB" --output "$OUTPUT_FILE"; then
    print_success "步骤5完成 ✓"
else
    print_error "步骤5失败 ✗"
    exit 1
fi
echo ""

# 结束计时
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

print_success "======================================================================"
print_success "🎉 全部完成！"
print_success "======================================================================"
print_success "耗时: ${MINUTES}分${SECONDS}秒"
print_success "报告: $OUTPUT_FILE"
print_success "数据库: $DB"
print_success "======================================================================"

# 显示数据库统计
print_info ""
print_info "数据库统计:"
python3 -c "
import sqlite3
conn = sqlite3.connect('$DB')
cursor = conn.cursor()

# 获取年份
years = []
for year in range(2014, 2025):
    cursor.execute(f'SELECT COUNT(*) FROM companies_{year}')
    count = cursor.fetchone()[0]
    if count > 0:
        years.append((year, count))

if years:
    for year, count in years:
        print(f'  {year}年: {count}家公司')
else:
    print('  无数据')

conn.close()
"

