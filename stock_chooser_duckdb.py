import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time # Import time module for timing
import configparser

def optimize_and_query_stock_data_duckdb():
    """
    Connects to DuckDB, creates/ensures stock_data table exists (for testing),
    and queries stocks satisfying specific conditions using DuckDB.
    """

    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()

    # 读取 .conf 文件
    config.read('./config.conf')
    earliest_time_limit=config['settings']['earliest_time_limit']                               # 交易日期的最早时限，该日前的交易数据，不会被纳入选择
    history_trading_days=config['settings']['history_trading_days']                             # 历史交易日选择范围。40: N个交易日，60: 60个交易日，80: 80个交易日
    main_board_amplitude_threshold=config['settings']['main_board_amplitude_threshold']         # 主板振幅。25: 25%, 30: 30%, 35: 35%
    non_main_board_amplitude_threshold=config['settings']['non_main_board_amplitude_threshold'] # 创业板和科创板主板振幅。35: 35%， 40: 40%。
    max_market_capitalization=config['settings']['max_market_capitalization']                   # 最大流通市值，单位亿。
    min_market_capitalization=config['settings']['min_market_capitalization']                   # 最小流通市值，单位亿。
    net_profit_growth_rate=config['settings']['history_trading_days']                           # 净利润增长率。-20: -20%。
    total_revenue_growth_rate=config['settings']['history_trading_days']                        # 营业总收入增长率。-20: -20%。

    # Connect to DuckDB database file
    # Ensure 'stock_data.duckdb' exists and contains data,
    # or uncomment the data generation part below for testing.
    con = duckdb.connect(database='stock_data.duckdb', read_only=False)
    print("连接到数据库: stock_data.duckdb")

    # --- Optional: Data Generation for Testing ---
    # If your 'stock_data.duckdb' file is not already populated,
    # you can uncomment and run this section to generate some test data.
    # It's recommended to run 'import_stock_data_to_duckdb.py' first
    # to populate the database with more realistic data.
    
    # Define columns for the DataFrame to match your DuckDB schema
    columns = [
        'stock_code', 'stock_name', 'trade_date', 'open_price', 'high_price',
        'low_price', 'close_price', 'prev_close_price', 'volume', 'turnover',
        'market_cap', 'total_market_cap', 'net_profit_ttm', 'cash_flow_ttm',
        'net_assets', 'total_assets', 'total_liabilities', 'net_profit_quarter',
        'mid_investor_buy', 'mid_investor_sell', 'large_investor_buy',
        'large_investor_sell', 'retail_investor_buy', 'retail_investor_sell',
        'institutional_buy', 'institutional_sell', 'hs300_component',
        'sse50_component', 'csi500_component', 'csi1000_component',
        'csi2000_component', 'gem_component', 'industry_level1',
        'industry_level2', 'industry_level3', 'price_0935', 'price_0945', 'price_0955'
    ]
    
    # Define schema for the stock_data table for explicit creation
    column_definitions = []
    # This reflects the schema from `import_stock_data_to_duckdb.py`
    header_mapping_for_schema_keys = [
        'stock_code', 'stock_name', 'trade_date', 'open_price', 'high_price',
        'low_price', 'close_price', 'prev_close_price', 'volume', 'turnover',
        'market_cap', 'total_market_cap', 'net_profit_ttm', 'cash_flow_ttm',
        'net_assets', 'total_assets', 'total_liabilities', 'net_profit_quarter',
        'mid_investor_buy', 'mid_investor_sell', 'large_investor_buy',
        'large_investor_sell', 'retail_investor_buy', 'retail_investor_sell',
        'institutional_buy', 'institutional_sell', 'hs300_component',
        'sse50_component', 'csi500_component', 'csi1000_component',
        'csi2000_component', 'gem_component', 'industry_level1',
        'industry_level2', 'industry_level3', 'price_0935', 'price_0945', 'price_0955'
    ]

    for db_column in header_mapping_for_schema_keys:
        if db_column in ['open_price', 'high_price', 'low_price', 'close_price',
                         'prev_close_price', 'volume', 'turnover', 'market_cap',
                         'total_market_cap', 'net_profit_ttm', 'cash_flow_ttm',
                         'net_assets', 'total_assets', 'total_liabilities',
                         'net_profit_quarter', 'mid_investor_buy', 'mid_investor_sell',
                         'large_investor_buy', 'large_investor_sell', 'retail_investor_buy',
                         'retail_investor_sell', 'institutional_buy', 'institutional_sell',
                         'price_0935', 'price_0945', 'price_0955']:
            column_definitions.append(f"{db_column} DOUBLE")
        elif db_column in ['hs300_component', 'sse50_component', 'csi500_component',
                           'csi1000_component', 'csi2000_component', 'gem_component']:
            column_definitions.append(f"{db_column} INTEGER")
        elif db_column == 'trade_date':
            column_definitions.append(f"{db_column} DATE")
        else:
            column_definitions.append(f"{db_column} VARCHAR")
            
    # Always try to create the table just in case it doesn't exist or schema changed
    try:
        con.execute(f"CREATE TABLE IF NOT EXISTS stock_data ({', '.join(column_definitions)});")
        print("Table 'stock_data' ensured to exist or created.")
    except Exception as e:
        print(f"Error ensuring table 'stock_data' exists: {e}")
        # If table creation fails, it's critical, so close connection and exit
        con.close()
        return
    
    # 查询库中的数据条数
    result = con.execute("SELECT COUNT(*) FROM stock_data;").fetchone()
    print(f"数据库中有{result[0]}条记录。")

    # Main Query SQL (optimized for DuckDB)
    # The SQL is mostly the same as DuckDB handles window functions efficiently.
    query_sql = f"""
    -- 📝 计算符合条件的股票交易日窗口
    WITH YearEndReports AS (
        -- ✅ 提取年报：report_date 以“1231”结尾，排除季度报/半年报
        SELECT
            f.stock_code,
            f.report_date,
            f.publish_date,
            f.R_np,                           -- 净利润
            f.R_operating_total_revenue,      -- 营业总收入
            ROW_NUMBER() OVER (
                PARTITION BY f.stock_code, SUBSTR(f.report_date, 1, 4) -- 按年份分组
                ORDER BY f.publish_date DESC                           -- 取最新发布的记录
            ) AS rn
        FROM stock_finance_data f
        WHERE f.report_date LIKE '%1231'
    ),
    YearEndReportsUnique AS (
        -- ✅ 保留每年最新的年报（去重）
        SELECT *
        FROM YearEndReports
        WHERE rn = 1
    ),
    FinanceWithYoY AS (
        -- ✅ 计算净利润和营业总收入同比增长率
        SELECT
            y1.stock_code,
            y1.report_date,
            y1.publish_date,
            y1.R_np,
            y1.R_operating_total_revenue,
            -- ✅ 上一年度的净利润
            LAG(y1.R_np, 1) OVER (
                PARTITION BY y1.stock_code
                ORDER BY y1.report_date
            ) AS prev_year_R_np,
            -- ✅ 上一年度的营业总收入
            LAG(y1.R_operating_total_revenue, 1) OVER (
                PARTITION BY y1.stock_code
                ORDER BY y1.report_date
            ) AS prev_year_revenue
        FROM YearEndReportsUnique y1
    ),
    StockWindows AS (
        SELECT
            t.stock_code,
            t.trade_date,
            t.stock_name,
            t.close_price,
            t.high_price,
            t.low_price,
            t.industry_level2,
            t.industry_level3,
            -- ✅ 流通市值换算成“亿”
            (t.market_cap / 100000000) AS market_cap_of_100_million,
            f.R_np,
            f.R_operating_total_revenue,
            -- ✅ 计算净利润同比增长率
            (f.R_np - f.prev_year_R_np) / NULLIF(f.prev_year_R_np, 0) AS net_profit_yoy,
            -- ✅ 计算营业总收入同比增长率
            (f.R_operating_total_revenue - f.prev_year_revenue) / NULLIF(f.prev_year_revenue, 0) AS revenue_yoy,
            -- ✅ N个交易日内（不含当日）的最高收盘价
            MAX(t.close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_close_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最高价（用于振幅计算）
            MAX(t.high_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_high_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最低价（用于振幅计算）
            MIN(t.low_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS min_low_n_days,
            -- ✅ N个交易日内（不含当日）的最低收盘价，用作振幅分母
            MIN(t.close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS min_close_n_days_for_amplitude_base,
            -- ✅ N个交易日内是否存在单日涨幅 ≥ 5%
            MAX(CASE
                WHEN (t.close_price - t.prev_close_price) / NULLIF(t.prev_close_price, 0) >= 0.05 THEN 1
                ELSE 0
            END) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS has_gain_5_percent,
            -- ✅ 行号：确保窗口至少包含N个交易日
            ROW_NUMBER() OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
            ) AS rn
        FROM
            stock_data t
        LEFT JOIN FinanceWithYoY f
            ON f.stock_code = t.stock_code
        AND f.publish_date = (
                -- ✅ 取最近一个已发布的年报
                SELECT MAX(f2.publish_date)
                FROM FinanceWithYoY f2
                WHERE f2.stock_code = t.stock_code
                AND CAST(f2.publish_date AS DATE) <= t.trade_date
            )
        WHERE
            t.stock_code NOT LIKE 'bj%' -- 排除北交所股票
            AND t.trade_date > '{earliest_time_limit}'
    ),
    FilteredRawData AS (
        SELECT
            sw.stock_code,
            sw.stock_name,
            sw.trade_date,
            sw.close_price,
            sw.industry_level2,
            sw.industry_level3,
            -- ✅ 标记连续交易日的“区块”
            SUM(new_block_flag) OVER (PARTITION BY sw.stock_code ORDER BY sw.trade_date) AS block_id,
            ROW_NUMBER() OVER (PARTITION BY sw.stock_code ORDER BY sw.trade_date) AS row_in_stock
        FROM (
            SELECT
                *,
                CASE
                    WHEN LAG(trade_date) OVER (PARTITION BY stock_code ORDER BY trade_date) IS NULL THEN 1
                    WHEN trade_date - LAG(trade_date) OVER (PARTITION BY stock_code ORDER BY trade_date) > 1 THEN 1
                    ELSE 0
                END AS new_block_flag
            FROM StockWindows
            WHERE
                -- 📌 条件0：窗口内至少有N个交易日数据
                rn > {history_trading_days}
                -- 📌 条件1：当日收盘价大于前N个交易日的最高收盘价
                AND close_price > max_close_n_days
                -- 📌 条件2：前N个交易日内有涨幅（大于等于5%）的K线
                AND has_gain_5_percent = 1
                -- 📌 条件3：前N个交易日的股票价格振幅度，上证和深证股票小于等于25%(30%, 35%)，创业板和科创析股票小于等于35%(40%, 40%)
                AND (
                    -- ✅ 根据股票代码板块（前缀）确定振幅阈值
                    CASE
                        WHEN min_close_n_days_for_amplitude_base > 0
                        THEN (max_high_n_days - min_low_n_days) * 1.0 / min_close_n_days_for_amplitude_base * 100
                        ELSE 999999 -- 避免除零错误
                    END
                ) <= (
                    CASE
                        -- ✅ 创业板（以300，301，302开头）或科创板（以688开头），小于等于35%(40%, 40%)
                        WHEN stock_code SIMILAR TO '(sz300|sz301|sz302|sh688)%' THEN {non_main_board_amplitude_threshold}
                        -- ✅ 上证主板（以600，601，603，605开头）小于等于25%(30%, 35%)
                        WHEN stock_code SIMILAR TO '(sh600|sh601|sh603|sh605)%' THEN {main_board_amplitude_threshold}
                        -- ✅ 深证主板（以000，001，002，003开头）小于等于25%(30%, 35%)
                        WHEN stock_code SIMILAR TO '(sz000|sz001|sz002|sz003)%' THEN {main_board_amplitude_threshold}
                        ELSE 1000
                    END
                )
                -- 📌 条件4：流通市值在30亿至500亿之间
                AND market_cap_of_100_million BETWEEN {min_market_capitalization} AND {max_market_capitalization}
                -- 📌 条件5：最近一个财报周期净利润同比增长率和营业总收入同比增长率大于等于-20%
                AND net_profit_yoy >= -0.2
                AND revenue_yoy >= -0.2
        ) AS sw
    ),
    BlockWithFilteredRows AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY stock_code, block_id ORDER BY trade_date) AS row_in_block,
            COUNT(*) OVER (PARTITION BY stock_code, block_id) AS block_size,
            COUNT(DISTINCT close_price) OVER (PARTITION BY stock_code, block_id) AS distinct_close_count
        FROM FilteredRawData
    ),
    FinalResult AS (
        SELECT *
        FROM BlockWithFilteredRows
        WHERE
            -- ✅ 限制连续交易日 <= 20 天
            block_size <= 20
            -- 📌 条件1.1: 如果区块内收盘价连续相同，剔除前 N-1 条，仅保留最后一条
            AND NOT (
                row_in_block < block_size AND distinct_close_count = 1
            )
            -- 📌 条件1.2: 每个区块只保留最早的一条记录
            AND row_in_block = 1
    )
    SELECT
        stock_code,
        stock_name,
        trade_date,
        industry_level2,
        industry_level3
    FROM FinalResult
    ORDER BY stock_code, trade_date;
    """

    print("\n--- 分析查询计划 (DuckDB) ---")
    # DuckDB provides 'EXPLAIN' for query plans
    con.execute("EXPLAIN " + query_sql)
    query_plan = con.fetchall()
    for step in query_plan:
        # print(step)
        pass
    print("--------------------------------------\n")

    print("\n执行筛选...")
    start_time = time.time()
    results_df = con.execute(query_sql).fetchdf() # Fetch results directly as a Pandas DataFrame
    end_time = time.time()
    print(f"筛选于: {end_time - start_time:.2f}秒内完成.")

    if not results_df.empty:
        num_results = len(results_df)
        print(f"\n筛选到 {num_results} 条股票及交易日期数据:")
        # 如果筛选到的记录数小于50，则直接打印
        print(results_df.head(50).to_string())
        if num_results > 50:
            # 否则导入到查询结果文件choose_result.csv文件中
            print("...")
            # Export to CSV with UTF-8 BOM encoding
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"stock_query_results_{timestamp}.csv"
            try:
                results_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
                print(f"筛选结果 (共 {num_results} 条记录) 已导出到文件 {output_filename}.")
            except Exception as e:
                print(f"导出到文件失败，原因: {e}")
        print(f"总记录数: {num_results} 条.")
    else:
        print("\n没有找到符合条件的股票及期交易日期数据.")

    # Close the database connection
    con.close()

if __name__ == '__main__':
    # Call the function to run the optimization and query
    optimize_and_query_stock_data_duckdb()
