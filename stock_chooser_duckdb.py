import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time # Import time module for timing

def optimize_and_query_stock_data_duckdb():
    """
    Connects to DuckDB, creates/ensures stock_data table exists (for testing),
    and queries stocks satisfying specific conditions using DuckDB.
    """
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
    query_sql = """
    WITH StockWindows AS (
        SELECT
            stock_code,
            trade_date,
            close_price,
            high_price,
            low_price,
            -- Calculate maximum close price within the 40-day window (not including current day)
            -- 计算40天内（包括当前日）的最高收盘价
            MAX(close_price) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS max_close_40d,
            -- Calculate maximum high price within the 40-day window (for amplitude calculation)
            -- 计算40天窗口内的最高价格（用于幅度计算）
            MAX(high_price) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS max_high_40d,
            -- Calculate minimum low price within the 40-day window (for amplitude calculation)
            -- 计算40天窗口内的最低价格（用于振幅计算）
            MIN(low_price) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS min_low_40d,
            -- Calculate minimum close price within the 40-day window (as denominator for amplitude calculation, to avoid division by zero)
            -- 在40天的时间窗口内计算最低收盘价（作为振幅计算的分母，以避免除以零）
            MIN(close_price) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS min_close_40d_for_amplitude_base,
            -- Row number to ensure the window has at least 40 data points for calculation
            -- 行号以确保窗口至少有 40 个数据点用于计算
            ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date) as rn
        FROM
            stock_data
        WHERE
            stock_code NOT LIKE 'bj%' AND trade_date > '2022-01-01 00:00:00'
    )
    SELECT
        sw.stock_code,
        sw.trade_date
    FROM
        StockWindows sw
    WHERE
        -- Ensure the current window contains at least 40 trading days of data
        -- 确保当前窗口包含至少40个交易日的数据
        sw.rn > 40
        -- Condition 1: Current day's close price is greater than or equal to the highest close price of the previous 40 trading days
        -- 条件1：当日收盘价大于前40个交易日的最高收盘价
        AND sw.close_price > sw.max_close_40d
        -- Condition 2: The stock price amplitude over the previous 40 trading days meets the sector requirements
        -- 条件2：前40个交易日的股票价格振幅度，上证和深证股票小于等于30%，创业板和科创析股票小于等于40%
        AND (
            -- Calculate amplitude percentage: (Max_High - Min_Low) / Min_Close * 100%
            -- If denominator is 0, set a very large value to fail the condition
            -- 计算振幅百分比：(Max_High - Min_Low) / Min_Close * 100%
            -- 如果分母为0，则设置一个非常大的值设置为以避免除零的错误
            CASE
                WHEN sw.min_close_40d_for_amplitude_base > 0
                THEN (sw.max_high_40d - sw.min_low_40d) * 1.0 / sw.min_close_40d_for_amplitude_base * 100
                ELSE 999999
            END
        ) <= (
            -- Determine the amplitude threshold based on stock code prefix (sector)
            -- 根据股票代码前缀（板块）确定振幅阈值
            CASE
                -- ChiNext board (starts with 300) or STAR market (starts with 688)
                -- 创业板（以300开头）或科创板（以688开头），小于等于%40
                WHEN sw.stock_code LIKE 'sz300%' OR sw.stock_code LIKE 'sh688%' THEN 40
                -- Shanghai Main Board (starts with 60)
                -- 上证主板小于等于30%
                WHEN sw.stock_code LIKE 'sh60%' THEN 30
                -- Shenzhen Main Board (starts with 000)
                -- 深证主板小于等于30%
                WHEN sw.stock_code LIKE 'sz000%' THEN 30
                -- Other stock types, set a high default threshold so they don't easily meet the condition
                -- 至于其他板块，设置一个很高的振幅阈值，不作考虑
                ELSE 1000
            END
        )
    -- Condition 3: The stock price amplitude over the previous 40 trading days meets the sector requirements
        -- 条件3：前40个交易日的股票价格振幅度，上证和深证股票大于等于15%，创业板和科创析股票大于等于20%
        AND (
            -- Calculate amplitude percentage: (Max_High - Min_Low) / Min_Close * 100%
            -- If denominator is 0, set a very large value to fail the condition
            -- 计算振幅百分比：(Max_High - Min_Low) / Min_Close * 100%
            -- 如果分母为0，则设置一个非常大的值设置为以避免除零的错误
            CASE
                WHEN sw.min_close_40d_for_amplitude_base > 0
                THEN (sw.max_high_40d - sw.min_low_40d) * 1.0 / sw.min_close_40d_for_amplitude_base * 100
                ELSE 999999
            END
        ) >= (
            -- Determine the amplitude threshold based on stock code prefix (sector)
            -- 根据股票代码前缀（板块）确定振幅阈值
            CASE
                -- ChiNext board (starts with 300) or STAR market (starts with 688)
                -- 创业板（以300开头）或科创板（以688开头），大于等于%20
                WHEN sw.stock_code LIKE 'sz300%' OR sw.stock_code LIKE 'sh688%' THEN 20
                -- Shanghai Main Board (starts with 60)
                -- 上证主板大于等于30%
                WHEN sw.stock_code LIKE 'sh60%' THEN 15
                -- Shenzhen Main Board (starts with 000)
                -- 深证主板大于等于30%
                WHEN sw.stock_code LIKE 'sz000%' THEN 15
                -- Other stock types, set a high default threshold so they don't easily meet the condition
                -- 至于其他板块，设置一个很高的振幅阈值，不作考虑
                ELSE 1000
            END
        )
    ORDER BY
        sw.stock_code, sw.trade_date;
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
