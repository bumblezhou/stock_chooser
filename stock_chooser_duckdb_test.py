import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time # Import time module for timing
import configparser

# 计算工作日间隔
def calculate_workday_diff(dates):
    dates = dates.values
    return pd.Series([float('inf')] + [len(pd.date_range(start=dates[i-1], end=dates[i], freq='B')) - 1 for i in range(1, len(dates))])

# 筛选函数：筛选结果后N个交易日内筛选出的日期不作为筛选结果。
def filter_records(group):
    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()
    config.read('./config.conf')
    range_days_of_cond_1_2=config['settings']['range_days_of_cond_1_2']         # 使用条件1.2时，其后N个交易日设定值
    range_days_of_cond_1_2=int(range_days_of_cond_1_2)

    if len(group) <= 1:
        return group
    group = group.copy()
    group['workday_diff'] = calculate_workday_diff(group['trade_date'])
    keep = [True] * len(group)  # 初始化保留标志
    last_kept_idx = 0  # 记录最后保留的记录索引

    # 从第二条记录开始检查
    for i in range(1, len(group)):
        # 计算当前记录与最后保留记录的间隔
        workday_diff = len(pd.date_range(start=group.iloc[last_kept_idx]['trade_date'], end=group.iloc[i]['trade_date'], freq='B')) - 1
        if workday_diff <= range_days_of_cond_1_2:
            # 如果间隔≤20，删除最后保留的记录和当前记录
            keep[last_kept_idx] = False
            keep[i] = False
        else:
            # 保留当前记录，更新最后保留的索引
            last_kept_idx = i

    # 确保第一条记录保留
    keep[0] = True
    return group[keep].drop(columns='workday_diff')

# 筛选函数：次高收盘价为前一个交易日收盘价的不作为筛选结果。
def mark_records(group):
    if len(group) <= 1:
        return group
    group = group.copy()
    # 初始化标记列，0 表示保留，1 表示删除
    group['delete_flag'] = 0
    # 计算相邻记录的工作日间隔和价格差异
    dates = group['trade_date'].values
    prices = group['close_price'].values
    for i in range(1, len(group)):
        # 计算工作日间隔（忽略周末）
        workday_diff = len(pd.date_range(start=dates[i-1], end=dates[i], freq='B')) - 1
        # 如果间隔为1个工作日且后一条记录的 close_price 大于前一条
        if workday_diff == 1 and prices[i] > prices[i-1]:
            group.iloc[i, group.columns.get_loc('delete_flag')] = 1
    return group

# 从库中筛选符合条件的记录，处理后导出到结果csv文件。
def optimize_and_query_stock_data_duckdb():
    """
    Connects to DuckDB, creates/ensures stock_data table exists (for testing),
    and queries stocks satisfying specific conditions using DuckDB.
    """

    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()

    # 读取 .conf 文件
    config.read('./config.conf')
    earliest_time_limit=config['settings']['earliest_time_limit']                                   # 交易日期的最早时限，该日前的交易数据，不会被纳入选择
    cond1_and_cond3=config['settings']['cond1_and_cond3']                                           # 条件1和条件3的配置项。
    cond2=config['settings']['cond2']                                                               # 条件2：前N个交易日内有涨幅（大于等于5%）的K线
    apply_cond2_or_not=config['settings']['apply_cond2_or_not']                                     # 是否启用条件2：yes, 启用; no: 不启用。
    apply_cond5_or_not=config['settings']['apply_cond5_or_not']                                     # 是否启用条件5：yes, 启用; no: 不启用。
    # history_trading_days=config['settings']['history_trading_days']                               # 条件1：历史交易日选择范围。40: 40个交易日，60: 60个交易日，80: 80个交易日
    # main_board_amplitude_threshold=config['settings']['main_board_amplitude_threshold']           # 条件3：主板振幅。25: 25%, 30: 30%, 35: 35%
    # non_main_board_amplitude_threshold=config['settings']['non_main_board_amplitude_threshold']   # 条件3：创业板和科创板主板振幅。35: 35%， 40: 40%。
    history_trading_days=cond1_and_cond3.split('_')[0]
    main_board_amplitude_threshold=cond1_and_cond3.split('_')[1]
    non_main_board_amplitude_threshold=cond1_and_cond3.split('_')[2]
    max_market_capitalization=config['settings']['max_market_capitalization']                       # 最大流通市值，单位亿。
    min_market_capitalization=config['settings']['min_market_capitalization']                       # 最小流通市值，单位亿。
    net_profit_growth_rate=config['settings']['net_profit_growth_rate']                             # 净利润增长率。-20: -20%。
    total_revenue_growth_rate=config['settings']['total_revenue_growth_rate']                       # 营业总收入增长率。-20: -20%。
    use_cond_1_1_or_cond_1_2=config['settings']['use_cond_1_1_or_cond_1_2']                         # 使用条件1.1还是1.2进行筛选：1.1，使用条件1.1; 1.2, 使用条件1.2。
    range_days_of_cond_1_2=config['settings']['range_days_of_cond_1_2']                             # 使用条件1.2时，其后N个交易日设定值

    cond2_sql_where_clause = ''
    if apply_cond2_or_not == 'yes':
        cond2_sql_where_clause = 'AND has_gain_5_percent = 1'
    if apply_cond2_or_not == 'no':
        cond2_sql_where_clause = '-- AND has_gain_5_percent = 1'

    cond5_sql_where_clause = ''
    if apply_cond5_or_not == 'yes':
        cond5_sql_where_clause = f'AND net_profit_yoy >= {net_profit_growth_rate} AND revenue_yoy >= {total_revenue_growth_rate}'
    if apply_cond5_or_not == 'no':
        cond5_sql_where_clause = f'-- AND net_profit_yoy >= {net_profit_growth_rate} AND revenue_yoy >= {total_revenue_growth_rate}'

    # Connect to DuckDB database file
    # Ensure 'stock_data.duckdb' exists and contains data,
    # or uncomment the data generation part below for testing.
    con = duckdb.connect(database='stock_data.duckdb', read_only=False)
    print("连接到数据库: stock_data.duckdb")
            
    # 查询库中的数据条数
    result = con.execute("SELECT COUNT(*) FROM stock_data;").fetchone()
    print(f"数据库中有{result[0]}条记录。")

    # Main Query SQL (optimized for DuckDB)
    # The SQL is mostly the same as DuckDB handles window functions efficiently.
    query_sql = f"""
    -- 📝 计算符合条件的股票交易日窗口
    WITH DeduplicatedStockData AS (
        -- ✅ 去掉 stock_data 中完全重复的行
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY stock_code, trade_date, open_price, close_price, high_price, low_price, prev_close_price, market_cap
                    ORDER BY trade_date
                ) AS rn
            FROM stock_data
        )
        WHERE rn = 1
    ),
    StockWithRiseFall AS (
        -- ✅ 计算复权涨跌幅，公式: 复权涨跌幅 = 收盘价 / 前收盘价 - 1
        SELECT *,
            (close_price / NULLIF(prev_close_price, 0)) - 1 AS rise_fall
        FROM DeduplicatedStockData
        WHERE rn = 1
    ),
    AdjustmentFactorComputed AS (
        -- ✅ 计算复权因子, 公式: 复权因子 = (1 + 复权涨跌幅).cumprod()
        SELECT *,
            EXP(SUM(LN(1 + rise_fall)) OVER (PARTITION BY stock_code ORDER BY trade_date)) AS adjustment_factor
        FROM StockWithRiseFall
    ),
    LastRecordComputed AS (
        -- ✅ 获取每个 stock_code 的最后一条记录的收盘价和复权因子
            SELECT 
            stock_code,
            close_price AS last_close_price,
            adjustment_factor AS last_adjustment_factor
        FROM (
            SELECT 
                stock_code,
                close_price,
                adjustment_factor,
                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date DESC) AS rn
            FROM AdjustmentFactorComputed
        ) t
        WHERE rn = 1
    ),
    AdjustedStockData AS (
        SELECT 
            a.*,
            -- ✅ 计算前复权收盘价, 公式: 前复权收盘价 = 复权因子 * (最后一条数据的收盘价 / 最后一条数据的复权因子)
            a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0)) AS adj_close_price,
            -- ✅ 前复权其他价格
            (a.open_price / NULLIF(a.close_price, 0)) * 
                (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_open_price,
            (a.high_price / NULLIF(a.close_price, 0)) * 
                (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_high_price,
            (a.low_price / NULLIF(a.close_price, 0)) * 
                (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_low_price,
            (a.prev_close_price / NULLIF(a.close_price, 0)) * 
                (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_prev_close_price
        FROM AdjustmentFactorComputed a
        LEFT JOIN LastRecordComputed l ON a.stock_code = l.stock_code
    ),
    StockWindows AS (
        SELECT
            t.stock_code,
            t.trade_date,
            t.stock_name,
            t.close_price,
            t.high_price,
            t.low_price,
            t.open_price,
            t.adj_close_price,
            t.adj_high_price,
            t.adj_low_price,
            t.adj_open_price,
            t.industry_level2,
            t.industry_level3,
            -- ✅ 流通市值换算成“亿”
            (t.market_cap / 100000000) AS market_cap_of_100_million,
            -- ✅ N个交易日内（不含当日）的最高收盘价, 使用的是复权后的收盘价
            MAX(t.adj_close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_close_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最高价（用于振幅计算）, 使用的是复权后的最高价
            MAX(t.adj_high_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_high_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最低价（用于振幅计算）, 使用的是复权后的最低价
            MIN(t.adj_low_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS min_low_n_days,
            -- ✅ N个交易日内（不含当日）的第一个交易日的开盘价，用作振幅分母。使用的是复权后的开盘价。
            FIRST_VALUE(t.adj_open_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS open_price_of_first_day_of_n_days,
            -- ✅ N个交易日内是否存在单日涨幅 ≥ 5%
            MAX(CASE
                WHEN (t.adj_close_price - t.adj_prev_close_price) / NULLIF(t.adj_prev_close_price, 0) >= {cond2} THEN 1
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
            AdjustedStockData t
        WHERE
            -- ✅ 排除北交所股票
            t.stock_code NOT LIKE 'bj%' AND
            t.trade_date > '{earliest_time_limit}'
    ),
    FilteredRawData AS (
        SELECT
            sw.stock_code,
            sw.stock_name,
            sw.trade_date,
            sw.adj_close_price,
            sw.max_close_n_days,
            sw.industry_level2,
            sw.industry_level3
        FROM
            StockWindows AS sw
        WHERE
            -- 📌 条件0：窗口内至少有N个交易日数据
            rn > {history_trading_days}
            -- 📌 条件1：当日收盘价大于前N个交易日的最高收盘价
            AND adj_close_price > max_close_n_days
    )
    SELECT
        stock_code,
        stock_name,
        trade_date,
        adj_close_price,
        max_close_n_days,
        industry_level2,
        industry_level3
    FROM FilteredRawData
    ORDER BY stock_code, trade_date;
    """

    # # 调试代码
    # print(f"SQL: {query_sql}")
    # return

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

    new_df = results_df[results_df['stock_name'] == '赢时胜'].copy()
    print(new_df.to_string())

    # # 确保 trade_date 是 datetime 格式
    # results_df['trade_date'] = pd.to_datetime(results_df['trade_date'])
    # # 按 stock_code 和 trade_date 升序排序
    # results_df = results_df.sort_values(['stock_code', 'trade_date'], ascending=[True, True]).reset_index(drop=True)

    # if use_cond_1_1_or_cond_1_2 == "1.1":
    #     # 📌 条件1.1: 次高收盘价为前一个交易日收盘价的不作为筛选结果
    #     # 按 stock_code 分组并添加删除标记
    #     results_df = results_df.groupby('stock_code', group_keys=False).apply(mark_records)
    #     # 删除标记为“删除”的记录
    #     results_df = results_df[results_df['delete_flag'] == 0].drop(columns='delete_flag').reset_index(drop=True)

    # if use_cond_1_1_or_cond_1_2 == "1.2":
    #     # 📌 条件1.2: 筛选结果后20个交易日内筛选出的日期不作为筛选结果
    #     results_df = results_df.groupby('stock_code', group_keys=False).apply(filter_records).reset_index(drop=True)

    # if not results_df.empty:
    #     num_results = len(results_df)
    #     print(f"\n筛选到 {num_results} 条股票及交易日期数据:")
    #     # # 如果筛选到的记录数小于50，则直接打印
    #     # print(results_df.head(50).to_string())
    #     # new_df = results_df[results_df['stock_name'] == '招商南油'].copy()
    #     new_df = results_df[results_df['stock_name'] == '赢时胜'].copy()
    #     print(new_df.to_string())
    #     if num_results > 50:
    #         # 否则导入到查询结果文件choose_result.csv文件中
    #         print("...")
    #         # Export to CSV with UTF-8 BOM encoding
    #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #         if use_cond_1_1_or_cond_1_2 == '1.2':
    #             filter_conditions = f"{history_trading_days}days_{main_board_amplitude_threshold}per_{non_main_board_amplitude_threshold}per_{apply_cond2_or_not}_cond2_cond1.2_{range_days_of_cond_1_2}days_{apply_cond5_or_not}_cond5"
    #         else:
    #             filter_conditions = f"{history_trading_days}days_{main_board_amplitude_threshold}per_{non_main_board_amplitude_threshold}per_{apply_cond2_or_not}_cond2_{apply_cond5_or_not}_cond5"
    #         output_filename = f"stock_query_results_{timestamp}_cond{use_cond_1_1_or_cond_1_2}_{filter_conditions}.csv"
    #         try:
    #             results_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    #             print(f"筛选结果 (共 {num_results} 条记录) 已导出到文件 {output_filename}.")
    #         except Exception as e:
    #             print(f"导出到文件失败，原因: {e}")
    #     print(f"总记录数: {num_results} 条.")
    # else:
    #     print("\n没有找到符合条件的股票及期交易日期数据.")

    # Close the database connection
    con.close()

if __name__ == '__main__':
    # Call the function to run the optimization and query
    optimize_and_query_stock_data_duckdb()
