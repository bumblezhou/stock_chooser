import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time # Import time module for timing
import configparser

# 计算工作日间隔
def calculate_workday_diff(dates):
    dates = dates.values
    return pd.Series([float('inf')] + [len(pd.date_range(start=dates[i-1], end=dates[i], freq='B')) - 1 for i in range(1, len(dates))])

# 筛选函数：筛选结果后20个交易日内筛选出的日期不作为筛选结果。
def filter_records(group):
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
        if workday_diff <= 20:
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
    # history_trading_days=config['settings']['history_trading_days']                               # 条件1：历史交易日选择范围。40: N个交易日，60: 60个交易日，80: 80个交易日
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

    cond2_sql_where_clause = ''
    if apply_cond2_or_not == 'yes':
        cond2_sql_where_clause = 'AND has_gain_5_percent = 1'
    if apply_cond2_or_not == 'no':
        cond2_sql_where_clause = '-- AND has_gain_5_percent = 1'


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
                WHEN (t.close_price - t.prev_close_price) / NULLIF(t.prev_close_price, 0) >= {cond2} THEN 1
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
            sw.industry_level3
        FROM
            StockWindows AS sw
        WHERE
            -- 📌 条件0：窗口内至少有N个交易日数据
            rn > {history_trading_days}
            -- 📌 条件1：当日收盘价大于前N个交易日的最高收盘价
            AND close_price > max_close_n_days
            -- 📌 条件2：前N个交易日内有涨幅（大于等于5%）的K线
            -- AND has_gain_5_percent = 1
            {cond2_sql_where_clause}
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
            AND net_profit_yoy >= {net_profit_growth_rate}
            AND revenue_yoy >= {total_revenue_growth_rate}
    )
    SELECT
        stock_code,
        stock_name,
        trade_date,
        close_price,
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

    # 确保 trade_date 是 datetime 格式
    results_df['trade_date'] = pd.to_datetime(results_df['trade_date'])
    # 按 stock_code 和 trade_date 升序排序
    results_df = results_df.sort_values(['stock_code', 'trade_date'], ascending=[True, True]).reset_index(drop=True)

    if use_cond_1_1_or_cond_1_2 == "1.1":
        # 📌 条件1.1: 次高收盘价为前一个交易日收盘价的不作为筛选结果
        # 按 stock_code 分组并添加删除标记
        results_df = results_df.groupby('stock_code', group_keys=False).apply(mark_records)
        # 删除标记为“删除”的记录
        results_df = results_df[results_df['delete_flag'] == 0].drop(columns='delete_flag').reset_index(drop=True)

    if use_cond_1_1_or_cond_1_2 == "1.2":
        # 📌 条件1.2: 筛选结果后20个交易日内筛选出的日期不作为筛选结果
        results_df = results_df.groupby('stock_code', group_keys=False).apply(filter_records).reset_index(drop=True)

    if not results_df.empty:
        num_results = len(results_df)
        print(f"\n筛选到 {num_results} 条股票及交易日期数据:")
        # # 如果筛选到的记录数小于50，则直接打印
        # print(results_df.head(50).to_string())
        new_df = results_df[results_df['stock_name'] == '招商南油'].copy()
        print(new_df.to_string())
        if num_results > 50:
            # 否则导入到查询结果文件choose_result.csv文件中
            print("...")
            # Export to CSV with UTF-8 BOM encoding
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filter_conditions = f"{history_trading_days}days_{main_board_amplitude_threshold}per_{non_main_board_amplitude_threshold}per_{apply_cond2_or_not}_cond2"
            output_filename = f"stock_query_results_{timestamp}_cond{use_cond_1_1_or_cond_1_2}_{filter_conditions}.csv"
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
