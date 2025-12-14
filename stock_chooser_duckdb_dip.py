import duckdb
import pandas as pd
from packaging import version
from datetime import datetime, timedelta
import time # Import time module for timing
from typing import List, Dict, Union
import configparser

# 定义时间窗口和回踩条件
HISTORY_DAYS = 40  # 支撑价向前看的天数
FUTURE_DAYS = 40   # 回踩日向后看的天数
VOLATILITY_LIMIT = 0.05  # 回踩日波动性限制（C条件）
SUPPORT_PRICE_TOLERANCE = 0.995 # 回踩日最低价要包含支持价的比例（A条件）

# 加载需要做回测运算的xlsx文件
def load_df_from_excel_file(file_path):
    df = None
    try:
        # 读取 Excel 文件的第一个工作表，第一行作为列名
        df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl', header=0)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
    except Exception as e:
        print(f"Error: {str(e)}")
    return df

# 把df中某列的值转换为datetime格式
def convert_date_format_of_df_column(df, column_name="备注"):
    try:
        # 将“备注”列从 yyyyMMdd 转换为 yyyy-MM-dd
        df[column_name] = pd.to_datetime(df[column_name], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        print(f"Error converting dates in column '{column_name}': {str(e)}")
        return df

def load_target_df(excel_file_path: str):
    df = load_df_from_excel_file(excel_file_path)
    convert_date_format_of_df_column(df=df)

    # 复制备注列为breakthrough_date
    df['breakthrough_date'] = df['备注']
    df['stock_code'] = df['代码'].str.lower()
    stock_data_list = (
        df.rename(columns={
            '备注': 'breakthrough_date',
            '代码': 'stock_code',
            '    名称': 'stock_name',
            '现价': 'adj_stock_price'}
        )[['breakthrough_date', 'stock_code', 'stock_name', 'adj_stock_price']].to_dict(orient='records')
    )
    stock_data_df = pd.DataFrame(stock_data_list)
    return stock_data_df

# 从库中找出复权计算过的数据。
def get_next_N_days_data(stock_data_list, max_holding_days):
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
        cond5_sql_where_clause = f''

    # Connect to DuckDB database file
    # Ensure 'stock_data.duckdb' exists and contains data,
    # or uncomment the data generation part below for testing.
    con = duckdb.connect(database='stock_data.duckdb', read_only=False)
    print("连接到数据库: stock_data.duckdb")
    
    stock_code_list = ", ".join(f"'{item['stock_code']}'" for item in stock_data_list)
    days_limit = 41 if max_holding_days is None else (int(max_holding_days) + 1)

    # Main Query SQL (optimized for DuckDB)
    # The SQL is mostly the same as DuckDB handles window functions efficiently.
    query_sql = f"""
    -- 📝 计算符合条件的股票交易日窗口
    WITH DeduplicatedStockData AS (
        -- ✅ 去掉 stock_data 中完全重复的行
        SELECT DISTINCT stock_code, stock_name, trade_date, open_price, close_price, high_price, low_price, prev_close_price, market_cap, total_market_cap, industry_level1, industry_level2, industry_level3 
        FROM stock_data
        -- 🔧 限定 stock_code 范围，只查询给定股票列表
        WHERE stock_code IN (
            -- ⚠️ 这里的 stock_code_list 可以是 Python 格式 ['AAPL','TSM'] 转换成 SQL 字符串 'AAPL','TSM'
            {stock_code_list}
        )
    ),
    StockWithRiseFall AS (
        -- ✅ 计算复权涨跌幅，公式: 复权涨跌幅 = 收盘价 / 前收盘价 - 1
        SELECT *,
            (close_price / NULLIF(prev_close_price, 0)) - 1 AS rise_fall
        FROM DeduplicatedStockData
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
            t.stock_code,
            t.close_price AS last_close_price,
            t.adjustment_factor AS last_adjustment_factor
        FROM (
            SELECT 
                stock_code,
                close_price,
                adjustment_factor,
                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date DESC) AS rn
            FROM AdjustmentFactorComputed
        ) t
        WHERE t.rn = 1
    ),
    AdjustedStockData AS (
        SELECT 
            a.*,
            -- ✅ 计算前复权收盘价, 公式: 前复权收盘价 = 复权因子 * (最后一条数据的收盘价 / 最后一条数据的复权因子)
            a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0)) AS adj_close_price,
            -- ✅ 前复权其他价格
            (a.open_price / NULLIF(a.close_price, 0)) * (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_open_price,
            (a.high_price / NULLIF(a.close_price, 0)) * (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_high_price,
            (a.low_price / NULLIF(a.close_price, 0)) * (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_low_price,
            (a.prev_close_price / NULLIF(a.close_price, 0)) * (a.adjustment_factor * (l.last_close_price / NULLIF(l.last_adjustment_factor, 0))) AS adj_prev_close_price
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
            t.industry_level1,
            t.industry_level2,
            t.industry_level3,
            -- ✅ 流通市值换算成“亿”
            (t.market_cap / 100000000) AS market_cap_of_100_million,
            -- 
            (t.total_market_cap / 100000000) AS total_market_cap_of_100_million,
            -- ✅ N个交易日内（不含当日）的最高收盘价, 使用的是复权后的收盘价
            MAX(t.adj_close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_close_n_days,
            -- ✅ 对应的最高收盘价日期
            arg_max(t.trade_date, t.adj_close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN {history_trading_days} PRECEDING AND 1 PRECEDING
            ) AS max_close_n_days_date,
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
            -- ✅ 排除2022年1月1号之前的交易数据
            t.trade_date >= '{earliest_time_limit}'
    ),
    FilteredStockData AS (
        SELECT
            sw.stock_code,
            sw.stock_name,
            sw.trade_date,
            sw.adj_close_price,
            sw.adj_high_price,
            sw.adj_low_price,
            sw.adj_open_price,
            sw.max_close_n_days,
            sw.max_close_n_days_date,
            sw.market_cap_of_100_million,
            sw.total_market_cap_of_100_million,
            sw.industry_level1,
            sw.industry_level2,
            sw.industry_level3
        FROM
            StockWindows AS sw
        WHERE
            -- 📌 条件0：窗口内至少有N个交易日数据
            sw.rn > {history_trading_days}
            -- 📌 条件1：当日收盘价大于前N个交易日的最高收盘价的101%
            AND sw.adj_close_price > (sw.max_close_n_days * 1.01)
            -- 📌 条件2：前N个交易日内有涨幅（大于等于5%）的K线
            {cond2_sql_where_clause}
            -- 📌 条件3：前N个交易日的股票价格振幅度，上证和深证股票小于等于25%(30%, 35%)，创业板和科创板股票小于等于35%(40%, 40%)
            AND (
                -- ✅ 根据股票代码板块（前缀）确定振幅阈值
                CASE
                    WHEN sw.open_price_of_first_day_of_n_days > 0
                    THEN (sw.max_high_n_days - sw.min_low_n_days) * 1.0 / sw.open_price_of_first_day_of_n_days * 100
                    ELSE 999999 -- 避免除零错误
                END
            ) <= (
                CASE
                    -- ✅ 创业板（以300，301，302开头）或科创板（以688开头），小于等于35%(40%, 40%)
                    WHEN sw.stock_code LIKE 'sz300%' OR sw.stock_code LIKE 'sz301%' OR sw.stock_code LIKE 'sz302%' OR sw.stock_code LIKE 'sh688%' THEN {non_main_board_amplitude_threshold}
                    -- ✅ 上证主板（以600，601，603，605开头）小于等于25%(30%, 35%)
                    WHEN sw.stock_code LIKE 'sh600%' OR sw.stock_code LIKE 'sh601%' OR sw.stock_code LIKE 'sh603%' OR sw.stock_code LIKE 'sh605%' THEN {main_board_amplitude_threshold}
                    -- ✅ 深证主板（以000，001，002，003开头）小于等于25%(30%, 35%)
                    WHEN sw.stock_code LIKE 'sz000%' OR sw.stock_code LIKE 'sz001%' OR sw.stock_code LIKE 'sz002%' OR sw.stock_code LIKE 'sz003%' THEN {main_board_amplitude_threshold}
                    ELSE 1000
                END
            )
            -- 📌 条件4：流通市值在30亿至500亿之间
            AND sw.market_cap_of_100_million BETWEEN {min_market_capitalization} AND {max_market_capitalization}
    ),
    LimitedRangeStockData AS (
        -- 🔧 限定范围：每支股票从其 max_close_n_days_date 起，往后取 {days_limit} 个交易日数据
        SELECT *
        FROM StockWindows w
        WHERE EXISTS (
            SELECT 1
            FROM FilteredStockData f
            WHERE f.stock_code = w.stock_code
            AND w.trade_date BETWEEN f.max_close_n_days_date AND DATE_ADD(f.max_close_n_days_date, INTERVAL {days_limit} DAY)
        )
    )
    -- ✅ 最终输出
    SELECT
        stock_code,
        stock_name,
        trade_date,
        max_close_n_days_date AS adj_support_date,
        ROUND(max_close_n_days, 2) AS adj_support_price,
        ROUND(adj_close_price, 2) AS adj_close_price,
        ROUND(adj_high_price, 2) AS adj_high_price,
        ROUND(adj_low_price, 2) AS adj_low_price,
        ROUND(adj_open_price, 2) AS adj_open_price,
        industry_level2,
        industry_level3
    FROM LimitedRangeStockData
    ORDER BY stock_code, trade_date;
    """
    
    # 获取查询结果
    results_df = con.execute(query_sql).fetchdf()
    
    # 关闭连接
    con.close()

    #返回查询结果
    return results_df

def find_support_and_dip_dates(
    limited_adjusted_df: pd.DataFrame, 
    targets: List[Dict[str, str]]
) -> pd.DataFrame:
    """
    根据 DuckDB 预处理的 limited_adjusted_df，查找回踩日。
    
    【修改内容】
    1. 忽略突破日后的第一个交易日作为回踩备选。
    2. 收集所有符合条件的回踩日。
    """
    results = []
    
    # 确保 trade_date 是日期类型
    limited_adjusted_df['trade_date'] = pd.to_datetime(limited_adjusted_df['trade_date'])

    for target in targets:
        stock_code = target['stock_code']
        breakthrough_date_str = target['breakthrough_date']
        stock_name = target['stock_name']
        
        try:
            breakthrough_date_dt = pd.to_datetime(breakthrough_date_str)
        except ValueError:
            print(f"Skipping {stock_code}: Invalid breakthrough_date format.")
            continue

        # 1. 筛选目标股票数据
        stock_df = limited_adjusted_df[limited_adjusted_df['stock_code'] == stock_code].sort_values('trade_date').reset_index(drop=True)
        
        # 2. 确定突破日和支撑价
        breakthrough_row = stock_df[stock_df['trade_date'] == breakthrough_date_dt]
        
        if breakthrough_row.empty:
            continue
        
        # 获取支撑价和支撑日期
        support_price = breakthrough_row['adj_support_price'].iloc[0]
        support_date_dt = breakthrough_row['adj_support_date'].iloc[0] # 注意：这里使用 adj_support_date
        
        if support_price is None or support_price == 0:
            continue

        # 3. 确定回踩窗口 (Dip Window)
        
        # 突破日位置
        breakthrough_pos = stock_df.index.get_loc(breakthrough_row.index[0])
        
        # 【修改点 1：忽略突破日后的第一个交易日】
        # 回踩窗口从突破日后的第二个交易日开始
        # dip_start_pos 原为 breakthrough_pos + 1
        dip_start_pos = breakthrough_pos + 2 
        
        # 提取回踩窗口的数据：从突破日后第二天到数据结束
        # 确保 dip_start_pos 不会超出数据范围
        if dip_start_pos >= len(stock_df):
             # 没有足够的数据来继续查找回踩，跳过当前股票
             continue
             
        dip_window_df = stock_df.iloc[dip_start_pos:].copy()
        
        
        # 4. 寻找所有回踩日 (Dip Dates)
        dip_dates = [] 
        
        if not dip_window_df.empty:
            # 找到所有符合条件的备选回踩日（逻辑不变）
            # A. 备选回踩日当天的最高价(adj_high_price)和最低价(adj_low_price)*99.5%要包含支持价
            condition_A = (dip_window_df['adj_high_price'] >= support_price) & \
                          (dip_window_df['adj_low_price'] * SUPPORT_PRICE_TOLERANCE <= support_price)
            
            # B. 备选回踩日当天的收盘价(adj_close_price)高于支持价(support_price)
            condition_B = dip_window_df['adj_close_price'] > support_price
            
            # C. 备选回踩日当天的波动性小于 VOLATILITY_LIMIT
            # condition_C = (abs(dip_window_df['adj_close_price'] - dip_window_df['adj_open_price']) / dip_window_df['adj_open_price']) < VOLATILITY_LIMIT
            
            # candidate_dips = dip_window_df[condition_A & condition_B & condition_C]

            candidate_dips = dip_window_df[condition_A & condition_B]
            
            
            if not candidate_dips.empty:
                # 【修改点 2：收集所有回踩日】
                # 将所有符合条件的日期转换为 YYYY-MM-DD 字符串，并收集到一个列表中
                dip_dates = [dt.strftime('%Y-%m-%d') for dt in candidate_dips['trade_date'].tolist()]

        # 5. 记录结果
        # 【修改点 2：将多个回踩日连接成一个字符串】
        # dip_date_str = ", ".join(dip_dates) if dip_dates else None
        
        # =============== 【新增过滤逻辑】 ===============
        if not dip_dates:
            # 如果 dip_dates 列表为空，则跳过本次循环，不将结果添加到 results
            continue
        # ===============================================

        for dip_date in dip_dates:
            results.append({
                'stock_code': stock_code,
                'stock_name': stock_name,
                # 确保突破日和支撑日格式统一为 YYYY-MM-DD 字符串
                'breakthrough_date': breakthrough_date_dt.strftime('%Y-%m-%d'),
                'support_price': support_price,
                'support_date': support_date_dt.strftime('%Y-%m-%d'),
                'dip_date': dip_date
            })

    return pd.DataFrame(results)


if __name__ == '__main__':
    # 获取数据
    target_df = load_target_df("Table.xlsx")
    target_df['breakthrough_date'] = pd.to_datetime(target_df['breakthrough_date'])
    stock_data_list = target_df[['breakthrough_date', 'stock_code', 'stock_name']].to_dict('records')

    print("\n执行筛选...")
    start_time = time.time()

    # 2. 计算前复权数据
    MAX_HOLDING_DAYS = 40
    limited_df = get_next_N_days_data(stock_data_list, MAX_HOLDING_DAYS)

    # 3. 查找支撑价和回踩日
    final_results = find_support_and_dip_dates(limited_df, stock_data_list)
    
    # 4. 输出结果
    # print("\n--- 最终结果 ---")
    # print(final_results[['stock_code', 'stock_name', 'breakthrough_date', 'support_price', 'dip_date']].to_markdown(index=False))

    end_time = time.time()
    print(f"筛选于: {end_time - start_time:.2f}秒内完成.")

    # 5. 导出到 Excel 文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file_name = f'回踩筛选结果_{timestamp}.xlsx'
    
    # 导出时，只保留您要求的四列（加上支撑价方便检查）
    columns_to_export = [
        'stock_code', 
        'stock_name', 
        'breakthrough_date', 
        'dip_date',
        'support_date',
        'support_price' # 导出支撑价方便查看
    ]
    
    # 使用 to_excel 方法导出
    final_results[columns_to_export].to_excel(
        excel_file_name, 
        index=False # 不导出 pandas 的行索引
    )
    
    print(f"\n✅ 结果已成功导出到文件: {excel_file_name}")