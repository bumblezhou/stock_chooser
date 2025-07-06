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
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS max_close_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最高价（用于振幅计算）
            MAX(t.high_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS max_high_n_days,
            -- ✅ N个交易日窗口内（不含当日）的最低价（用于振幅计算）
            MIN(t.low_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS min_low_n_days,
            -- ✅ N个交易日内（不含当日）的最低收盘价，用作振幅分母
            MIN(t.close_price) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS min_close_n_days_for_amplitude_base,
            -- ✅ N个交易日内是否存在单日涨幅 ≥ 5%
            MAX(CASE
                WHEN (t.close_price - t.prev_close_price) / NULLIF(t.prev_close_price, 0) >= 0.05 THEN 1
                ELSE 0
            END) OVER (
                PARTITION BY t.stock_code
                ORDER BY t.trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
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
            AND t.trade_date > '2022-01-01 00:00:00'
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
            rn > 40
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
                    WHEN stock_code SIMILAR TO '(sz300|sz301|sz302|sh688)%' THEN 35
                    -- ✅ 上证主板（以600，601，603，605开头）小于等于25%(30%, 35%)
                    WHEN stock_code SIMILAR TO '(sh600|sh601|sh603|sh605)%' THEN 25
                    -- ✅ 深证主板（以000，001，002，003开头）小于等于25%(30%, 35%)
                    WHEN stock_code SIMILAR TO '(sz000|sz001|sz002|sz003)%' THEN 25
                    ELSE 1000
                END
            )
            -- 📌 条件4：流通市值在30亿至500亿之间
            AND market_cap_of_100_million BETWEEN 30 AND 500
            -- 📌 条件5：最近一个财报周期净利润同比增长率和营业总收入同比增长率大于等于-20%
            AND net_profit_yoy >= -0.2
            AND revenue_yoy >= -0.2
    )
    -- FinalResult AS (
    --     SELECT *
    --     FROM BlockWithFilteredRows
    --     WHERE
    --         -- ✅ 限制连续交易日 <= 20
    --         block_size <= 20
    --         -- 📌 条件1.1: 次高收盘价为前一个交易日收盘价的不作为筛选结果
    --         -- AND NOT (
    --         --     row_in_block < block_size AND distinct_close_count = 1
    --         -- )
    --         -- 📌 条件1.2: 每个区块只保留最早的一条记录
    --         -- AND row_in_block = 1
    -- )
    SELECT
        stock_code,
        stock_name,
        trade_date,
        industry_level2,
        industry_level3
    FROM FilteredRawData
    WHERE stock_name = '招商南油'
    ORDER BY stock_code, trade_date;