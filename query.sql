-- 📝 计算符合条件的股票交易日窗口
WITH DeduplicatedStockData AS (
    -- ✅ 去掉 stock_data 中完全重复的行
    SELECT DISTINCT stock_code, stock_name, trade_date, open_price, close_price, high_price, low_price, prev_close_price, market_cap, industry_level2, industry_level3, volume FROM stock_data
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
RankedData AS (
    SELECT
        t.stock_code,
        t.trade_date,
        t.stock_name,
        t.volume,
        t.adj_close_price,
        t.adj_high_price,
        t.adj_low_price,
        t.adj_open_price,
        t.industry_level2,
        t.industry_level3,
        row_number() OVER (PARTITION BY t.stock_code ORDER BY t.trade_date) AS rn
    FROM AdjustedStockData t
    WHERE t.stock_code = 'SZ300834' AND t.trade_date >= '2025-06-03'
)
SELECT
    stock_code,
    trade_date,
    stock_name,
    volume,
    adj_close_price AS close,
    adj_high_price AS high,
    adj_low_price AS low,
    adj_open_price AS open,
    industry_level2,
    industry_level3
FROM RankedData
WHERE trade_date = '2025-06-03'
ORDER BY trade_date;
        