import duckdb
import pandas as pd
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.styles import Alignment, Font

# ========== 参数配置 ==========
MAX_HOLDING_TRADING_DAYS = 40   # 最大持有天数40天
INITIAL_CASH = 100000         # 每股初始购买金额10万元
# ========== 参数配置 ==========

# 盈亏报告的字段映射
PROFIT_LOSS_MAPPING = {
    "no": "编号",
    "stock_code": "股票代码",
    "stock_name": "股票名称", 
    "bought_date": "购入日期",
    "init_cash": "初始金额",
    "trade_date": "结算日期",
    "holding_days": "持有天数",
    "max_holding_days": "最大持有天数",
    "market_value": "市值(元)",
    "profit": "盈亏金额(元)",
    "profit_percent": "盈亏比"
}

# 字段顺序
PROFIT_LOSS_ORDER = [
    "no",
    "stock_code", 
    "stock_name", 
    "bought_date", 
    "init_cash",
    "holding_days",
    "max_holding_days", 
    "trade_date",
    "market_value",
    "profit", 
    "profit_percent"
]


# 加载需要做回测运算的xslx文件，【注意：先把xls文件转换居xlsx格式！】
def load_df_from_excel_file(file_path):
    df = None
    try:
        # 读取 Excel 文件的第一个工作表，第一行作为列名
        df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl', header=0)
        
        # # 打印工作表名称
        # print(f"Sheet name: {pd.ExcelFile(file_path).sheet_names[0]}")
        
        # # 打印列名
        # print(f"Column names: {df.columns.tolist()}")
        
        # # 打印整个工作表内容
        # print("\nDataFrame content:")
        # print(df)
        
        # # 逐行打印数据（不包含列名）
        # print("\nRow-by-row content:")
        # for index, row in df.iterrows():
        #     print(row.tolist())
            
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

def load_target_df():
    # 读取 CSV 文件，跳过第一行作为列名称
    # df = pd.read_csv('stock_query_results_20250813_122711_cond1.1_40days_25per_35per_no_cond2_yes_cond5.csv', header=0)
    df = load_df_from_excel_file("1009all.xlsx")
    convert_date_format_of_df_column(df=df)

    # # 将 DataFrame 转换为指定格式的列表
    # stock_data_list = df[['交易日期', '股票代码', '股票名称', '前复权_收盘价', '前复权_前N天最高收盘价']].to_dict('records')

    # 复制备注列为breakthrough_date
    df['breakthrough_date'] = df['备注']
    df['stock_code'] = df['代码'].str.lower()
    stock_data_list = (
        df.rename(columns={
            '备注': 'trade_date',
            '代码': 'stock_code',
            '    名称': 'stock_name',
            '现价': 'adj_stock_price',
            '支撑价': 'adj_support_price'}
        )[['trade_date', 'breakthrough_date', 'stock_code', 'stock_name', 'adj_stock_price', 'adj_support_price']].to_dict(orient='records')
    )

    # for item in stock_data_list:
    #     print(item)

    stock_data_df = pd.DataFrame(stock_data_list)
    return stock_data_df

# 根据选中的突破日股票数据(结构:[{"breakthrough_date": '2025-07-01', "stock_code": "AAPL", "adj_support_price": 25.3},{"breakthrough_date": '2025-08-04', "stock_code": "AAPL", "adj_support_price": 26.2}])
# 获取被选股票突破日后N天的交易数据
def get_next_N_days_data(stock_data_list, holding_day):
    # 连接到DuckDB数据库
    con = duckdb.connect('stock_data.duckdb')
    
    # 初始化一个空的DataFrame，用于存储所有股票的查询结果
    all_results = []
    
    # 遍历所有的突破日股票记录
    for record in stock_data_list:
        stock_code = record['stock_code']
        breakthrough_date = record['breakthrough_date']
        adj_support_price = record['adj_support_price']
        
        # SQL查询：从突破日开始查询后20个交易日的数据
        query = f"""
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
            WHERE t.stock_code = '{stock_code}' AND t.trade_date >= '{breakthrough_date}'
        )
        SELECT
            stock_code,
            stock_name,
            trade_date,
            '{breakthrough_date}' AS breakthrough_date,
            '{adj_support_price}' AS adj_support_price,
            volume,
            adj_close_price AS close,
            adj_high_price AS high,
            adj_low_price AS low,
            adj_open_price AS open,
            industry_level2,
            industry_level3
        FROM RankedData
        WHERE rn <= (SELECT row_number() OVER (PARTITION BY stock_code ORDER BY trade_date) FROM RankedData WHERE trade_date = '{breakthrough_date}' LIMIT 1) + {holding_day} + 1
        ORDER BY trade_date;
        """

        # 执行查询并获取结果
        df = con.execute(query).fetchdf()
        
        # 将查询结果添加到 all_results 列表中
        all_results.append(df)
    
    # 将所有结果合并成一个大的DataFrame
    final_df = pd.concat(all_results, ignore_index=True)
    con.close()  # 关闭数据库连接
    return final_df

def do_back_test():
    # Fetch data
    target_df = load_target_df()
    stock_data_list = target_df[['trade_date', 'breakthrough_date', 'stock_code', 'stock_name', 'adj_stock_price', 'adj_support_price']].to_dict('records')
    stock_df = get_next_N_days_data(stock_data_list, MAX_HOLDING_TRADING_DAYS)

    # Convert dates to datetime
    target_df['breakthrough_date'] = pd.to_datetime(target_df['breakthrough_date'])
    stock_df['trade_date'] = pd.to_datetime(stock_df['trade_date'])

    # Sort stock_data
    stock_df = stock_df.sort_values(['stock_code', 'trade_date'])

    # Join to get support price and breakthrough date into stock_df if needed, but process per stock

    results = []
    stock_to_remaining = {}

    for idx, target in target_df.iterrows():
        stock_code = target['stock_code']
        stock_name = target['stock_name']
        support_price = target['adj_support_price']
        breakthrough_date = target['breakthrough_date']
        
        group = stock_df[stock_df['stock_code'] == stock_code].reset_index(drop=True)
        if group.empty:
            continue
        
        # Find the next trading day after breakthrough_date
        next_days = group[group['trade_date'] > breakthrough_date]
        if next_days.empty:
            continue
        bought_date = next_days['trade_date'].iloc[0]
        bought_idx = group[group['trade_date'] == bought_date].index[0]
        
        # Buy: 50% at open, 50% at close, in multiples of 100 shares
        initial_cash = INITIAL_CASH
        cash_morning = initial_cash * 0.5
        cash_evening = initial_cash * 0.5
        
        shares_morning = ((cash_morning / group.loc[bought_idx, 'open']) // 100) * 100
        cost_morning = shares_morning * group.loc[bought_idx, 'open']
        remaining_morning = cash_morning - cost_morning
        
        shares_evening = ((cash_evening / group.loc[bought_idx, 'close']) // 100) * 100
        cost_evening = shares_evening * group.loc[bought_idx, 'close']
        remaining_evening = cash_evening - cost_evening
        
        total_shares = shares_morning + shares_evening
        if total_shares == 0:
            continue  # No shares bought, skip
        
        total_cost = cost_morning + cost_evening
        remaining_cash = remaining_morning + remaining_evening
        cost_price = total_cost / total_shares
        
        stock_to_remaining[stock_code] = remaining_cash
        
        # Initial position
        current_position = total_shares
        stop_loss = cost_price * 0.95
        half_sold = False
        recover_count = 0
        holding_start = bought_date
        holding = True
        
        # Track for dynamic strategy
        max_rise = 1.0  # Current max rise achieved
        rise_break_date = None
        rise_count = 0
        
        for i in range(bought_idx, len(group)):
            current_date = group.loc[i, 'trade_date']
            current_open = group.loc[i, 'open']
            current_high = group.loc[i, 'high']
            current_low = group.loc[i, 'low']
            current_close = group.loc[i, 'close']
            
            if not holding:
                break
            
            holding_days = (current_date - holding_start).days
            
            # Check max holding calendar days
            if holding_days > 40:
                if i > bought_idx:
                    # Sell at previous day's close
                    prev_i = i - 1
                    sell_price = group.loc[prev_i, 'close']
                    current_date = group.loc[prev_i, 'trade_date']
                    holding_days = (current_date - holding_start).days
                    market_value = current_position * sell_price
                    profit = market_value - (current_position * cost_price)
                    profit_percent = (sell_price / cost_price - 1) * 100
                    results.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'init_cash': initial_cash,
                        'bought_date': bought_date,
                        'cost_price': cost_price,
                        'trade_date': current_date,
                        'trade_positions': current_position,
                        'holding_days': holding_days,
                        'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                        'market_value': market_value,
                        'profit': profit,
                        'profit_percent': profit_percent
                    })
                    holding = False
                    continue
            
            # Check stop loss (immediate clear if low < stop_loss)
            if current_low < stop_loss:
                sell_price = min(current_open, stop_loss)  # Assume sell at stop_loss or open if lower
                market_value = current_position * sell_price
                profit = market_value - (current_position * cost_price)
                profit_percent = (sell_price / cost_price - 1) * 100
                results.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'init_cash': initial_cash,
                    'bought_date': bought_date,
                    'cost_price': cost_price,
                    'trade_date': current_date,
                    'trade_positions': current_position,
                    'holding_days': holding_days,
                    'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                    'market_value': market_value,
                    'profit': profit,
                    'profit_percent': profit_percent
                })
                holding = False
                continue
            
            # Check support line break
            if current_low < support_price:
                if current_close >= support_price:
                    recover_count = 0
                else:
                    recover_count += 1
                if recover_count >= 3:
                    sell_price = current_close
                    market_value = current_position * sell_price
                    profit = market_value - (current_position * cost_price)
                    profit_percent = (sell_price / cost_price - 1) * 100
                    results.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'init_cash': initial_cash,
                        'bought_date': bought_date,
                        'cost_price': cost_price,
                        'trade_date': current_date,
                        'trade_positions': current_position,
                        'holding_days': holding_days,
                        'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                        'market_value': market_value,
                        'profit': profit,
                        'profit_percent': profit_percent
                    })
                    holding = False
                    continue
            else:
                recover_count = 0
            
            # Current rise
            current_rise = current_close / cost_price
            
            # Sell 50% at 10% rise
            if not half_sold and current_rise >= 1.10:
                sell_position = current_position * 0.5
                current_position -= sell_position
                sell_price = current_close
                market_value = sell_position * sell_price
                profit = market_value - (sell_position * cost_price)
                profit_percent = (sell_price / cost_price - 1) * 100
                results.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'init_cash': initial_cash,
                    'bought_date': bought_date,
                    'cost_price': cost_price,
                    'trade_date': current_date,
                    'trade_positions': sell_position,
                    'holding_days': holding_days,
                    'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                    'market_value': market_value,
                    'profit': profit,
                    'profit_percent': profit_percent
                })
                half_sold = True
                stop_loss = cost_price * 1.02
                max_rise = 1.10
            
            if half_sold:
                # Update max_rise
                if current_rise > max_rise:
                    if max_rise < 1.10 and current_rise >= 1.10:
                        rise_break_date = current_date
                        rise_count = 0
                        max_rise = 1.10
                        stop_loss = cost_price * 1.10
                    elif max_rise < 1.20 and current_rise >= 1.20:
                        rise_break_date = current_date
                        rise_count = 0
                        max_rise = 1.20
                        stop_loss = cost_price * 1.20
                    elif max_rise < 1.30 and current_rise >= 1.30:
                        rise_break_date = current_date
                        rise_count = 0
                        max_rise = 1.30
                        stop_loss = cost_price * 1.30
                    # For 200%
                    if current_rise >= 2.00:
                        sell_price = current_close
                        market_value = current_position * sell_price
                        profit = market_value - (current_position * cost_price)
                        profit_percent = (sell_price / cost_price - 1) * 100
                        results.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'init_cash': initial_cash,
                            'bought_date': bought_date,
                            'cost_price': cost_price,
                            'trade_date': current_date,
                            'trade_positions': current_position,
                            'holding_days': holding_days,
                            'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                            'market_value': market_value,
                            'profit': profit,
                            'profit_percent': profit_percent
                        })
                        holding = False
                        continue
                
                # Count days after break
                if rise_break_date is not None:
                    rise_count = (current_date - rise_break_date).days
                    
                    # Check not exceed next level in 5 days
                    next_level = max_rise + 0.10 if max_rise < 1.30 else 1.40
                    if rise_count >= 5 and current_rise < next_level:
                        sell_price = current_close
                        market_value = current_position * sell_price
                        profit = market_value - (current_position * cost_price)
                        profit_percent = (sell_price / cost_price - 1) * 100
                        results.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'init_cash': initial_cash,
                            'bought_date': bought_date,
                            'cost_price': cost_price,
                            'trade_date': current_date,
                            'trade_positions': current_position,
                            'holding_days': holding_days,
                            'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                            'market_value': market_value,
                            'profit': profit,
                            'profit_percent': profit_percent
                        })
                        holding = False
                        continue
                
                # Callback sell
                if current_low < stop_loss:
                    sell_price = stop_loss
                    market_value = current_position * sell_price
                    profit = market_value - (current_position * cost_price)
                    profit_percent = (sell_price / cost_price - 1) * 100
                    results.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'init_cash': initial_cash,
                        'bought_date': bought_date,
                        'cost_price': cost_price,
                        'trade_date': current_date,
                        'trade_positions': current_position,
                        'holding_days': holding_days,
                        'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                        'market_value': market_value,
                        'profit': profit,
                        'profit_percent': profit_percent
                    })
                    holding = False
                    continue
                
                # For 130% and above, additional 20 days hold sell
                if max_rise >= 1.30 and holding_days >= 20:
                    sell_price = current_close
                    market_value = current_position * sell_price
                    profit = market_value - (current_position * cost_price)
                    profit_percent = (sell_price / cost_price - 1) * 100
                    results.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'init_cash': initial_cash,
                        'bought_date': bought_date,
                        'cost_price': cost_price,
                        'trade_date': current_date,
                        'trade_positions': current_position,
                        'holding_days': holding_days,
                        'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                        'market_value': market_value,
                        'profit': profit,
                        'profit_percent': profit_percent
                    })
                    holding = False
                    continue
            
            # If last day and still holding, sell
            if i == len(group) - 1 and holding:
                sell_price = current_close
                market_value = current_position * sell_price
                profit = market_value - (current_position * cost_price)
                profit_percent = (sell_price / cost_price - 1) * 100
                results.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'init_cash': initial_cash,
                    'bought_date': bought_date,
                    'cost_price': cost_price,
                    'trade_date': current_date,
                    'trade_positions': current_position,
                    'holding_days': holding_days,
                    'max_holding_days': MAX_HOLDING_TRADING_DAYS,
                    'market_value': market_value,
                    'profit': profit,
                    'profit_percent': profit_percent
                })
                holding = False

    # Create DataFrame
    final_df = pd.DataFrame(results)

    # Group by stock_code and stock_name to merge
    merged_df = final_df.groupby(['stock_code', 'stock_name']).agg(
        bought_date=('bought_date', 'min'),
        init_cash=('init_cash', 'max'),
        cost_price=('cost_price', 'max'),
        trade_date=('trade_date', 'max'),
        trade_positions=('trade_positions', 'sum'),
        holding_days=('holding_days', 'max'),
        max_holding_days=('max_holding_days', 'max'),
        market_value=('market_value', 'sum'),
        profit=('profit', 'sum')
    ).reset_index()

    # Add remaining cash to market_value
    for idx, row in merged_df.iterrows():
        stock_code = row['stock_code']
        remaining = stock_to_remaining.get(stock_code, 0)
        merged_df.at[idx, 'market_value'] += remaining

    # Recalculate profit and profit_percent
    merged_df['profit'] = merged_df['market_value'] - merged_df['init_cash']
    merged_df['profit_percent'] = ((merged_df['market_value'] - merged_df['init_cash']) / merged_df['init_cash'] * 100).round(2)

    # Add no field
    merged_df['no'] = range(1, len(merged_df) + 1)

    # Reorder columns
    merged_df = merged_df[['no', 'stock_code', 'stock_name', 'init_cash', 'bought_date', 'cost_price', 'trade_date', 'trade_positions', 'holding_days', 'max_holding_days', 'market_value', 'profit', 'profit_percent']]

    # Add total row
    total_init_cash = merged_df['init_cash'].sum()
    total_market_value = merged_df['market_value'].sum()
    total_profit_percent = ((total_market_value - total_init_cash) / total_init_cash * 100).round(2)
    total_row = pd.DataFrame({
        'no': ['总计'],
        'stock_code': ['总计'],
        'stock_name': ['总计'],
        'init_cash': [total_init_cash],
        'bought_date': [None],
        'cost_price': [None],
        'trade_date': [None],
        'trade_positions': [None],
        'holding_days': [None],
        'max_holding_days': [None],
        'market_value': [total_market_value],
        'profit': [total_market_value - total_init_cash],
        'profit_percent': [total_profit_percent]
    })

    # Append total row
    final_export_df = pd.concat([merged_df, total_row], ignore_index=True)

    # Apply conditional formatting for profit_percent > 0 red background, else green
    # For Excel, use openpyxl to set colors
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill
    from openpyxl.utils.dataframe import dataframe_to_rows

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"组合盈亏报告_{timestamp}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "组合盈亏报告"

    # Write dataframe to excel
    for r in dataframe_to_rows(final_export_df, index=False, header=True):
        ws.append(r)

    # Apply formatting to rows 2 to len(merged_df)+1 (excluding header and total)
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    green_fill = PatternFill(start_color="00FF00", end_color="00FF00", fill_type="solid")

    for row in range(2, len(merged_df) + 2):  # Rows 2 to n+1 for stocks
        profit_pct = final_export_df.loc[row-2, 'profit_percent']
        if profit_pct > 0:
            for col in range(1, len(final_export_df.columns) + 1):
                ws.cell(row=row, column=col).fill = red_fill
        else:
            for col in range(1, len(final_export_df.columns) + 1):
                ws.cell(row=row, column=col).fill = green_fill

    # Set number formats
    col_map = {col: idx+1 for idx, col in enumerate(final_export_df.columns)}
    for row in range(2, ws.max_row + 1):
        # Dates
        for date_col in ['bought_date', 'trade_date']:
            if date_col in col_map:
                cell = ws.cell(row, col_map[date_col])
                if cell.value is not None:
                    cell.number_format = 'yyyy-mm-dd'
        
        # Floats to 2 decimal places
        for float_col in ['cost_price', 'market_value', 'profit']:
            if float_col in col_map:
                cell = ws.cell(row, col_map[float_col])
                if cell.value is not None:
                    cell.number_format = '0.00'
        
        # Profit percent
        if 'profit_percent' in col_map:
            cell = ws.cell(row, col_map['profit_percent'])
            if cell.value is not None:
                cell.number_format = '0.00%'
        
        # Integers
        for int_col in ['init_cash', 'trade_positions', 'holding_days', 'max_holding_days']:
            if int_col in col_map:
                cell = ws.cell(row, col_map[int_col])
                if cell.value is not None:
                    cell.number_format = '0'

    wb.save(filename)

if __name__ == '__main__':
    do_back_test()