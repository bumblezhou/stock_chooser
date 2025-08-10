import backtrader as bt
import pandas as pd
import duckdb
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import configparser
from datetime import datetime

# 盈亏报告的字段映射
PROFIT_LOSS_MAPPING = {
    "stock_code": "股票代码",
    "stock_name": "股票名称", 
    "trade_date": "交易日期",
    "holding_days": "持有天数",
    "profit": "盈亏金额(元)",
    "profit_percent": "盈亏比例"
}

# 字段顺序
PROFIT_LOSS_ORDER = [
    "stock_code", 
    "stock_name", 
    "trade_date", 
    "holding_days", 
    "profit", 
    "profit_percent"
]

# 回测结果
PROFIT_AND_LOSS_SITUATION = []

# 根据选中的突破日股票数据(结构:[{"trade_date": '2025-07-01', "stock_code": "AAPL"},{"trade_date": '2025-08-04', "stock_code": "AAPL"}])
# 获取被选股票突破日后N天的交易数据
def get_next_N_days_data(stock_data_list, holding_day):
    # 连接到DuckDB数据库
    con = duckdb.connect('stock_data.duckdb')
    
    # 初始化一个空的DataFrame，用于存储所有股票的查询结果
    all_results = []
    
    # 遍历所有的突破日股票记录
    for record in stock_data_list:
        stock_code = record['stock_code']
        trade_date = record['trade_date']
        
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
            WHERE t.stock_code = '{stock_code}' AND t.trade_date >= '{trade_date}'
        )
        SELECT
            stock_code,
            trade_date,
            stock_name,
            volume AS vol,
            adj_close_price,
            adj_high_price,
            adj_low_price,
            adj_open_price,
            industry_level2,
            industry_level3
        FROM RankedData
        WHERE rn > (SELECT row_number() OVER (PARTITION BY stock_code ORDER BY trade_date) FROM RankedData WHERE trade_date = '{trade_date}' LIMIT 1)
        AND rn <= (SELECT row_number() OVER (PARTITION BY stock_code ORDER BY trade_date) FROM RankedData WHERE trade_date = '{trade_date}' LIMIT 1) + {holding_day}
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

# 根据选中的突破日股票数据(结构:[{"trade_date": '2025-07-01', "stock_code": "AAPL"},{"trade_date": '2025-08-04', "stock_code": "AAPL"}])
# 获取被选突破日股票的第一支撑位日期和价格
def get_first_support_level(stock_data_list, history_trading_days):
    # 连接到DuckDB数据库
    con = duckdb.connect('stock_data.duckdb')
    
    all_results = []
    
    for record in stock_data_list:
        stock_code = record['stock_code']
        trade_date = record['trade_date']
        adj_close_price = record['adj_close_price']
        
        # SQL查询：从突破日开始查询后20个交易日的数据，并计算复权价格
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
                row_number() OVER (PARTITION BY t.stock_code ORDER BY t.trade_date DESC) AS rn
            FROM AdjustedStockData t
            WHERE t.stock_code = '{stock_code}' AND t.trade_date <= '{trade_date}'
        )
        SELECT
            stock_code,
            trade_date,
            stock_name,
            volume AS vol,
            adj_close_price,
            adj_high_price,
            adj_low_price,
            adj_open_price,
            industry_level2,
            industry_level3
        FROM RankedData
        WHERE rn < {history_trading_days} AND adj_close_price > adj_open_price
        ORDER BY trade_date;
        """

        # 执行查询并获取结果
        df = con.execute(query).fetchdf()
        
        # 计算第一支撑位
        support_level = None
        # 获取突破日的收盘价作为基准价
        base_price = adj_close_price

        # 找到突破日前的次高收盘价作为支撑
        # 这里你原来代码逻辑有点问题，建议取支撑价为小于突破日adj_close_price的最大收盘价
        df_filtered = df[df['adj_close_price'] < base_price]
        if not df_filtered.empty:
            support_row = df_filtered.loc[df_filtered['adj_close_price'].idxmax()]
            support_level = {
                'stock_code': support_row['stock_code'],
                'support_date': support_row['trade_date'],
                'support_price': support_row['adj_close_price']
            }

        # 如果找到了支撑位，则将结果存储
        if support_level:
            all_results.append(support_level)
    
    con.close()  # 关闭数据库连接
    return all_results

# 买入策略
class MyStrategy(bt.Strategy):
    # 设置参数，支持传入突破日和支撑位信息
    params = (
        ('breakout_date', None),            # 突破日
        ('first_support_price', None),      # 第一支撑位价格
        ('first_support_date', None),       # 第一支撑位日期
        ('target_size', 100),               # 目标仓位，单位股
        ('data_name', None),                # 新增参数：关联的数据名称
    )

    def __init__(self):
        # 初始化所需的变量和字段
        self.breakout_date = self.params.breakout_date
        self.first_support_price = self.params.first_support_price
        self.first_support_date = self.params.first_support_date
        self.target_size = self.params.target_size
        self.average_price = 0.00

        self.buy_stage = {}  # 使用字典跟踪每只股票的买入阶段
        self.orders = {}  # 跟踪订单状态

        # 为每只数据初始化买入阶段
        for data in self.datas:
            # 增加一个变量记录买入阶段：0未买入，1买入50%，2买入全部
            self.buy_stage[data._name] = 0
            self.orders[data._name] = None
    
    def notify_order(self, order):
        """订单状态通知"""
        stock_code = order.data._name
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f"买入订单完成：股票 {stock_code}, 数量: {order.executed.size}, " f"价格: {order.executed.price:.2f}, 日期: {self.datas[0].datetime.date(0)}")
            elif order.issell():
                print(f"卖出订单完成：股票 {stock_code}, 数量: {order.executed.size}, " f"价格: {order.executed.price:.2f}, 日期: {self.datas[0].datetime.date(0)}")
            self.orders[stock_code] = None  # 清除订单
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f"订单失败：股票 {stock_code}, 状态: {order.status}")
            self.orders[stock_code] = None

    def next(self):
        for data in self.datas:
            stock_code = data._name
            adj_close_price = data.close[0]
            adj_open_price = data.open[0]
            adj_low_price = data.low[0]
            current_date = data.datetime.date(0)

            # 跳过无效价格
            if not adj_close_price or not adj_open_price or adj_close_price <= 0 or adj_open_price <= 0:
                print(f"股票 {stock_code} 在 {current_date} 的价格无效，跳过")
                continue

            profit = self.broker.get_value() - self.broker.startingcash
            print(f"股票: {stock_code}, 日期: {current_date}, 收盘价: {adj_close_price:.2f}, " f"开盘价: {adj_open_price:.2f}, 持仓: {self.getposition(data).size}, 持仓均价: {self.getposition(data).price:.2f}, 盈亏: {profit:.2f}")

            # 如果有未完成的订单，跳过
            if self.orders.get(stock_code):
                continue

            if self.buy_stage[stock_code] == 0:
                cash_to_use = self.broker.get_cash() * 0.5
                remaining_cash = self.broker.get_cash() - cash_to_use - self.broker.get_fundvalue()
                first_buy_size = int(cash_to_use / adj_open_price)
                if first_buy_size > 0:
                    # self.orders[stock_code] = self.buy(data=data, size=first_buy_size, price=adj_open_price)
                    self.buy_stage[stock_code] = 1
                    print(f"开盘买入50%：股票 {stock_code}, 数量: {first_buy_size}, 价格: {adj_open_price:.2f}")

                second_buy_size = 0
                # 回踩支撑位买入剩余50%
                if self.buy_stage[stock_code] == 1 and adj_low_price <= self.first_support_price:
                    second_buy_size = int(remaining_cash / self.first_support_price)
                    if second_buy_size > 0:
                        # self.orders[stock_code] = self.buy(data=data, size=second_buy_size, price=self.first_support_price)
                        # self.buy_stage[stock_code] = 2
                        print(f"回踩支撑买入剩余50%：股票 {stock_code}, 数量: {second_buy_size}, 价格: {self.first_support_price:.2f}")

                # 按收盘价买入剩余50%
                if self.buy_stage[stock_code] == 1 and adj_low_price > self.first_support_price:
                    second_buy_size = int(remaining_cash / adj_close_price)
                    if second_buy_size > 0:
                        # self.orders[stock_code] = self.buy(data=data, size=second_buy_size, price=adj_close_price)
                        # self.buy_stage[stock_code] = 2
                        print(f"按收盘价买入剩余50%：股票 {stock_code}, 数量: {second_buy_size}, 价格: {adj_close_price:.2f}")
                
                # 因为基本框架不支持一天内使用两个价格进行买入，则干脆使用均价进行一次买来进行模拟
                total_buy_size = first_buy_size + second_buy_size
                self.average_price = ((self.broker.get_cash() - self.broker.get_fundvalue()) / (total_buy_size))
                total_cost = total_buy_size * self.average_price
                print(f"总持仓: {total_buy_size}, 均价: {self.average_price:.2f}, 总成本: {total_cost:.2f}")
                self.orders[stock_code] = self.buy(data=data, size=total_buy_size, price=self.average_price, exectype=bt.Order.Market)
                self.buy_stage[stock_code] = 2

    def stop(self):
        # 计算最终收益
        for data in self.datas:
            profit = self.broker.get_value() - self.broker.startingcash
            profit_per = profit / self.broker.startingcash * 100
            print(f"策略结束：股票 {data._name} 最终组合价值: {self.broker.get_value():.2f}, 利润: {profit:.2f}, 盈利率: {profit_per:.2f}%")

# 获取第一支撑位
def get_support_levels(stock_data_list):
    # 获取每个突破日股票的第一支撑位
    return get_first_support_level(stock_data_list)

# 获取股票数据并转换时间
def convert_trade_date(df):
    # 将 'trade_date' 列从 Unix 时间戳转换为 pandas datetime 类型
    if df['trade_date'].dtype == 'O':
        df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 确保数据按日期升序排序
    df = df.sort_values(by='trade_date')

    return df

# 使用pandas导出回测结果
def export_to_csv_pandas_with_mapping(data_list, filename="profit_loss_report.csv", field_mapping=None, field_order=None):
    """
    使用 pandas 导出 CSV，支持字段映射和自定义顺序
    
    Args:
        data_list: 字典列表数据
        filename: 导出文件名
        field_mapping: 字段映射字典，格式 {"英文字段": "中文表头"}
        field_order: 字段顺序列表，如果不指定则按原顺序
    """
    if not data_list:
        print("数据列表为空，无法导出")
        return False
    
    try:
        # 创建 DataFrame
        df = pd.DataFrame(data_list)
        
        # 如果指定了字段顺序，重新排列列
        if field_order:
            # 只保留存在的字段
            available_fields = [field for field in field_order if field in df.columns]
            df = df[available_fields]
        
        # 如果指定了字段映射，重命名列
        if field_mapping:
            # 只映射存在的字段
            rename_dict = {k: v for k, v in field_mapping.items() if k in df.columns}
            df = df.rename(columns=rename_dict)
        
        # 导出到 CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 回测数据已成功导出到 {filename}")
        # print(f"📊 共导出 {len(data_list)} 条记录，{len(df.columns)} 个字段")
        # print(f"📋 字段列表: {', '.join(df.columns.tolist())}")
        
        # 显示前几行数据预览
        print(f"📖 回测数据预览:")
        print(df.head())
        
        return True
        
    except Exception as e:
        print(f"❌ 导出CSV文件时出错: {e}")
        return False

# 导出盈亏报告的便捷函数
def export_profit_loss_report(data_list, filename=None, add_timestamp=True):
    """导出盈亏报告的便捷函数"""
    if filename is None:
        base_name = "盈亏报告"
    else:
        # 分离文件名和扩展名
        if filename.endswith('.csv'):
            base_name = filename[:-4]
        else:
            base_name = filename
    
    # 添加时间戳
    if add_timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.csv"
    else:
        filename = f"{base_name}.csv"
    
    return export_to_csv_pandas_with_mapping(
        data_list=data_list,
        filename=filename,
        field_mapping=PROFIT_LOSS_MAPPING,
        field_order=PROFIT_LOSS_ORDER
    )

# 定义回测流程，支持多股票批量回测
def run_backtest(stock_data_list, history_trading_days, holding_days):
    global PROFIT_AND_LOSS_SITUATION

    # 运行之前，先清空全局变量
    if len(PROFIT_AND_LOSS_SITUATION) > 0:
        PROFIT_AND_LOSS_SITUATION.clear()
    
    # 先获取所有股票的第一支撑位信息（批量）
    support_levels = get_first_support_level(stock_data_list, history_trading_days)
    # 准备一个字典，方便通过stock_code快速查支撑位
    support_dict = {item['stock_code']: item for item in support_levels}

    # 遍历每个突破日股票，分别加载数据和添加策略
    for record in stock_data_list:
        for holding_day in holding_days:
            cerebro = bt.Cerebro()

            # 设置初始现金
            cerebro.broker.set_cash(100000)
            # 设置佣金
            cerebro.broker.setcommission(commission=0.001)
            # 设置滑点
            cerebro.broker.set_slippage_perc(0.001)

            stock_code = record['stock_code']
            trade_date = record['trade_date']
            stock_name = record['stock_name']

            # 获取突破日后N日行情数据（单只股票）
            df = get_next_N_days_data([record], holding_day)
            if df.empty:
                print(f"股票 {stock_code} 在 {trade_date} 后无数据，跳过")
                continue

            # 转换时间
            df = convert_trade_date(df)
            df.set_index('trade_date', inplace=True)
            df.sort_index(inplace=True)

            # ==== 数据清洗 ====
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.dropna(subset=['adj_close_price', 'adj_open_price', 'vol'], inplace=True)
            df = df[df['vol'] >= 0]

            if df.empty:
                print(f"股票{stock_code}数据清洗后为空，跳过")
                continue

            # 检查数据完整性
            required_columns = ['adj_close_price', 'adj_open_price', 'adj_high_price', 'adj_low_price', 'vol']
            if not all(col in df.columns for col in required_columns):
                print(f"股票 {stock_code} 缺少必要列，跳过")
                continue

            # 自定义Pandas数据类，包含复权价
            class PandasData(bt.feeds.PandasData):
                lines = ('open', 'high', 'low', 'close', 'volume', 'openinterest')
                params = (
                    ('datetime', None),  # 使用索引作为时间
                    ('open', 'adj_open_price'),
                    ('high', 'adj_high_price'),
                    ('low', 'adj_low_price'),
                    ('close', 'adj_close_price'),
                    ('volume', 'vol'),
                    ('openinterest', None),  # 股票数据无未平仓合约，设为None
                )

            # 加载该股票数据
            try:
                data_feed = PandasData(dataname=df)
                # print(f"股票 {stock_code} 数据预览:\n{data_feed._dataname.head()}")
                cerebro.adddata(data_feed, name=stock_code)
            except Exception as e:
                print(f"加载股票 {stock_code} 数据失败: {e}")
                continue

            # 获取该股票对应的支撑位信息
            support_info = support_dict.get(stock_code, {})
            first_support_price = support_info.get('support_price', None)
            first_support_date = support_info.get('support_date', None)

            # 添加策略时传入对应参数
            cerebro.addstrategy(MyStrategy,
                breakout_date=trade_date,
                first_support_price=first_support_price,
                first_support_date=first_support_date,
                target_size=100) # 目标仓位： 100

            print(f"=======================开始对股票{stock_name}模拟在{trade_date}日买入时持有{holding_day}天的回测======================")
            print(f"起始组合价值: {cerebro.broker.startingcash:.2f}\n")
            begin_value = cerebro.broker.startingcash
            cerebro.run()
            end_value = cerebro.broker.getvalue()
            profit = end_value - begin_value
            profit_percent = profit / begin_value * 100
            PROFIT_AND_LOSS_SITUATION.append({
                "stock_code": stock_code,
                "stock_name": stock_name,
                "trade_date": trade_date,
                "holding_days": holding_day,
                "profit": f"{profit:.2f}",
                "profit_percent": f"{profit_percent:.2f}%"
            })
            print(f"结束组合价值: {cerebro.broker.getvalue():.2f}\n")
            print(f"=======================结束对股票{stock_name}模拟在{trade_date}日买入时持有{holding_day}天的回测======================")
            print("\n")

            # 绘图前检查数据
            for data in cerebro.datas:
                df = data._dataname
                if df['vol'].isna().any() or (df['vol'] == np.inf).any():
                    print(f"警告: 股票 {data._name} 的成交量数据无效，跳过绘图")
                    return
            
            # 绘制回测结果
            # cerebro.plot()
    
    export_profit_loss_report(PROFIT_AND_LOSS_SITUATION)


if __name__ == "__main__":

    # 创建 ConfigParser 对象
    config = configparser.ConfigParser()

    # 读取 .conf 文件
    config.read('./config.conf')
    cond1_and_cond3=config['settings']['cond1_and_cond3']                                           # 条件1和条件3的配置项。
    holdingdays_settings=config['settings']['holdingdays']                                          # 持有天数配置
    history_trading_days=cond1_and_cond3.split('_')[0]
    holding_days = [int(x.strip()) for x in holdingdays_settings.split(',')]

    # 读取 CSV 文件，跳过第一行作为列名称
    df = pd.read_csv('stock_query_results_20250809_233124_cond1.1_40days_25per_35per_no_cond2_yes_cond5.csv', header=0)

    # 将 DataFrame 转换为指定格式的列表
    stock_data_list = df[['交易日期', '股票代码', '股票名称', '前复权_收盘价']].to_dict('records')

    # 重命名键为 trade_date 和 stock_code
    for d in stock_data_list:
        d['trade_date'] = d.pop('交易日期')
        d['stock_code'] = d.pop('股票代码')
        d['stock_name'] = d.pop('股票名称')
        d['adj_close_price'] = d.pop('前复权_收盘价')

    # 执行批量回测
    run_backtest(stock_data_list, history_trading_days, holding_days)
