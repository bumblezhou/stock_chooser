import duckdb
import csv
import os
import pandas as pd
import time # Import time for performance measurement

def convert_and_read_csv(file_path):
    """
    Reads a CSV file, skipping the first row and using the second row as headers.
    It attempts to read with UTF-8 encoding first, then falls back to GB2312.
    It processes data types and handles missing values.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        list: A list of dictionaries, where each dictionary represents a row of data.
    """
    data = []
    # Define the mapping from CSV header names to desired column names and types
    # This mapping ensures consistent column names for the DuckDB table
    header_mapping = {
        '股票代码': 'stock_code',
        '股票名称': 'stock_name',
        '交易日期': 'trade_date',
        '开盘价': 'open_price',
        '最高价': 'high_price',
        '最低价': 'low_price',
        '收盘价': 'close_price',
        '前收盘价': 'prev_close_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '流通市值': 'market_cap',
        '总市值': 'total_market_cap',
        '净利润TTM': 'net_profit_ttm',
        '现金流TTM': 'cash_flow_ttm',
        '净资产': 'net_assets',
        '总资产': 'total_assets',
        '总负债': 'total_liabilities',
        '净利润(当季)': 'net_profit_quarter',
        '中户资金买入额': 'mid_investor_buy',
        '中户资金卖出额': 'mid_investor_sell',
        '大户资金买入额': 'large_investor_buy',
        '大户资金卖出额': 'large_investor_sell',
        '散户资金买入额': 'retail_investor_buy',
        '散户资金卖出额': 'retail_investor_sell',
        '机构资金买入额': 'institutional_buy',
        '机构资金卖出额': 'institutional_sell',
        '沪深300成分股': 'hs300_component',
        '上证50成分股': 'sse50_component',
        '中证500成分股': 'csi500_component',
        '中证1000成分股': 'csi1000_component',
        '中证2000成分股': 'csi2000_component',
        '创业板指成分股': 'gem_component',
        '新版申万一级行业名称': 'industry_level1',
        '新版申万二级行业名称': 'industry_level2',
        '新版申万三级行业名称': 'industry_level3',
        '09:35收盘价': 'price_0935',
        '09:45收盘价': 'price_0945',
        '09:55收盘价': 'price_0955'
    }

    encodings = ['utf-8', 'gb2312']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                # 1. Read and discard the first line
                next(file)

                # 2. Now the file pointer is at the beginning of the second line,
                #    DictReader will treat it as the header row
                reader = csv.DictReader(file)
                
                # Check if all required headers are present after mapping
                # This helps in debugging if a file has unexpected headers
                missing_headers = [
                    csv_h for csv_h, db_h in header_mapping.items() 
                    if csv_h not in reader.fieldnames
                ]
                if missing_headers:
                    print(f"Warning: Missing expected headers in {file_path} for encoding {encoding}: {missing_headers}. Skipping this file.")
                    continue # Try next encoding or next file

                # 3. Iterate through the remaining rows (starting from the third line)
                #    and process them as data rows
                for row in reader:
                    processed_row = {}
                    for csv_header, db_column in header_mapping.items():
                        value = row.get(csv_header) # Use .get() to avoid KeyError if header is truly missing

                        # Convert values to appropriate types, handle None for empty strings
                        if db_column in ['open_price', 'high_price', 'low_price', 'close_price',
                                         'prev_close_price', 'volume', 'turnover', 'market_cap',
                                         'total_market_cap', 'net_profit_ttm', 'cash_flow_ttm',
                                         'net_assets', 'total_assets', 'total_liabilities',
                                         'net_profit_quarter', 'mid_investor_buy', 'mid_investor_sell',
                                         'large_investor_buy', 'large_investor_sell', 'retail_investor_buy',
                                         'retail_investor_sell', 'institutional_buy', 'institutional_sell',
                                         'price_0935', 'price_0945', 'price_0955']:
                            processed_row[db_column] = float(value) if value else None
                        elif db_column in ['hs300_component', 'sse50_component', 'csi500_component',
                                           'csi1000_component', 'csi2000_component', 'gem_component']:
                            processed_row[db_column] = 1 if value and value.lower() == 'true' else 0
                        else:
                            processed_row[db_column] = value
                    data.append(processed_row)
            return data # If successful, break out of encoding loop
        except Exception as e:
            # If an error occurs (e.g., encoding issues, incorrect headers), try the next encoding
            # print(f"Error reading {file_path} with encoding {encoding}: {e}")
            continue
    
    # If no encoding worked, return empty list
    return []

def main():
    # Connect to DuckDB (creates a file-based database 'stock_data.duckdb' if not exists)
    # Using a file-based database persists data across runs.
    # For in-memory database: con = duckdb.connect(database=':memory:')
    con = duckdb.connect(database='stock_data.duckdb', read_only=False)
    print("Connected to DuckDB database: stock_data.duckdb")

    data_dir = './stock-trading-data-pro-2025-06-23N'
    
    # Ensure the directory exists
    if not os.path.isdir(data_dir):
        print(f"Error: Data directory '{data_dir}' not found. Please create it and place CSV files inside.")
        return

    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in '{data_dir}'. Please ensure your CSV files are in this directory.")
        return

    # Define the column mapping and types for explicit table creation
    header_mapping_for_schema = {
        '股票代码': 'stock_code',
        '股票名称': 'stock_name',
        '交易日期': 'trade_date',
        '开盘价': 'open_price',
        '最高价': 'high_price',
        '最低价': 'low_price',
        '收盘价': 'close_price',
        '前收盘价': 'prev_close_price',
        '成交量': 'volume',
        '成交额': 'turnover',
        '流通市值': 'market_cap',
        '总市值': 'total_market_cap',
        '净利润TTM': 'net_profit_ttm',
        '现金流TTM': 'cash_flow_ttm',
        '净资产': 'net_assets',
        '总资产': 'total_assets',
        '总负债': 'total_liabilities',
        '净利润(当季)': 'net_profit_quarter',
        '中户资金买入额': 'mid_investor_buy',
        '中户资金卖出额': 'mid_investor_sell',
        '大户资金买入额': 'large_investor_buy',
        '大户资金卖出额': 'large_investor_sell',
        '散户资金买入额': 'retail_investor_buy',
        '散户资金卖出额': 'retail_investor_sell',
        '机构资金买入额': 'institutional_buy',
        '机构资金卖出额': 'institutional_sell',
        '沪深300成分股': 'hs300_component',
        '上证50成分股': 'sse50_component',
        '中证500成分股': 'csi500_component',
        '中证1000成分股': 'csi1000_component',
        '中证2000成分股': 'csi2000_component',
        '创业板指成分股': 'gem_component',
        '新版申万一级行业名称': 'industry_level1',
        '新版申万二级行业名称': 'industry_level2',
        '新版申万三级行业名称': 'industry_level3',
        '09:35收盘价': 'price_0935',
        '09:45收盘价': 'price_0945',
        '09:55收盘价': 'price_0955'
    }

    # Define schema for the stock_data table based on header_mapping
    column_definitions = []
    for csv_header, db_column in header_mapping_for_schema.items():
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
            column_definitions.append(f"{db_column} DATE") # Use DATE type for trade_date
        else:
            column_definitions.append(f"{db_column} VARCHAR") # Default to VARCHAR for text fields
    
    # Create the table if it does not exist. This ensures existing data is not deleted.
    try:
        create_table_sql = f"CREATE TABLE IF NOT EXISTS stock_data ({', '.join(column_definitions)});"
        con.execute(create_table_sql)
        print("Table 'stock_data' ensured to exist with defined schema (or created if new).")
    except Exception as e:
        print(f"Error ensuring table 'stock_data' exists: {e}")
        con.close()
        return

    total_records_inserted = 0
    
    print(f"Found {len(csv_files)} CSV files to process.")
    for i, csv_file in enumerate(csv_files):
        file_path = os.path.join(data_dir, csv_file)
        print(f"Processing file {i+1}/{len(csv_files)}: {file_path}")
        
        processed_data_from_file = convert_and_read_csv(file_path)
        
        if processed_data_from_file:
            # Convert current file's data to DataFrame
            df_current_file = pd.DataFrame(processed_data_from_file)
            
            try:
                # Ensure the DataFrame columns match the table schema for append
                # This handles cases where a CSV might be missing a column
                # It's crucial that all columns defined in header_mapping_for_schema
                # are present in df_current_file before appending, even if they are None.
                # Reindex df_current_file to match the exact columns of the DuckDB table.
                df_current_file = df_current_file.reindex(columns=[col.split(' ')[0] for col in column_definitions], fill_value=None)
                
                con.append("stock_data", df_current_file)
                total_records_inserted += len(df_current_file)
                print(f"Successfully inserted {len(df_current_file)} records from {csv_file}. Total inserted: {total_records_inserted}")
            except Exception as e:
                print(f"Error appending data from {csv_file} to DuckDB: {e}")
        else:
            print(f"Skipped {csv_file} due to processing issues or no valid data found.")
    
    print(f"\nTotal records inserted into DuckDB: {total_records_inserted}")

    # Example query: Fetch closing price and volume for a specific date range
    start_date = '2023-06-01'
    end_date = '2023-06-30'
    
    query = f"""
        SELECT trade_date, close_price, volume
        FROM stock_data
        WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
    """
    
    print(f"\nExecuting example query for data from {start_date} to {end_date}...")
    start_query_time = time.time()
    try:
        result_df = con.execute(query).fetchdf()
        end_query_time = time.time()
        print(f"Query completed in: {end_query_time - start_query_time:.2f} seconds.")
        print(f"Stock data from {start_date} to {end_date}:")
        print(result_df.head()) # Print first few rows of the result DataFrame
        print(f"Total rows in result: {len(result_df)}")
    except Exception as e:
        print(f"Error executing query: {e}")

    # Close the database connection
    con.close()
    print("\nDuckDB connection closed.")

if __name__ == '__main__':
    main()