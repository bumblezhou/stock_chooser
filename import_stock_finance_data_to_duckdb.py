import duckdb
import csv
import os
import pandas as pd
import time

def convert_and_read_csv(file_path):
    """
    Reads a CSV file, skipping the first row and using the second row as headers.
    It attempts to read with GB2312 encoding first, then falls back to UTF-8.
    It processes data types, handles missing values, and cleans field names by removing '@xbx'.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        tuple: (list of dictionaries with row data, list of cleaned headers)
    """
    data = []
    encodings = ['gb2312', 'utf-8']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                # 1. Read and discard the first line
                next(file)

                # 2. Read the second line to get headers and clean them
                headers = next(file).strip().split(',')
                cleaned_headers = [h.replace('@xbx', '') for h in headers]
                header_mapping = {h: ch for h, ch in zip(headers, cleaned_headers)}

                # # 3. Reset file pointer to start of data (third line)
                # next(file)  # Skip first line again
                # next(file)  # Skip header line again

                # 4. Use DictReader with cleaned headers
                reader = csv.DictReader(file, fieldnames=headers)
                
                # Process each row
                for row in reader:
                    processed_row = {}
                    for csv_header, db_column in header_mapping.items():
                        value = row.get(csv_header)
                        
                        # Convert values to appropriate types, handle None for empty strings
                        if db_column in ['stock_code', 'statement_format', 'report_date', 'publish_date', '抓取时间']:
                            processed_row[db_column] = value if value else None
                        else:
                            try:
                                processed_row[db_column] = float(value) if value else None
                            except (ValueError, TypeError):
                                processed_row[db_column] = None
                    data.append(processed_row)
            return data, cleaned_headers
        except Exception as e:
            print(f"Error reading {file_path} with encoding {encoding}: {e}")
            continue
    
    # If no encoding worked, return empty list and empty headers
    return [], []

def main():
    # Connect to DuckDB
    con = duckdb.connect(database='stock_data.duckdb', read_only=False)
    print("Connected to DuckDB database: stock_data.duckdb")

    data_dir = './stock-fin-data-xbx-2025-06-25'
    
    # Ensure the directory exists
    if not os.path.isdir(data_dir):
        print(f"Error: Data directory '{data_dir}' not found. Please create it and place CSV files inside.")
        con.close()
        return

    # Collect CSV files from subdirectories
    csv_files = []
    for stock_code in os.listdir(data_dir):
        stock_dir = os.path.join(data_dir, stock_code)
        if os.path.isdir(stock_dir):
            csv_file = os.path.join(stock_dir, f"{stock_code}_一般企业.csv")
            if os.path.isfile(csv_file):
                csv_files.append((stock_code, csv_file))
    
    if not csv_files:
        print(f"No '_一般企业.csv' files found in subdirectories of '{data_dir}'. Please ensure your CSV files are in the correct structure.")
        con.close()
        return

    # Get headers from the first valid CSV file to define the schema
    first_valid_file = None
    cleaned_headers = []
    for stock_code, csv_file in csv_files:
        data, headers = convert_and_read_csv(csv_file)
        if data and headers:
            first_valid_file = csv_file
            cleaned_headers = headers
            break
    
    if not cleaned_headers:
        print("No valid CSV files found to determine schema. Aborting.")
        con.close()
        return

    # Define schema for the stock_finance_data table
    column_definitions = []
    for header in cleaned_headers:
        if header in ['stock_code', 'statement_format', 'report_date', 'publish_date', '抓取时间']:
            column_definitions.append(f"{header} VARCHAR")
        elif header == 'report_date':
            column_definitions.append(f"{header} DATE")
        else:
            column_definitions.append(f"{header} DOUBLE")
    
    # Create the table if it does not exist
    try:
        create_table_sql = f"CREATE TABLE IF NOT EXISTS stock_finance_data ({', '.join(column_definitions)});"
        con.execute(create_table_sql)
        print("Table 'stock_finance_data' ensured to exist with defined schema (or created if new).")
    except Exception as e:
        print(f"Error ensuring table 'stock_finance_data' exists: {e}")
        con.close()
        return

    total_records_inserted = 0
    
    print(f"Found {len(csv_files)} CSV files to process.")
    for i, (stock_code, csv_file) in enumerate(csv_files):
        print(f"Processing file {i+1}/{len(csv_files)}: {csv_file}")
        
        processed_data_from_file, _ = convert_and_read_csv(csv_file)
        
        if processed_data_from_file:
            # Convert current file's data to DataFrame
            df_current_file = pd.DataFrame(processed_data_from_file)
            
            try:
                # Ensure DataFrame columns match the table schema
                df_current_file = df_current_file.reindex(columns=cleaned_headers, fill_value=None)
                
                con.append("stock_finance_data", df_current_file)
                total_records_inserted += len(df_current_file)
                print(f"Successfully inserted {len(df_current_file)} records from {csv_file}. Total inserted: {total_records_inserted}")
            except Exception as e:
                print(f"Error appending data from {csv_file} to DuckDB: {e}")
        else:
            print(f"Skipped {csv_file} due to processing issues or no valid data found.")
    
    print(f"\nTotal records inserted into DuckDB: {total_records_inserted}")

    # Example query: Fetch some financial metrics for a specific date range
    start_date = '20230601'
    end_date = '20231231'
    
    query = f"""
        SELECT stock_code, report_date, B_total_assets, B_total_liab, B_total_owner_equity
        FROM stock_finance_data
        WHERE report_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY report_date
    """
    
    print(f"\nExecuting example query for data from {start_date} to {end_date}...")
    start_query_time = time.time()
    try:
        result_df = con.execute(query).fetchdf()
        end_query_time = time.time()
        print(f"Query completed in: {end_query_time - start_query_time:.2f} seconds.")
        print(f"Financial data from {start_date} to {end_date}:")
        print(result_df.head())
        print(f"Total rows in result: {len(result_df)}")
    except Exception as e:
        print(f"Error executing query: {e}")

    # Close the database connection
    con.close()
    print("\nDuckDB connection closed.")

if __name__ == '__main__':
    main()