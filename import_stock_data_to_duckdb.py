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
    duckdb_path = "./stock_data.duckdb"
    if os.path.exists(duckdb_path):
        os.remove(duckdb_path)
    
    # Connect to DuckDB (creates a file-based database 'stock_data.duckdb' if not exists)
    # Using a file-based database persists data across runs.
    # For in-memory database: con = duckdb.connect(database=':memory:')
    con = duckdb.connect(database=duckdb_path, read_only=False)
    print(f"Connected to DuckDB database: {duckdb_path}")

    data_dir = f'F:\股票数据\stock-trading-data-pro-2025-08-19'
    
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
    
    # Create the stock table if it does not exist. This ensures existing data is not deleted.
    try:
        create_table_sql = f"CREATE TABLE IF NOT EXISTS stock_data ({', '.join(column_definitions)});"
        con.execute(create_table_sql)
        print("Table 'stock_data' ensured to exist with defined schema (or created if new).")
    except Exception as e:
        print(f"Error ensuring table 'stock_data' exists: {e}")
        con.close()
        return
    
    # Create the finance table if it does not exist
    try:
        create_table_sql = f'CREATE TABLE stock_finance_data(stock_code VARCHAR, statement_format VARCHAR, report_date VARCHAR, publish_date VARCHAR, "抓取时间" VARCHAR, B_currency_fund DOUBLE, B_settle_reserves DOUBLE, B_lending_fund DOUBLE, B_tradable_fnncl_assets DOUBLE, B_derivative_fnncl_assets DOUBLE, B_bill_receivable DOUBLE, B_account_receivable DOUBLE, B_bill_and_account_receivable DOUBLE, B_receivable_financing DOUBLE, B_prepays DOUBLE, B_premium_receivable DOUBLE, B_rein_account_receivable DOUBLE, B_rein_contract_reserve DOUBLE, B_interest_receivable DOUBLE, B_dividend_receivable DOUBLE, B_other_receivables DOUBLE, B_other_receivables_sum DOUBLE, B_buy_resale_fnncl_assets DOUBLE, B_inventory DOUBLE, B_contract_asset DOUBLE, B_divided_into_asset_for_sale DOUBLE, B_noncurrent_asset_due_within1y DOUBLE, B_other_cunrren_assets DOUBLE, B_flow_assets_diff_sri DOUBLE, B_flow_assets_diff_tbi DOUBLE, B_total_current_assets DOUBLE, B_loans_and_payments DOUBLE, B_fa_calc_by_amortized_cost DOUBLE, B_other_compre_fa_by_fv DOUBLE, B_saleable_finacial_assets DOUBLE, B_held_to_maturity_invest DOUBLE, B_debt_right_invest DOUBLE, B_other_debt_right_invest DOUBLE, B_lt_receivable DOUBLE, B_lt_equity_invest DOUBLE, B_other_ei_invest DOUBLE, B_other_uncurrent_fa DOUBLE, B_invest_property DOUBLE, B_fixed_asset DOUBLE, B_fixed_asset_sum DOUBLE, B_construction_in_process DOUBLE, B_construction_in_process_sum DOUBLE, B_project_goods_and_material DOUBLE, B_fixed_assets_disposal DOUBLE, B_productive_biological_assets DOUBLE, B_oil_and_gas_asset DOUBLE, B_right_of_use_assets DOUBLE, B_intangible_assets DOUBLE, B_dev_expenditure DOUBLE, B_goodwill DOUBLE, B_lt_deferred_expense DOUBLE, B_dt_assets DOUBLE, B_othr_noncurrent_assets DOUBLE, B_noncurrent_assets_diff_sri DOUBLE, B_noncurrent_assets_diff_tbi DOUBLE, B_total_noncurrent_assets DOUBLE, B_asset_diff_sri DOUBLE, B_asset_diff_tbi DOUBLE, B_total_assets DOUBLE, B_st_borrow DOUBLE, B_loan_from_central_bank DOUBLE, B_saving_and_interbank_deposit DOUBLE, B_borrowing_funds DOUBLE, B_tradable_fnncl_liab DOUBLE, B_derivative_fnncl_liab DOUBLE, B_bill_payable DOUBLE, B_accounts_payable DOUBLE, B_bill_and_account_payable DOUBLE, B_advance_payment DOUBLE, B_contract_liab DOUBLE, B_fnncl_assets_sold_for_repur DOUBLE, B_charge_and_commi_payable DOUBLE, B_payroll_payable DOUBLE, B_tax_payable DOUBLE, B_interest_payable DOUBLE, B_dividend_payable DOUBLE, B_other_payables DOUBLE, B_other_payables_sum DOUBLE, B_rein_payable DOUBLE, B_insurance_contract_reserve DOUBLE, B_acting_td_sec DOUBLE, B_act_underwriting_sec DOUBLE, B_divided_into_liab_for_sale DOUBLE, B_noncurrent_liab_due_in1y DOUBLE, B_differed_income_current_liab DOUBLE, B_st_bond_payable DOUBLE, B_other_current_liab DOUBLE, B_flow_debt_diff_sri DOUBLE, B_flow_debt_diff_tbi DOUBLE, B_total_current_liab DOUBLE, B_lt_loan DOUBLE, B_bond_payable DOUBLE, B_perpetual_capital_sec DOUBLE, B_preferred DOUBLE, B_lease_libilities DOUBLE, B_lt_payable DOUBLE, B_lt_payable_sum DOUBLE, B_lt_staff_salary_payable DOUBLE, B_special_payable DOUBLE, B_estimated_liab DOUBLE, B_dt_liab DOUBLE, B_differed_incomencl DOUBLE, B_othr_noncurrent_liab DOUBLE, B_noncurrent_liab_diff_sri DOUBLE, B_noncurrent_liab_diff_sbi DOUBLE, B_total_noncurrent_liab DOUBLE, B_liab_diff_sri DOUBLE, B_liab_diff_tbi DOUBLE, B_total_liab DOUBLE, B_actual_received_capital DOUBLE, B_capital_reserve DOUBLE, B_treasury DOUBLE, B_bs_other_compre_income DOUBLE, B_other_equity_instruments DOUBLE, B_preferred_shares DOUBLE, B_appropriative_reserve DOUBLE, B_earned_surplus DOUBLE, B_general_risk_provision DOUBLE, B_undstrbtd_profit DOUBLE, B_frgn_currency_convert_diff DOUBLE, B_total_equity_atoopc DOUBLE, B_minority_equity DOUBLE, B_holder_equity_diff_sri DOUBLE, B_equity_right_diff_tbi DOUBLE, B_total_owner_equity DOUBLE, B_liab_and_equity_diff_sri DOUBLE, B_liab_and_equity_diff_tbi DOUBLE, B_total_liab_and_owner_equity DOUBLE, R_operating_total_revenue DOUBLE, R_revenue DOUBLE, R_interest_income DOUBLE, R_earned_premium DOUBLE, R_fee_and_commi_income DOUBLE, R_operating_revenuediff_sri DOUBLE, R_operating_revenuediff_tbi DOUBLE, R_operating_total_cost DOUBLE, R_operating_cost DOUBLE, R_interest_payout DOUBLE, R_charge_and_commi_expenses DOUBLE, R_refunded_premium DOUBLE, R_compensate_net_pay DOUBLE, R_extract_ic_reserve_net_amt DOUBLE, R_commi_on_insurance_policy DOUBLE, R_rein_expenditure DOUBLE, R_operating_taxes_and_surcharge DOUBLE, R_sales_fee DOUBLE, R_manage_fee DOUBLE, R_rad_cost_sum DOUBLE, R_financing_expenses DOUBLE, R_interest_fee DOUBLE, R_fc_interest_income DOUBLE, R_asset_impairment_loss DOUBLE, R_credit_impairment_loss DOUBLE, R_operating_cost_diff_sri DOUBLE, R_operating_cost_diff_tbi DOUBLE, R_fv_chg_income DOUBLE, R_invest_income DOUBLE, R_ii_from_jc_etc DOUBLE, R_amortized_cost_fnncl_ass_cfrm DOUBLE, R_net_open_hedge_income DOUBLE, R_exchange_gain DOUBLE, R_asset_disposal_gain DOUBLE, R_other_income DOUBLE, R_op_diff_sri DOUBLE, R_op_diff_tbi DOUBLE, R_op DOUBLE, R_non_operating_income DOUBLE, R_noncurrent_asset_dispose_gain DOUBLE, R_nonoperating_cost DOUBLE, R_noncurrent_asset_dispose_loss DOUBLE, R_total_profit_diff_sri DOUBLE, R_total_profit_diff_tbi DOUBLE, R_total_profit DOUBLE, R_income_tax_cost DOUBLE, R_np_diff_sri DOUBLE, R_np_diff_tbi DOUBLE, R_np DOUBLE, R_continued_operating_np DOUBLE, R_stop_operating_np DOUBLE, R_np_atoopc DOUBLE, R_minority_gal DOUBLE, R_basic_eps DOUBLE, R_dlt_earnings_per_share DOUBLE, R_othrcompre_income_atoopc DOUBLE, R_cannt_reclass_to_gal DOUBLE, R_asset_change_due_to_remeasure DOUBLE, R_cannt_reclass_gal_equity_law DOUBLE, R_other_not_reclass_to_gal DOUBLE, R_other_equity_invest_fvc DOUBLE, R_corp_credit_risk_fvc DOUBLE, R_reclass_to_gal DOUBLE, R_reclass_togal_in_equity_law DOUBLE, R_saleable_fv_chg_gal DOUBLE, R_reclass_and_salable_gal DOUBLE, R_cf_hedging_gal_valid_part DOUBLE, R_fc_convert_diff DOUBLE, R_other_reclass_to_gal DOUBLE, R_other_debt_right_invest_fvc DOUBLE, R_fa_reclassi_amt DOUBLE, R_other_debt_right_invest_ir DOUBLE, R_cash_flow_hedge_reserve DOUBLE, R_othrcompre_income_atms DOUBLE, R_total_compre_income DOUBLE, R_total_compre_income_atsopc DOUBLE, R_total_compre_income_atms DOUBLE, C_effect_of_exchange_chg_on_cce DOUBLE, C_cce_net_add_amt_diff_sri_dm DOUBLE, C_cce_net_add_amt_diff_tbi_dm DOUBLE, C_cash_received_of_sales_service DOUBLE, C_deposit_and_interbank_net_add DOUBLE, C_borrowing_net_add_central_bank DOUBLE, C_lending_net_add_other_org DOUBLE, C_cash_received_from_orig_ic DOUBLE, C_net_cash_received_from_rein DOUBLE, C_naaassured_saving_and_invest DOUBLE, C_naa_of_disposal_fnncl_assets DOUBLE, C_cash_received_of_interest_etc DOUBLE, C_borrowing_net_increase_amt DOUBLE, C_net_add_in_repur_capital DOUBLE, C_refund_of_tax_and_levies DOUBLE, C_cash_received_of_other_oa DOUBLE, C_oa_cash_inflow_diff_sri DOUBLE, C_oa_cash_inflow_diff_tbi DOUBLE, C_sub_total_of_ci_from_oa DOUBLE, C_goods_buy_and_service_cash_pay DOUBLE, C_loan_and_advancenet_add DOUBLE, C_naa_of_cb_and_interbank DOUBLE, C_cash_of_orig_ic_indemnity DOUBLE, C_cash_paid_for_interests_etc DOUBLE, C_cash_paid_for_pd DOUBLE, C_cash_paid_to_staff_etc DOUBLE, C_payments_of_all_taxes DOUBLE, C_other_cash_paid_related_to_oa DOUBLE, C_oa_cash_outflow_diff_sri DOUBLE, C_oa_cash_outflow_diff_tbi DOUBLE, C_sub_total_of_cos_from_oa DOUBLE, C_ncf_diff_of_oa_sri DOUBLE, C_ncf_diff_of_oa_tbi DOUBLE, C_ncf_from_oa DOUBLE, C_cash_received_of_dspsl_invest DOUBLE, C_invest_income_cash_received DOUBLE, C_net_cash_of_disposal_assets DOUBLE, C_net_cash_of_disposal_branch DOUBLE, C_cash_received_of_other_fa DOUBLE, C_ia_cash_inflow_diff_sri DOUBLE, C_ia_cash_inflow_diff_tbi DOUBLE, C_sub_total_of_ci_from_ia DOUBLE, C_cash_paid_for_assets DOUBLE, C_invest_paid_cash DOUBLE, C_net_add_in_pledge_loans DOUBLE, C_net_cash_amt_from_branch DOUBLE, C_other_cash_paid_related_to_ia DOUBLE, C_ia_cash_outflow_diff_sri DOUBLE, C_ia_cash_outflow_diff_tbi DOUBLE, C_sub_total_of_cos_from_ia DOUBLE, C_ncf_diff_from_ia_sri DOUBLE, C_ncf_diff_from_ia_tbi DOUBLE, C_ncf_from_ia DOUBLE, C_cash_received_of_absorb_invest DOUBLE, C_cr_from_minority_holders DOUBLE, C_cash_received_of_borrowing DOUBLE, C_cash_received_from_bond_issue DOUBLE, C_cash_received_of_othr_fa DOUBLE, C_fa_cash_in_flow_diff_sri DOUBLE, C_fa_cash_in_flow_diff_tbi DOUBLE, C_sub_total_of_ci_from_fa DOUBLE, C_cash_pay_for_debt DOUBLE, C_cash_paid_of_distribution DOUBLE, C_dap_paid_to_minority_holder DOUBLE, C_othrcash_paid_relating_to_fa DOUBLE, C_fa_cash_out_flow_diff_sri DOUBLE, C_fa_cash_out_flow_diff_tbi DOUBLE, C_sub_total_of_cos_from_fa DOUBLE, C_ncf_diff_from_fa_sri DOUBLE, C_ncf_diff_from_fa_tbi DOUBLE, C_ncf_from_fa DOUBLE, C_net_increase_in_cce DOUBLE, C_initial_cce_balance DOUBLE, C_final_balance_of_cce DOUBLE, C_np_cfs DOUBLE, C_asset_impairment_reserve DOUBLE, C_depreciation_etc DOUBLE, C_intangible_assets_amortized DOUBLE, C_lt_deferred_expenses_amrtzt DOUBLE, C_loss_of_disposal_assets DOUBLE, C_fixed_assets_scrap_loss DOUBLE, C_loss_from_fv_chg DOUBLE, C_finance_cost_cfs DOUBLE, C_invest_loss DOUBLE, C_dt_assets_decrease DOUBLE, C_dt_liab_increase DOUBLE, C_inventory_decrease DOUBLE, C_operating_items_decrease DOUBLE, C_increase_of_operating_item DOUBLE, C_si_other DOUBLE, C_ncf_diff_from_oa_im_sri DOUBLE, C_ncf_diff_from_oa_im_tbi DOUBLE, C_ncf_from_oa_im DOUBLE, C_debt_tranfer_to_capital DOUBLE, C_cb_due_within1y DOUBLE, C_finance_lease_fixed_assets DOUBLE, C_ending_balance_of_cash DOUBLE, C_initial_balance_of_cash DOUBLE, C_si_final_balance_of_cce DOUBLE, C_initial_balance_of_cce DOUBLE, C_cce_net_add_diff_im_sri DOUBLE, C_cce_net_add_diff_im_tbi DOUBLE, C_net_increase_in_cce_im DOUBLE);'
        con.execute(create_table_sql)
        print("Table 'stock_finance_data' ensured to exist with defined schema (or created if new).")
    except Exception as e:
        print(f"Error ensuring table 'stock_finance_data' exists: {e}")
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