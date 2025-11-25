import os
import pandas as pd
import glob
from datetime import datetime
import re
import time

class AWSUtilizationConsolidator:
    def __init__(self, base_path):
        self.base_path = base_path
        self.output_dir = os.path.join(base_path, "Consolidated_Reports")
        self.environments = ['Patikar', 'Batalan', 'Shared_services', 'Production']
        self.date_folders = self.get_date_folders()
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
    def get_date_folders(self):
        """Get all date folders from the base path"""
        date_folders = []
        if not os.path.exists(self.base_path):
            print(f"Error: Base path '{self.base_path}' does not exist!")
            return date_folders
            
        for item in os.listdir(self.base_path):
            item_path = os.path.join(self.base_path, item)
            if os.path.isdir(item_path) and not item == "Consolidated_Reports":
                # Validate date format (MM-DD-YYYY) - updated to hyphens
                if re.match(r'\d{1,2}-\d{1,2}-\d{4}', item):
                    date_folders.append(item)
        
        if not date_folders:
            print("No date folders found! Please check the base path structure.")
            return []
            
        return sorted(date_folders, key=lambda x: datetime.strptime(x, '%m-%d-%Y'), reverse=True)
    
    def clean_dataframe(self, df):
        """Remove unwanted columns and clean the dataframe"""
        # Remove specified columns
        columns_to_remove = []
        for col in df.columns:
            col_str = str(col).lower()
            # Remove Source_File, Date_Folder
            if col in ['Source_File', 'Date_Folder']:
                columns_to_remove.append(col)
        
        # Keep only columns that are not in the remove list
        df_cleaned = df.drop(columns=columns_to_remove, errors='ignore')
        return df_cleaned
    
    def merge_data_horizontally(self, dataframes):
        """Merge dataframes horizontally based on common identifiers"""
        if not dataframes:
            return pd.DataFrame()
        
        # Start with the first dataframe
        merged_df = dataframes[0].copy()
        
        # For each subsequent dataframe, merge horizontally
        for i in range(1, len(dataframes)):
            df = dataframes[i]
            
            # Find common identifier columns for merging
            common_cols = []
            for col in ['InstanceId', 'Identifier', 'DBInstanceIdentifier', 'DBName', 'InstanceName']:
                if col in merged_df.columns and col in df.columns:
                    common_cols.append(col)
                    break
            
            if common_cols:
                # Merge on the first common identifier found
                merge_col = common_cols[0]
                merged_df = pd.merge(merged_df, df, on=merge_col, how='outer', suffixes=('', f'_dup_{i}'))
            else:
                # If no common identifier, concatenate horizontally (this might create duplicate rows)
                print("  Warning: No common identifier found for horizontal merge")
                # Reset index for both dataframes
                merged_df_reset = merged_df.reset_index(drop=True)
                df_reset = df.reset_index(drop=True)
                
                # Concatenate horizontally
                merged_df = pd.concat([merged_df_reset, df_reset], axis=1)
        
        # Remove duplicate columns (keeping the first occurrence)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        
        return merged_df
    
    def read_and_merge_excel_files(self, date_folder, environment):
        """Read and merge all Excel files for a specific date and environment"""
        env_path = os.path.join(self.base_path, date_folder, environment)
        
        if not os.path.exists(env_path):
            return pd.DataFrame()
        
        dataframes = []
        excel_files = glob.glob(os.path.join(env_path, "*.xlsx"))
        
        if not excel_files:
            print(f"  No Excel files found in: {env_path}")
            return pd.DataFrame()
        
        for file in excel_files:
            try:
                print(f"    Reading: {os.path.basename(file)}")
                df = pd.read_excel(file)
                
                # Add source information - use hyphens in Date_Report
                df['Source_File'] = os.path.basename(file)
                df['Date_Report'] = date_folder  # This will now be in MM-DD-YYYY format
                df['Environment'] = environment
                
                # Clean the dataframe
                df_cleaned = self.clean_dataframe(df)
                dataframes.append(df_cleaned)
                
            except Exception as e:
                print(f"    Error reading {file}: {e}")
                continue
        
        if not dataframes:
            return pd.DataFrame()
        
        # Merge dataframes horizontally instead of vertically
        if len(dataframes) == 1:
            merged_data = dataframes[0]
        else:
            merged_data = self.merge_data_horizontally(dataframes)
        
        return merged_data
    
    def find_columns_to_highlight(self, df, environment):
        """Find specific columns to highlight based on environment"""
        highlight_columns = []
        
        # Define columns to highlight for each environment
        environment_columns = {
            'Batalan': [
                '95p CPUUtilization (%) - 30 days',
                '95p CPUUtilization (%) - 24 hours', 
                'Current CPUUtilization (%)'
            ],
            'Patikar': [
                '95p CPUUtilization (%) - 30 days',
                '95p CPUUtilization (%) - 24 hours',
                'Current CPUUtilization (%)'
            ],
            'Production': [
                '95p CPUUtilization (%) - 30 days',
                '95p CPUUtilization (%) - 24 hours',
                'Current CPUUtilization (%)',
                '95p CPUUtilization (%) - 30 days_dup_1',
                '95p CPUUtilization (%) - 24 hours_dup_1',
                'Current CPUUtilization (%)_dup_1',
                'Max Engine CPUUtilization (%) - 30 days',
                'Max Engine CPUUtilization (%) - 24 hours',
                'Broker 1 CpuUser (%) - Max Over 1 day',
                'Broker 1 CpuSystem (%) - Max Over 1 day',
                'Broker 1 MemoryUsed (GB) - Max Over 1 day',
                'Broker 2 CpuUser (%) - Max Over 1 day',
                'Broker 2 CpuSystem (%) - Max Over 1 day',
                'Broker 2 MemoryUsed (GB) - Max Over 1 day',
                'Broker 3 CpuUser (%) - Max Over 1 day',
                'Broker 3 CpuSystem (%) - Max Over 1 day',
                'Broker 3 MemoryUsed (GB) - Max Over 1 day'
            ],
            'Shared_services': [
                '95p CPUUtilization (%) - 30 days',
                '95p CPUUtilization (%) - 24 hours',
                'Current CPUUtilization (%)',
                '95p CPUUtilization (%) - 30 days_dup_1',
                '95p CPUUtilization (%) - 24 hours_dup_1',
                'Current CPUUtilization (%)_dup_1',
                'Maximum Database Memory Usage (%) - 30 days',
                'Maximum Database Memory Usage (%) - 24 hours',
                'Current Database Memory Usage (%)',
                'Max Engine CPUUtilization (%) - 30 days',
                'Max Engine CPUUtilization (%) - 24 hours',
                'Current Engine CPUUtilization (%)'
            ]
        }
        
        # Get the columns for this environment
        target_columns = environment_columns.get(environment, [])
        
        # Find which target columns actually exist in the dataframe
        for col in target_columns:
            if col in df.columns:
                highlight_columns.append(col)
        
        return highlight_columns
    
    def apply_conditional_formatting(self, writer, sheet_name, df, environment):
        """Apply conditional formatting for specific columns from 0-14% - ONLY FOR NUMERIC VALUES"""
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Find only the specific columns to highlight for this environment
        highlight_columns = self.find_columns_to_highlight(df, environment)
        
        if not highlight_columns:
            print(f"  No target columns found for {environment} in sheet: {sheet_name}")
            return
        
        print(f"  Highlighting columns for {environment}: {highlight_columns}")
        
        # Create green format
        green_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        
        # Get column indices for highlight columns
        col_indices = {}
        for idx, col_name in enumerate(df.columns):
            col_indices[col_name] = idx
        
        # Apply conditional formatting to each highlight column
        for col_name in highlight_columns:
            if col_name in col_indices:
                col_idx = col_indices[col_name]
                start_row = 1  # Skip header row
                end_row = len(df)
                
                # Use FORMULA type to only highlight cells with numeric values between 0-14
                # This prevents highlighting empty cells or text cells
                col_letter = chr(65 + col_idx)  # Convert column index to letter (A, B, C, etc.)
                
                worksheet.conditional_format(
                    start_row, col_idx, end_row, col_idx,
                    {
                        'type': 'formula',
                        'criteria': f'=AND(ISNUMBER({col_letter}2), {col_letter}2>=0, {col_letter}2<=14)',
                        'format': green_format
                    }
                )
    
    def consolidate_environment_data(self, environment):
        """Consolidate data for a specific environment across all dates"""
        print(f"  Consolidating data for {environment}...")
        all_environment_data = []
        date_sheets_data = {}
        
        for date_folder in self.date_folders:
            print(f"    Processing date: {date_folder}")
            date_data = self.read_and_merge_excel_files(date_folder, environment)
            if not date_data.empty:
                # Add parsed date for sorting - updated to hyphens
                date_data['Parsed_Date'] = datetime.strptime(date_folder, '%m-%d-%Y')
                all_environment_data.append(date_data)
                date_sheets_data[date_folder] = date_data
                print(f"      Found {len(date_data)} records with {len(date_data.columns)} columns")
            else:
                print(f"      No data found for {date_folder}")
        
        if not all_environment_data:
            print(f"  No data found for environment: {environment}")
            return None
        
        # Combine all data vertically (by date)
        combined_data = pd.concat(all_environment_data, ignore_index=True, sort=False)
        
        # Sort by date descending
        combined_data = combined_data.sort_values('Parsed_Date', ascending=False)
        if 'Parsed_Date' in combined_data.columns:
            combined_data = combined_data.drop('Parsed_Date', axis=1)
        
        print(f"  Total records for {environment}: {len(combined_data)}")
        print(f"  Total columns for {environment}: {len(combined_data.columns)}")
        return combined_data, date_sheets_data
    
    def create_consolidated_workbook(self, environment):
        """Create consolidated workbook for a specific environment"""
        print(f"\n{'='*50}")
        print(f"Processing environment: {environment}")
        print(f"{'='*50}")
        
        result = self.consolidate_environment_data(environment)
        if result is None:
            return
        
        combined_data, date_sheets_data = result
        
        # Determine output filename
        if environment == 'Shared_services':
            output_file = 'Shared_Services_Consolidated.xlsx'
        else:
            output_file = f'{environment}_Consolidated.xlsx'
        
        output_path = os.path.join(self.output_dir, output_file)
        
        # Check if file is already open and close it if possible
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Create Excel writer
                print(f"  Creating workbook: {output_path}")
                with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                    # Write All_Data sheet
                    print("  Creating 'All_Data' sheet...")
                    combined_data.to_excel(writer, sheet_name='All_Data', index=False)
                    self.apply_conditional_formatting(writer, 'All_Data', combined_data, environment)
                    
                    # Write individual date sheets
                    print("  Creating date sheets...")
                    for date_folder, date_data in date_sheets_data.items():
                        # Use hyphens in sheet names - no need to replace since folders already use hyphens
                        sheet_name = date_folder.replace('-', '_')  # Replace hyphens with underscores for Excel compatibility
                        if len(sheet_name) > 31:  # Excel sheet name limit
                            sheet_name = sheet_name[:31]
                        
                        date_data.to_excel(writer, sheet_name=sheet_name, index=False)
                        self.apply_conditional_formatting(writer, sheet_name, date_data, environment)
                        print(f"    Created sheet: {sheet_name}")
                    
                    # Auto-adjust column widths
                    print("  Adjusting column widths...")
                    for sheet_name in writer.sheets:
                        worksheet = writer.sheets[sheet_name]
                        for idx, col in enumerate(combined_data.columns):
                            max_len = max(
                                combined_data[col].astype(str).str.len().max() if not combined_data.empty else 0,
                                len(str(col))
                            ) + 2
                            worksheet.set_column(idx, idx, min(max_len, 50))
                
                print(f"Successfully created: {output_path}")
                break  # Success, break out of retry loop
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    print(f"  Permission denied (attempt {attempt + 1}/{max_retries}). File might be open. Waiting 2 seconds...")
                    time.sleep(2)
                else:
                    print(f"  ERROR: Could not create {output_path} after {max_retries} attempts.")
                    print(f"  Please make sure the file is not open in Excel and you have write permissions.")
                    return
            except Exception as e:
                print(f"  ERROR creating {output_path}: {e}")
                return
    
    def process_all_environments(self):
        """Process all environments and create consolidated workbooks"""
        if not self.date_folders:
            print("No date folders to process. Exiting.")
            return
        
        print(f"Found date folders: {self.date_folders}")
        print(f"Processing environments: {self.environments}")
        
        processed_environments = []
        
        for environment in self.environments:
            # Check if environment folder exists in any date
            env_exists = False
            for date_folder in self.date_folders:
                env_path = os.path.join(self.base_path, date_folder, environment)
                if os.path.exists(env_path):
                    env_exists = True
                    break
            
            if env_exists:
                self.create_consolidated_workbook(environment)
                processed_environments.append(environment)
            else:
                print(f"\nEnvironment '{environment}' not found in any date folder")
        
        return processed_environments

def verify_consolidation(output_dir, processed_environments):
    """Verify the consolidation results and conditional formatting"""
    print(f"\n{'='*60}")
    print("VERIFICATION REPORT")
    print(f"{'='*60}")
    
    for env in processed_environments:
        if env == 'Shared_services':
            filename = 'Shared_Services_Consolidated.xlsx'
            display_name = 'Shared_Services'
        else:
            filename = f'{env}_Consolidated.xlsx'
            display_name = env
        
        file_path = os.path.join(output_dir, filename)
        
        try:
            # Read the consolidated file
            xl = pd.ExcelFile(file_path)
            
            print(f"\n{display_name}_Consolidated.xlsx")
            print(f"   Sheets: {xl.sheet_names}")
            
            # Check All_Data sheet
            all_data = pd.read_excel(file_path, sheet_name='All_Data')
            print(f"   All_Data records: {len(all_data):,}")
            print(f"   All_Data columns: {len(all_data.columns):,}")
            
            # Define columns to highlight for each environment
            environment_columns = {
                'Batalan': [
                    '95p CPUUtilization (%) - 30 days',
                    '95p CPUUtilization (%) - 24 hours', 
                    'Current CPUUtilization (%)'
                ],
                'Patikar': [
                    '95p CPUUtilization (%) - 30 days',
                    '95p CPUUtilization (%) - 24 hours',
                    'Current CPUUtilization (%)'
                ],
                'Production': [
                    '95p CPUUtilization (%) - 30 days',
                    '95p CPUUtilization (%) - 24 hours',
                    'Current CPUUtilization (%)',
                    '95p CPUUtilization (%) - 30 days_dup_1',
                    '95p CPUUtilization (%) - 24 hours_dup_1',
                    'Current CPUUtilization (%)_dup_1',
                    'Max Engine CPUUtilization (%) - 30 days',
                    'Max Engine CPUUtilization (%) - 24 hours',
                    'Broker 1 CpuUser (%) - Max Over 1 day',
                    'Broker 1 CpuSystem (%) - Max Over 1 day',
                    'Broker 1 MemoryUsed (GB) - Max Over 1 day',
                    'Broker 2 CpuUser (%) - Max Over 1 day',
                    'Broker 2 CpuSystem (%) - Max Over 1 day',
                    'Broker 2 MemoryUsed (GB) - Max Over 1 day',
                    'Broker 3 CpuUser (%) - Max Over 1 day',
                    'Broker 3 CpuSystem (%) - Max Over 1 day',
                    'Broker 3 MemoryUsed (GB) - Max Over 1 day'
                ],
                'Shared_services': [
                    '95p CPUUtilization (%) - 30 days',
                    '95p CPUUtilization (%) - 24 hours',
                    'Current CPUUtilization (%)',
                    '95p CPUUtilization (%) - 30 days_dup_1',
                    '95p CPUUtilization (%) - 24 hours_dup_1',
                    'Current CPUUtilization (%)_dup_1',
                    'Maximum Database Memory Usage (%) - 30 days',
                    'Maximum Database Memory Usage (%) - 24 hours',
                    'Current Database Memory Usage (%)',
                    'Max Engine CPUUtilization (%) - 30 days',
                    'Max Engine CPUUtilization (%) - 24 hours',
                    'Current Engine CPUUtilization (%)'
                ]
            }
            
            # Check for Date_Report column
            if 'Date_Report' in all_data.columns:
                date_report_counts = all_data['Date_Report'].value_counts()
                print(f"   Date_Report distribution:")
                for date, count in date_report_counts.items():
                    print(f"     {date}: {count:,} records")
            else:
                print(f"   WARNING: Date_Report column not found!")
            
            # Check for highlight columns and values in 0-14% range
            target_columns = environment_columns.get(env, [])
            highlight_cols = []
            
            for col in target_columns:
                if col in all_data.columns:
                    highlight_cols.append(col)
            
            if highlight_cols:
                print(f"   Highlighted columns (0-14% in green - ONLY numeric values):")
                for col in highlight_cols:
                    try:
                        # Clean the data by removing % signs and convert to numeric
                        series_clean = all_data[col].astype(str).str.replace('%', '', regex=False)
                        numeric_data = pd.to_numeric(series_clean, errors='coerce')
                        low_util_count = ((numeric_data >= 0) & (numeric_data <= 14) & (numeric_data.notna())).sum()
                        total_numeric = numeric_data.notna().sum()
                        print(f"     {col}: {low_util_count}/{total_numeric} numeric values in 0-14% range")
                    except Exception as e:
                        print(f"     {col}: Could not analyze - {e}")
            else:
                print(f"   No target columns found for highlighting")
                print(f"   Available columns: {[col for col in target_columns if col in all_data.columns]}")
                        
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

def main():
    """Main execution function"""
    # Set the base path to the current directory where the script is located
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    print("AWS Utilization Report Consolidation")
    print("====================================")
    print(f"Script location: {base_path}")
    print(f"Output directory: {os.path.join(base_path, 'Consolidated_Reports')}")
    print("\nProcessing rules:")
    print("  - Remove Source_File and Date_Folder columns")
    print("  - Keep Date_Report column for tracking (in MM-DD-YYYY format)")
    print("  - Merge data horizontally by identifiers (InstanceId, etc.)")
    print("  - Highlight ONLY specific columns in GREEN (0-14%):")
    print("    * Batalan: CPU utilization columns")
    print("    * Patikar: CPU utilization columns") 
    print("    * Production: CPU utilization + MSK broker metrics")
    print("    * Shared Services: CPU + Memory + Engine utilization")
    print("  - IMPORTANT: Only highlights NUMERIC values between 0-14%, ignores empty/text cells")
    
    # Check if base path exists
    if not os.path.exists(base_path):
        print(f"Base path '{base_path}' not found.")
        return
    
    # Initialize consolidator
    consolidator = AWSUtilizationConsolidator(base_path)
    
    # Process all environments
    processed_environments = consolidator.process_all_environments()
    
    if processed_environments:
        print(f"\nConsolidation completed successfully!")
        print(f"Processed environments: {processed_environments}")
        print(f"Output files saved to: {consolidator.output_dir}")
        
        # Run verification
        verify_consolidation(consolidator.output_dir, processed_environments)
        
        # Show final output structure
        print(f"\n{'='*60}")
        print("FINAL OUTPUT STRUCTURE")
        print(f"{'='*60}")
        print(f"Consolidated_Reports/")
        for env in processed_environments:
            if env == 'Shared_services':
                print(f"  └── Shared_Services_Consolidated.xlsx")
            else:
                print(f"  └── {env}_Consolidated.xlsx")
                
    else:
        print("\nNo environments were processed. Please check your folder structure.")

if __name__ == "__main__":
    main()