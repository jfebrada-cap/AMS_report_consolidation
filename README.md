# AWS Utilization Report Consolidation Script

## Overview

This Python script consolidates AWS utilization reports from multiple dates and environments into organized Excel workbooks with conditional formatting. It processes RDS, EC2, Redis, and MSK metrics across different environments and applies green highlighting to low utilization values.

## Features

- **Multi-Environment Processing**: Handles Patikar, Batalan, Production, and Shared Services environments
- **Date Consolidation**: Combines data from multiple date folders into single workbooks
- **Horizontal Data Merging**: Intelligently merges data from different AWS services (RDS, EC2, Redis, MSK)
- **Conditional Formatting**: Highlights low utilization values (0-14%) in green
- **Environment-Specific Highlighting**: Different columns are highlighted for each environment
- **Windows-Compatible**: Supports folder names with hyphens (MM-DD-YYYY format)

## Prerequisites

### Python Packages
Install the required packages:
```bash
pip install pandas openpyxl xlsxwriter
```

### Folder Structure
Your AWS reports should be organized as follows:
```
AWS/
├── 11-18-2025/
│   ├── Patikar/
│   │   ├── RDS_report.xlsx
│   │   ├── EC2_report.xlsx
│   │   └── Redis_report.xlsx
│   ├── Batalan/
│   ├── Production/
│   └── Shared_services/
├── 11-19-2025/
├── 11-20-2025/
├── 11-21-2025/
└── 11-24-2025/
```

## Usage

### 1. Basic Execution
Place the script (`ams_report.py`) in your main AWS reports directory and run:
```bash
python ams_report.py
```

### 2. Script Location
The script automatically detects its location and processes the surrounding folder structure. Make sure your directory structure matches the expected format.

### 3. Output
The script creates a `Consolidated_Reports` folder with the following files:
- `Patikar_Consolidated.xlsx`
- `Batalan_Consolidated.xlsx` 
- `Production_Consolidated.xlsx`
- `Shared_Services_Consolidated.xlsx`

## Output Structure

Each consolidated workbook contains:

### Sheets
- **All_Data**: Combined data from all dates
- **Individual Date Sheets**: Raw data for each specific date (e.g., `11_18_2025`)

### Columns
- **Preserved**: `Date_Report`, `Environment`, and all AWS metrics
- **Removed**: `Source_File`, `Date_Folder` columns
- **Added**: Tracking columns for data provenance

## Conditional Formatting

### Highlight Rules
- **Color**: Green background (`#C6EFCE`)
- **Range**: Values between 0-14% (inclusive)
- **Type**: Only numeric values (ignores empty/text cells)

### Environment-Specific Columns

#### Batalan & Patikar
- `95p CPUUtilization (%) - 30 days`
- `95p CPUUtilization (%) - 24 hours`
- `Current CPUUtilization (%)`

#### Production
- All CPU utilization columns from Batalan/Patikar
- Duplicate CPU columns (`_dup_1` suffixes)
- `Max Engine CPUUtilization` columns
- MSK Broker metrics (CPU, System, Memory for Brokers 1-3)

#### Shared Services
- All CPU utilization columns
- Duplicate CPU columns
- Database Memory Usage columns
- Engine CPU Utilization columns

## Troubleshooting

### Common Issues

1. **Permission Denied Error**
   - **Cause**: Output Excel file is open in another program
   - **Solution**: Close all Excel files and rerun the script

2. **No Date Folders Found**
   - **Cause**: Incorrect folder naming or structure
   - **Solution**: Ensure folders use `MM-DD-YYYY` format (e.g., `11-18-2025`)

3. **Missing Environment Data**
   - **Cause**: Environment folder doesn't exist for specific dates
   - **Solution**: Script will skip missing environments and continue

4. **Horizontal Merge Warnings**
   - **Cause**: No common identifiers found between Excel files
   - **Impact**: Data may be concatenated instead of properly merged
   - **Solution**: Ensure files contain common identifiers like `InstanceId`

### Error Handling
- The script includes retry logic for file access issues
- Provides detailed logging for debugging
- Continues processing other environments if one fails

## Script Details

### Key Functions

- `get_date_folders()`: Discovers and validates date folders
- `merge_data_horizontally()`: Combines data from different AWS services
- `find_columns_to_highlight()`: Environment-specific column targeting
- `apply_conditional_formatting()`: Applies green highlighting to low utilization values
- `verify_consolidation()`: Validates output and reports statistics

### Customization

To modify highlighted columns, edit the `environment_columns` dictionary in the `find_columns_to_highlight()` function.

## Example Output

After successful execution:
```
Consolidated_Reports/
├── Batalan_Consolidated.xlsx
├── Patikar_Consolidated.xlsx
├── Production_Consolidated.xlsx
└── Shared_Services_Consolidated.xlsx
```

Each file contains properly merged data with green highlighting on low utilization values, making it easy to identify underutilized AWS resources.
