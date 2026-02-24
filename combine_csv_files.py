"""
CSV File Combiner
=================
This module combines multiple CSV files from a specified directory into a single
consolidated CSV file. The script preserves the header from the first file and
appends all data rows from subsequent files, omitting their headers.

Author: Data Processing Team
Date: February 2026
"""

import os
import csv
import pathlib
from datetime import datetime


def combine_csv_files(input_dir, output_file):
    """
    Combine all CSV files in a directory into one consolidated CSV file.
    
    This function reads all CSV files from the specified directory, extracts the
    header from the first file, and combines all data rows into a single output
    file. Headers from subsequent files are automatically omitted to prevent
    duplicate header rows in the consolidated output.
    
    Parameters
    ----------
    input_dir : str
        Path to the directory containing CSV files to be combined
    output_file : str
        Path and filename for the output combined CSV file
        
    Returns
    -------
    None
        Function writes directly to file and prints progress to console
        
    Notes
    -----
    - CSV files are processed in alphabetical order
    - UTF-8 encoding is used for both input and output files
    - Empty directories will result in early return with warning message
    """
    
    # =====================================================================
    # BLOCK 1: File Discovery and Validation
    # =====================================================================
    # Scan the input directory for CSV files and sort them alphabetically
    # to ensure consistent processing order across different systems
    csv_files = sorted([f for f in os.listdir(input_dir) if f.endswith('.csv')])
    
    # Validate that CSV files exist in the directory
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    # Display discovery results to user
    print(f"Found {len(csv_files)} CSV files to combine")
    
    # =====================================================================
    # BLOCK 2: File Combination and Writing
    # =====================================================================
    # Open output file with appropriate settings for CSV writing
    # - 'w' mode: write mode, creates new file or overwrites existing
    # - newline='': prevents extra blank lines in CSV output
    # - encoding='utf-8': ensures proper handling of international characters
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        # Initialize control variables for the combination process
        writer = None              # CSV writer object (lazy initialization)
        header_written = False     # Flag to track if header has been written
        total_rows = 0            # Counter for total data rows processed
        
        # -------------------------------------------------------------
        # SUBBLOCK 2.1: Iterate Through Each CSV File
        # -------------------------------------------------------------
        for idx, csv_file in enumerate(csv_files, 1):
            # Construct full path to current CSV file
            filepath = os.path.join(input_dir, csv_file)
            
            # Display progress indicator to user
            print(f"Processing {idx}/{len(csv_files)}: {csv_file}")
            
            # ---------------------------------------------------------
            # SUBBLOCK 2.2: Read and Process Current CSV File
            # ---------------------------------------------------------
            # Open current CSV file for reading with UTF-8 encoding
            with open(filepath, 'r', encoding='utf-8') as infile:
                # Create CSV reader object for current file
                reader = csv.reader(infile)
                
                # Process each row in the current CSV file
                for row_idx, row in enumerate(reader):
                    
                    # -----------------------------------------------------
                    # SUBBLOCK 2.3: Header Row Processing
                    # -----------------------------------------------------
                    # First row (index 0) is always treated as header
                    if row_idx == 0:
                        # Write header only from the first file encountered
                        if not header_written:
                            # Lazy initialization of CSV writer on first write
                            if writer is None:
                                writer = csv.writer(outfile)
                            # Add "source_file" column to header row
                            header_with_source = row + ['source_file']
                            # Write the header row to output file
                            writer.writerow(header_with_source)
                            # Set flag to prevent writing headers from other files
                            header_written = True
                        # Skip to next row (don't process header as data)
                        continue
                    
                    # -----------------------------------------------------
                    # SUBBLOCK 2.4: Data Row Processing
                    # -----------------------------------------------------
                    # Skip blank rows (rows that are empty or contain only whitespace)
                    if not row or all(cell.strip() == '' for cell in row):
                        continue
                    
                    # All non-header, non-blank rows are written to the output file
                    # Lazy initialization of writer (in case first file was empty)
                    if writer is None:
                        writer = csv.writer(outfile)
                    # Append source filename to each data row for traceability
                    row_with_source = row + [csv_file]
                    # Write current data row to output file
                    writer.writerow(row_with_source)
                    # Increment total row counter for final statistics
                    total_rows += 1
    
    # =====================================================================
    # BLOCK 3: Completion Report
    # =====================================================================
    # Display summary statistics to user
    print(f"\n✓ Combined {len(csv_files)} files into {output_file}")
    print(f"✓ Total data rows: {total_rows}")

if __name__ == "__main__":
    """
    Main Execution Block
    ====================
    This block executes when the script is run directly (not imported as a module).
    It configures the input/output paths, validates the directory existence, and
    orchestrates the CSV combination process.
    """
    
    # =====================================================================
    # BLOCK 4: Configuration and Setup
    # =====================================================================
    
    # ---------------------------------------------------------------------
    # SUBBLOCK 4.1: Path Configuration - EDIT THESE VARIABLES
    # ---------------------------------------------------------------------
    
    # INPUT: Directory containing CSV files to combine
    INPUT_DIR = r"C:/Users/darko/pyprojects/PAX/data/canada_pax_vehicles_out"
    
    # OUTPUT: Directory where combined CSV will be saved (leave empty "" for current directory)
    OUTPUT_DIR = r"C:/Users/darko/pyprojects/PAX/data/canada_pax_vehicles_out/combined"
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"combined_specs_canada_{timestamp}.csv"
    output_filepath = os.path.join(OUTPUT_DIR, output_filename) if OUTPUT_DIR else output_filename
    
    # =====================================================================
    # BLOCK 5: Validation and Execution
    # =====================================================================
    
    # Verify that the input directory exists before attempting processing
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Directory not found: {INPUT_DIR}")
        print("\nPlease update the 'INPUT_DIR' variable to point to your CSV files.")
    else:
        # Directory exists - proceed with combining CSV files
        combine_csv_files(INPUT_DIR, output_filepath)
        print(f"\n✓ Output saved to: {os.path.abspath(output_filepath)}")
