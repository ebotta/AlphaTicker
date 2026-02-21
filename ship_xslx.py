import pandas as pd

# Define the input CSV file path and the output Excel file path
csv_file_path = 'Alphas.csv'
excel_file_path = 'Alphas.xlsx'

# Read the CSV file into a pandas DataFrame
try:
    df = pd.read_csv(csv_file_path)
    
    # Write the DataFrame to an Excel file (xlsx)
    # Setting index=False prevents the pandas DataFrame index from being written as a column in the Excel file
    df.to_excel(excel_file_path, index=False, engine='openpyxl')
    
    print(f"Successfully converted {csv_file_path} to {excel_file_path}")

except FileNotFoundError:
    print(f"Error: The file {csv_file_path} was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

