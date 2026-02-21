import pandas as pd
import sys

def xlsx_to_csv(excel_file, csv_file):
    try:
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Convert the DataFrame to a CSV file
        # index=None prevents pandas from writing row indices to the CSV
        df.to_csv(csv_file, index=None, header=True)
        print(f"Successfully converted '{excel_file}' to '{csv_file}'")

    except FileNotFoundError:
        print(f"Error: The file '{excel_file}' was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 convert.py <input_file.xlsx> <output_file.csv>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    xlsx_to_csv(input_file, output_file)

def remove_first_and_last_line(filename):
    """
    Reads a file, removes the first and last lines, and writes the
    remaining lines back to the same file.
    """
    with open(filename, 'r') as f:
        # Read all lines and store them in a list
        lines = f.readlines()
    
    # Use list slicing to get all lines except the first (lines[1:])
    # and the last (lines[:-1]), combining to lines[1:-1].
    # This creates a new list with the middle lines.
    new_lines = lines[1:-2]
    
    with open(filename, 'w') as f:
        # Write the modified list of lines back to the file
        f.writelines(new_lines)

# Example usage:
file_path = 'output_data.csv'
remove_first_and_last_line(file_path)

