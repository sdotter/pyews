import json
import argparse
from datetime import datetime

def load_json(file_path):
    """Helper function to load JSON data from a file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("File {} not found.".format(file_path))
        return []

def save_json(data, file_path):
    """Helper function to save JSON data to a file."""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def merge_data(data1, data2):
    """Merge two lists of weather metric data."""
    merged_data = []

    # Iterate through items, assuming both lists have the same structure
    for metric1, metric2 in zip(data1, data2):
        if metric1['id'] != metric2['id']:
            print("Mismatched metric ids.")
            continue

        # Combine the 'data' arrays
        combined_data = metric1['data'] + metric2['data']
        
        # Sort combined data by the first element of each sublist (timestamp)
        combined_data.sort(key=lambda x: x[0])

        # Create a new merged entry
        merged_entry = {
            **metric1,  # Use the structure from metric1
            'data': combined_data  # Use the sorted data array
        }

        merged_data.append(merged_entry)

    return merged_data

def merge_json_files(file1_path, file2_path, output_path):
    """Merge two JSON files and save the output, ensuring data arrays are ordered."""
    data1 = load_json(file1_path)
    data2 = load_json(file2_path)
    
    # Merge and sort the data entries
    merged_data = merge_data(data1, data2)
    
    # Save merged data to output file
    save_json(merged_data, output_path)
    
    print("Merged data successfully saved to {}".format(output_path))

def main():
    parser = argparse.ArgumentParser(description="Merge two weather JSON files.")
    parser.add_argument('file1', help='Path to the first JSON file')
    parser.add_argument('file2', help='Path to the second JSON file')
    parser.add_argument('output', help='Path for the output merged JSON file')
    args = parser.parse_args()
    merge_json_files(args.file1, args.file2, args.output)

if __name__ == "__main__":
    main()