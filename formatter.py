import os
import pandas as pd
import logging
import datetime

# def format_label_data_from_file(file_path):
#     # Open the file and read its contents
#     with open(file_path, 'r') as file:
#         lines = file.readlines()
    
#     # Skip the first line (header) and process the remaining lines
#     for line in lines[1:]:
#         parts = line.split()

#         if len(parts) >= 5:  # Ensure the line has at least 5 parts (start date, start time, end date, end time, transportation mode)
#             start_time = parts[0] + " " + parts[1]
#             end_time = parts[2] + " " + parts[3]
#             transportation_mode = parts[4]

#             # Format the start and end times
#             formatted_start_time = start_time.replace("/", "").replace(":", "").replace(" ", "")
#             formatted_end_time = end_time.replace("/", "").replace(":", "").replace(" ", "")
            
#             # Print the formatted times and transportation mode
#             print(f"Start time: {formatted_start_time}")
#             print(f"End time: {formatted_end_time}")
#             print(f"Transportation mode: {transportation_mode}\n")

# # Example usage with a file path
# file_path = "./dataset/dataset/Data/010/labels.txt"  # Update with the correct path to your file
# if os.path.exists(file_path):
#     format_label_data_from_file(file_path)
# else:
#     print(f"File not found: {file_path}")

# import os

# def extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels):
#     # Ensure the file exists before opening it
#     if not os.path.exists(plt_file_path):
#         raise FileNotFoundError(f"The file {plt_file_path} does not exist.")
    
#     # Open the .plt file and read its lines
#     with open(plt_file_path, 'r') as file:
#         lines = file.readlines()
        
#         if len(lines) <= 6:
#             raise ValueError("Invalid .plt file: Less than 7 lines of data")
        
#         # Start by searching for the first match of the start time (after the header)
#         formatted_start_time_from_labels = start_time_from_labels.replace('/', '-').replace(' ', ',')
#         start_time_found = None

#         for line in lines[6:]:  # Skip the first 6 header lines
#             line_parts = line.strip().split(',')
            
#             if len(line_parts) < 7:
#                 continue  # Skip invalid lines
            
#             # Extract the date and time from the current line
#             current_date = line_parts[5]  # The 6th element is the date
#             current_time = line_parts[6]  # The 7th element is the time
            
#             # Reformat the date and time to match the labels.txt format for comparison
#             current_formatted_time = current_date + ',' + current_time
            
#             if current_formatted_time == formatted_start_time_from_labels:
#                 # We found the first match for the start time
#                 start_time_found = current_date.replace('-', '/') + ' ' + current_time
#                 break
        
#         if not start_time_found:
#             raise ValueError(f"No matching start time found in {plt_file_path} for {start_time_from_labels}")
        
#         # Extract the last valid data line (end time)
#         last_data_line = lines[-1].strip()
#         last_data_line_parts = last_data_line.split(',')

#         if len(last_data_line_parts) < 7:
#             raise ValueError("Invalid .plt file: Insufficient data in the last line")

#         end_date = last_data_line_parts[5]  # The 6th element is the date
#         end_time = last_data_line_parts[6]  # The 7th element is the time
        
#         # Format the end time in the same format as labels.txt (YYYY/MM/DD HH:MM:SS)
#         formatted_end_time = end_date.replace('-', '/') + ' ' + end_time
        
#         return start_time_found, formatted_end_time

# def search_and_match_plt_files(base_directory, start_time_from_labels, end_time_from_labels):
#     # Walk through the directory and search for .plt files
#     for root, dirs, files in os.walk(base_directory):
#         for file in files:
#             if file.endswith('.plt'):
#                 plt_file_path = os.path.join(root, file)
                
#                 try:
#                     # Extract the start and end times from the current .plt file
#                     start_time_from_plt, end_time_from_plt = extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels)
                    
#                     # Compare the start and end times (from .plt and labels.txt)
#                     if start_time_from_plt == start_time_from_labels and end_time_from_plt == end_time_from_labels:
#                         print(f"Match found in file {plt_file_path}! Start time: {start_time_from_plt}, End time: {end_time_from_plt}")
#                         return (start_time_from_plt, end_time_from_plt)
#                     else:
#                         print(f"No match for {plt_file_path}. Start time: {start_time_from_plt}, End time: {end_time_from_plt}")
                
#                 except Exception as e:
#                     print(f"Error processing file {plt_file_path}: {e}")

# # Example usage
# base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
# start_time_from_labels = "2008/04/02 11:24:21"  # Example start time from labels.txt
# end_time_from_labels = "2008/04/02 11:50:45"  # Example end time from labels.txt

# search_and_match_plt_files(base_directory, start_time_from_labels, end_time_from_labels)


# # Function to search for matches between .plt files and labels.txt
# def search_and_match_plt_files(base_directory, labels_file_path):
#     # Parse labels.txt manually
#     labels = parse_labels_file(labels_file_path)
#     for root, dirs, files in os.walk(base_directory):
#         for file in files:
#             if file.endswith('.plt'):
#                 plt_file_path = os.path.join(root, file)
#                 try:
#                     for start_time_from_labels, end_time_from_labels, _ in labels:
#                         start_time_from_plt, end_time_from_plt = extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels)
#                         print(f"starttime: {start_time_from_plt}, end_time_from_labels: {end_time_from_plt} ")
#                         if start_time_from_plt == start_time_from_labels and end_time_from_plt == end_time_from_labels:
#                             logging.warning(f"Match found in file {plt_file_path}! Start time: {start_time_from_plt}, End time: {end_time_from_plt}")
#                         else:
#                             print(f"No match for {plt_file_path}. Start time: {start_time_from_plt}, End time: {end_time_from_plt}")

#                 except Exception as e:
#                     print(f"Error processing file {plt_file_path}: {e}")

# # Example usage
# base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
# labels_file_path = "./dataset/dataset/Data/010/labels.txt"  # Path to labels.txt

# search_and_match_plt_files(base_directory, labels_file_path)



import os

# Function to clean and format time from labels
def clean_time_format(time_string):
    """Ensure the time string is in the correct format: YYYY/MM/DD HH:MM:SS"""
    return time_string.strip()

# Function to extract the start and end times from a .plt file
def extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels):
    if not os.path.exists(plt_file_path):
        raise FileNotFoundError(f"The file {plt_file_path} does not exist.")
    
    with open(plt_file_path, 'r') as file:
        lines = file.readlines()

        if len(lines) <= 6:
            raise ValueError(f"Invalid .plt file {plt_file_path}: Less than 7 lines of data")

        # Skip the first 6 lines (header) and search for start time match
        formatted_start_time_from_labels = start_time_from_labels.replace('/', '-').replace(' ', ',')

        print(f"ffffff {formatted_start_time_from_labels} and count {len(formatted_start_time_from_labels)}")

        start_time_found = None
        for line in lines[6:]:  # Skip header lines
            line_parts = line.strip().split(',')
            if len(line_parts) < 7:
                continue  # Skip invalid lines
            current_date = line_parts[5]  # The 6th element is the date
            current_time = line_parts[6]  # The 7th element is the time
            current_formatted_time = current_date + ',' + current_time

            if current_formatted_time == formatted_start_time_from_labels:
                start_time_found = current_date.replace('-', '/') + ' ' + current_time
                break

        if not start_time_found:
            raise ValueError(f"No matching start time found in {plt_file_path} for {start_time_from_labels}")

        # Extract last valid data line (end time)
        last_data_line = lines[-1].strip()
        last_data_line_parts = last_data_line.split(',')

        if len(last_data_line_parts) < 7:
            raise ValueError(f"Invalid .plt file {plt_file_path}: Insufficient data in the last line")

        end_date = last_data_line_parts[5]
        end_time = last_data_line_parts[6]
        formatted_end_time = end_date.replace('-', '/') + ' ' + end_time

        print(f"start_time_found {start_time_found} and formatted_end_time {formatted_end_time}")

        return start_time_found, formatted_end_time

# Function to parse labels.txt without pandas
def parse_labels_file(labels_file_path):
    labels = []
    with open(labels_file_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:
            parts = line.strip().split()
            if len(parts) >= 3:
                start_time = clean_time_format(parts[0] + " " + parts[1])
                end_time = clean_time_format(parts[2] + " " + parts[3])
                transportation_mode = parts[4]
                labels.append((start_time, end_time, transportation_mode))
    return labels



# # Function to search for matches between .plt files and labels.txt
# def search_and_match_plt_files(base_directory, labels_file_path):
#     # Parse labels.txt manually
#     labels = parse_labels_file(labels_file_path)
    
#     for root, dirs, files in os.walk(base_directory):
#         for file in files:
#             if file.endswith('.plt'):
#                 plt_file_path = os.path.join(root, file)
#                 try:
#                     # Try matching the current .plt file with all labels in the file
#                     for start_time_from_labels, end_time_from_labels, _ in labels:
#                         try:
#                             # Extract the start and end time from the .plt file based on the start time from the label
#                             start_time_from_plt, end_time_from_plt = extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels)

#                             # Print or log the comparison of times
#                             print(f"Checking file {plt_file_path}: Start time from plt: {start_time_from_plt}, Start time from labels: {start_time_from_labels}, End time from labels: {end_time_from_labels}")

#                             # Check if the start and end times match
#                             if start_time_from_plt == start_time_from_labels and end_time_from_plt == end_time_from_labels:
#                                 print(f"Match found in file {plt_file_path}! Start time: {start_time_from_plt}, End time: {end_time_from_plt}")
#                             else:
#                                 print(f"No match for {plt_file_path}. Start time from plt: {start_time_from_plt}, End time from plt: {end_time_from_plt}")

#                         except Exception as inner_e:
#                             print(f"Error matching label times in file {plt_file_path}: {inner_e}")
                        
#                 except Exception as e:
#                     print(f"Error processing file {plt_file_path}: {e}")

# # Example usage
# base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
# labels_file_path = "./dataset/dataset/Data/010/labels.txt"  # Path to labels.txt

# search_and_match_plt_files(base_directory, labels_file_path)

# Function to search for matches between .plt files and labels.txt
def search_and_match_plt_files(base_directory, labels_file_path):
    # Parse labels.txt manually
    labels = parse_labels_file(labels_file_path)
    
    # Walk through the .plt files in the base directory
    for root, dirs, files in os.walk(base_directory):
        for file in files:
            if file.endswith('.plt'):
                plt_file_path = os.path.join(root, file)
                print(f"count {len(plt_file_path)}")
                matched = False  # To track if a match has been found for this file

                try:
                    # For each .plt file, iterate through all the labels
                    for start_time_from_labels, end_time_from_labels, _ in labels:
                        # Extract start and end time from the .plt file based on the start time from the label
                        start_time_from_plt, end_time_from_plt = extract_start_and_end_time_from_plt(plt_file_path, start_time_from_labels)

                        print(f"Checking file {plt_file_path}: Start time from plt: {start_time_from_plt} and count ${len(start_time_from_plt)}, Start time from labels: {start_time_from_labels}, End time from labels: {end_time_from_labels}")

                        # Check if the start and end times match
                        if start_time_from_plt == start_time_from_labels and end_time_from_plt == end_time_from_labels:
                            logging.warning(f"Match found in file {plt_file_path}! Start time: {start_time_from_plt}, End time: {end_time_from_plt}")
                            matched = True  # Flag as matched
                            break  # Exit the inner loop once a match is found

                    # Log when no match is found after checking all labels
                    if not matched:
                        print(f"No match found for file {plt_file_path} after checking all labels.")

                except Exception as e:
                    pass
                    # print(f"Error processing file {plt_file_path}: {e}")

# Example usage
base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
labels_file_path = "./dataset/dataset/Data/010/labels.txt"  # Path to labels.txt

search_and_match_plt_files(base_directory, labels_file_path)
