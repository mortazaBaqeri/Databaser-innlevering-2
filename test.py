import os
import pandas as pd

# Function to clean and format time from labels
def clean_time_format(time_string):
    """Ensure the time string is in the correct format: YYYY/MM/DD HH:MM:SS"""
    return time_string.strip()
    
# Function to parse labels.txt without pandas
def parse_labels_file(labels_file_path):
    # todo, make a hashmap for better performance
    labels = []
    with open(labels_file_path, 'r') as file:
        lines = file.readlines()
        # jump over first line
        for line in lines[1:]:
            parts = line.strip().split()
            if len(parts) >= 3:
                start_time = clean_time_format(parts[0] + " " + parts[1])
                end_time = clean_time_format(parts[2] + " " + parts[3])
                transportation_mode = parts[4]
                labels.append((start_time, end_time, transportation_mode))
    return labels

def has_more_than_2500_lines(plt_file_path):
        with open(plt_file_path, 'r') as file:
            # Skip the first 6 lines (header)
            for _ in range(6):
                next(file, None)
            # Count remaining lines, return True if more than 2500
            return sum(1 for _ in file) > 2500
        

# returns [(date, time)]
def extract_date_time_from_trejectories(file_path):

    date_time_set = set()  # Using a set for fast lookup

    # Open the file and read its contents
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Skip the first 6 lines and process the remaining lines
    for line in lines[6:]:
        parts = line.split(',')
        if len(parts) >= 7:  # Ensure the line has enough columns
            date = parts[5].strip()  # Date is in the 6th column
            time = parts[6].strip()  # Time is in the 7th column
            date_time_set.add((date, time))  # Add (date, time) tuple to the set, example: ('2007-08-04', '04:11:30')

    return date_time_set


# should return in format "date,time"
def extract_date_time_from_trejectories2(file_path):

    date_time_set = set()  # Using a set for fast lookup

    # Open the file and read its contents
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Skip the first 6 lines and process the remaining lines
    for line in lines[6:]:
        parts = line.split(',')
        if len(parts) >= 7:  # Ensure the line has enough columns
            date = parts[5].strip()  # Date is in the 6th column
            time = parts[6].strip()  # Time is in the 7th column
            date_time = date + "," + time
            date_time_set.add(date_time)  # Add (date, time) tuple to the set, example: ('2007-08-04', '04:11:30')

    return date_time_set


def check_date_time_exists(date_time_set, query_date, query_time):
    return (query_date, query_time) in date_time_set

def check_date_time_exists2(query_date_time, date_time_set):
    return query_date_time in date_time_set


def search_and_match_plt_files(base_directory, labels_file_path):
    labels = parse_labels_file(labels_file_path) # example: [('2007/06/26 11:32:29', '2007/06/26 11:40:29', 'bus'), ...]
    for lable in labels:
        # format times
        start_date_time = lable[0].replace('/', '-').replace(' ', ',')
        end_date_time = lable[1].replace('/', '-').replace(' ', ',')
        
        for root, dirs, files in os.walk(base_directory):
            for file in files:
                if file.endswith('.plt'):
                    plt_file_path = os.path.join(root, file)
                    if (has_more_than_2500_lines(plt_file_path)):
                        print(f"Skiped file '{plt_file_path}' as it has more than 2500 lines")
                    else:
                        date_time = extract_date_time_from_trejectories2(plt_file_path)
                        if (check_date_time_exists2(start_date_time, date_time)):
                            print(f"Found match for activity ... and start time {start_date_time}")
                            if(check_date_time_exists2(end_date_time, date_time)):
                                print(f"Also found the end_time {end_date_time}")
                                return 
                        


        # todo, ha de som er under i lpopet og sjekket ogm jeg finner en match i start og slutt tid




# Example usage
base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
labels_file_path = "./dataset/dataset/Data/010/labels.txt"  # Path to labels.txt

search_and_match_plt_files(base_directory, labels_file_path)



# Example usage
# file_path = "/Users/mortaza/Documents/Databaser innlevering 2/dataset/dataset/Data/010/Trajectory/20070804033032.plt"  # Replace this with the actual path to your .plt file

# # Extract (date, time) from the .plt file
# date_time_set = extract_date_time_from_trejectories(file_path)

# # Check if a specific (date, time) exists
# query_date = "2007-08-04"
# query_time = "03:30:45"
# print(check_date_time_exists(date_time_set, query_date, query_time))


# file_path = "/Users/mortaza/Documents/Databaser innlevering 2/dataset/dataset/Data/010/Trajectory/20070804033032.plt"  # Replace this with the actual path to your .plt file
# date_time_set = extract_date_time_from_trejectories2(file_path)
# print(date_time_set)
