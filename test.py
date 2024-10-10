from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
import pandas as pd
import fnmatch
import logging
import csv
import csv
import logging
from datetime import datetime

class Gurjot:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                   id INT NOT NULL,
                   has_labels BOOLEAN DEFAULT FALSE, 
                   PRIMARY KEY (id));
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                     id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                     user_id INT,
                     transportation_mode VARCHAR(255),
                     start_date_time DATETIME,
                     end_date_time DATETIME,
                     FOREIGN KEY (user_id) REFERENCES User(id));          
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """CREATE TABLE IF NOT EXISTS Trackpoint(
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    activity_id INT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    altitude DOUBLE,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id));
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    def clean_time_format(self, time_string):
        """Ensure the time string is in the correct format: YYYY/MM/DD HH:MM:SS"""
        return time_string.strip()
        

    def parse_labels_file(self, labels_file_path):        # todo, make a hashmap for better performance
        labels = []
        with open(labels_file_path, 'r') as file:
            lines = file.readlines()
            # jump over first line
            for line in lines[1:]:
                parts = line.strip().split()
                if len(parts) >= 3:
                    start_time = self.clean_time_format(parts[0] + " " + parts[1])
                    end_time = self.clean_time_format(parts[2] + " " + parts[3])
                    transportation_mode = parts[4]
                    labels.append((start_time, end_time, transportation_mode))
        return labels

    def has_more_than_2500_lines(self, plt_file_path):
            with open(plt_file_path, 'r') as file:
                # Skip the first 6 lines (header)
                for _ in range(6):
                    next(file, None)
                # Count remaining lines, return True if more than 2500
                return sum(1 for _ in file) > 2500
            

    def insert_tpoints(self, trajectory_file):
        # Traverse the data directory to find all trajectory files and insert data directly
            try:
                with open(trajectory_file, 'r') as file:
                    reader = csv.reader(file)
                    for _ in range(6):
                        next(reader)  # Skip header lines
                    
                    for row in reader:            
                        if len(row) < 7:
                            continue  # Skip invalid lines

                        latitude = float(row[0])
                        longitude = float(row[1])
                        altitude = int(float(row[3])) if int(float(row[3])) != -777 else None
                        date_days = float(row[4])
                        date_time = f"{row[5]} {row[6]}"

                        print(f"Latitude: {latitude}, Longitude: {longitude}, Altitude: {altitude}, Date Days: {date_days}, Date Time: {date_time}")

                        try:
                            # Convert date_time to a proper datetime format
                            date_time = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                            # Insert trackpoint directly into the Trackpoint table
                            trackpoint_query = "INSERT INTO Trackpoint (latitude, longitude, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s)"
                            self.cursor.execute(trackpoint_query, (latitude, longitude, altitude, date_days, date_time.strftime("%Y-%m-%d %H:%M:%S")))
                        except ValueError as ve:
                            logging.warning(f"Skipping trackpoint due to datetime format error: {ve}")
                            continue
                # Commit the transaction after processing each file
                self.db_connection.commit()
            except Exception as e:
                        logging.error(f"Error processing file {trajectory_file}: {e}")



    def insert_trackpoints(self, start_time, end_time, file_name):
        print("Trying to insert trackpoints for file:", file_name)

        try:
            # Convert start_time and end_time to datetime objects with the correct format
            start_time_dt = datetime.strptime(start_time, "%Y-%m-%d,%H:%M:%S")
            end_time_dt = datetime.strptime(end_time, "%Y-%m-%d,%H:%M:%S")

            with open(file_name, 'r') as file:
                reader = csv.reader(file)
                for _ in range(6):
                    next(reader)  # Skip header lines

                print("File opened successfully. Processing trackpoints...")
                list_to_save = []
                started = False  # Flag to indicate when to start collecting data

                for row in reader:
                    if len(row) < 7:
                        continue  # Skip invalid lines

                    try:
                        # Parse data from the row
                        latitude = float(row[0])
                        longitude = float(row[1])
                        altitude = int(float(row[3])) if int(float(row[3])) != -777 else None
                        date_days = float(row[4])
                        date_time_str = f"{row[5]},{row[6]}"  # Note the comma between date and time
                        date_time_dt = datetime.strptime(date_time_str, "%Y-%m-%d,%H:%M:%S")  # Adjusted format

                        # Debug print
                        print(f"Latitude: {latitude}, Longitude: {longitude}, Altitude: {altitude}, Date Days: {date_days}, Date Time: {date_time_dt}")

                        # Check if current row matches the start time
                        if date_time_dt == start_time_dt:
                            print(f"[INFO] Start time found for activity in file {file_name}, '{start_time_dt}'")
                            started = True  # Start collecting data

                        if started:
                            list_to_save.append((1, latitude, longitude, altitude, date_days, date_time_str))

                            # Stop collecting data after end_time
                            if date_time_dt == end_time_dt:
                                print(f"[INFO] End time found for activity in file {file_name}, '{end_time_dt}'")
                                break

                    except Exception as parse_error:
                        logging.error(f"Error parsing row {row}: {parse_error}")
                        continue


                if list_to_save:
                    try:
                        # Insert all trackpoints into the Trackpoint table at once
                        trackpoint_query = """
                            INSERT INTO Trackpoint (
                                activity_id, latitude, longitude, altitude, date_days, date_time
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        self.cursor.executemany(trackpoint_query, list_to_save)
                    except Exception as db_error:
                        logging.error(f"Error inserting trackpoints: {db_error}")
                        
                # Commit the transaction after processing each file
                self.db_connection.commit()

        except Exception as e:
            logging.error(f"Error processing file {file_name}: {e}")



    # should return in format "date,time"
    def extract_date_time_from_trejectories2(self, file_path):

        date_time_set = []  # Using a set for fast lookup

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
                date_time_set.append(date_time)  # Add (date, time) tuple to the set, example: ('2007-08-04', '04:11:30')

        return date_time_set


    def check_date_time_exists2(self, query_date_time, date_time_set) -> bool:
        return query_date_time in date_time_set


    def check_label_file(self, base_dir):
        # Construct the full path to 'label.txt'
        labels_file_path = os.path.join(base_dir, 'labels.txt')
        
        # Check if the file exists
        return os.path.isfile(labels_file_path)



    # Finner start og slutt tid -> Dette skal inn i activity 
    def search_and_match_plt_files(self, base_directory):
        user_id = base_directory.split("/")[4]

        if (self.check_label_file(base_directory)):
            labels_file_path = os.path.join(base_directory, 'labels.txt')
            labels = self.parse_labels_file(labels_file_path)  # example: [('2007/06/26 11:32:29', '2007/06/26 11:40:29', 'bus')]
    
            for label in labels:
                start_date_time = label[0].replace('/', '-').replace(' ', ',')
                end_date_time = label[1].replace('/', '-').replace(' ', ',')
                transportation_mode = label[2]

                for root, _, files in os.walk(base_directory):
                    for file in files:
                        if not file.endswith('.plt'):
                            continue
                        
                        plt_file_path = os.path.join(root, file)
                        if self.has_more_than_2500_lines(plt_file_path):
                            continue
                        
                        trajectory_datetimes = self.extract_date_time_from_trejectories2(plt_file_path)
                        
                        if self.check_date_time_exists2(start_date_time, trajectory_datetimes) and self.check_date_time_exists2(end_date_time, trajectory_datetimes):
                            print(f"Found match for activity ... start: {start_date_time}, end: {end_date_time} for transportation mode {transportation_mode}")
                            print(f"It was found in file {plt_file_path} for user {base_directory.split("/")[4]}")
                            self.insert_activity(user_id, transportation_mode, start_date_time, end_date_time)
                            self.insert_trackpoints(start_date_time, end_date_time, plt_file_path)
                            return
                            break  # Move to the next label after finding both start and end
                    else:
                        continue  # Only runs if the inner loop wasn't broken
                    break  # Break the outer loop when a match is found
        else:
            trajectory_file_path = os.path.join(base_directory, 'Trajectory')
            for root, _, files in os.walk(trajectory_file_path):
                    for file in files:
                        print("File ", file)
                        if not file.endswith('.plt'):
                            continue
                        
                        plt_file_path = os.path.join(root, file)
                        print("Processing file:", plt_file_path)

                        if self.has_more_than_2500_lines(plt_file_path):
                            print(f"Skipping {file} as it has more than 2500 lines.")
                            continue
                        
                        date_time = self.extract_date_time_from_trejectories2(plt_file_path)
                        self.insert_activity(user_id, "NULL", date_time[0], date_time[-1])


    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                VALUES (%s, %s, %s, %s);"""
        self.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        self.db_connection.commit()

    def drop_table(self, table_name):
        try:
            print(f"Dropping table {table_name} if it exists...")
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.cursor.execute(query)
            self.db_connection.commit()
            print(f"Table {table_name} dropped successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

    
    def insert_users(self, root_path, table_name='User'):
        try:
            user_ids = []

            # Traverse the directory structure
            for dirpath, dirnames, filenames in os.walk(root_path):
                for dirname in dirnames:
                    user_ids.append(dirname)

            # Insert each user ID into the User table
            for user_id in user_ids:
                query = f"INSERT IGNORE INTO {table_name} (id) VALUES (%s)"
                self.cursor.execute(query, (user_id,))

            self.db_connection.commit()
            print(f"User IDs from folders in {root_path} inserted successfully into {table_name} table.")
        except FileNotFoundError:
            print(f"Root path not found: {root_path}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def update_user_table(self, file_path, table_name='User'):
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()

            ids = [line.strip() for line in lines]

            # Set has_labels to TRUE for IDs in the file
            query = f"UPDATE {table_name} SET has_labels = TRUE WHERE id IN ({','.join(['%s'] * len(ids))})"
            self.cursor.execute(query, ids)

            # Set has_labels to FALSE for IDs not in the file
            query = f"UPDATE {table_name} SET has_labels = FALSE WHERE id NOT IN ({','.join(['%s'] * len(ids))})"
            self.cursor.execute(query, ids)

            self.db_connection.commit()
            print(f"User table updated successfully based on IDs in {file_path}.")
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")



def main():
    program = None
    try:
        program = Gurjot()

        base_directory = "./dataset/dataset/Data/010/"


        program.drop_table("Trackpoint")
        program.drop_table("Activity")
        program.drop_table('User')

        # base_path = "./dataset/dataset/Data"  # Path to the Data directory with user folders
        # labeled_ids_path = "./dataset/dataset/labeled_ids.txt"  # Path to labeled_ids.txt

        program.create_user_table()
        program.create_activity_table()
        program.create_trackpoint_table()


        program.insert_users('./dataset/dataset/')
        program.update_user_table('./dataset/dataset/labeled_ids.txt')

        program.search_and_match_plt_files(base_directory)




    except Exception as e:
        print("ERROR: Failed to use the database:", e)

    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()














    # base_directory = "./dataset/dataset/Data/010/Trajectory"  # Folder containing .plt files
    # labels_file_path = "./dataset/dataset/Data/010/labels.txt"  # Path to labels.txt

    # search_and_match_plt_files(base_directory, labels_file_path)



















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
