from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
import pandas as pd
import fnmatch
from trackpoint import Trackpoint
import logging
import csv

class Tasks():

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # def create_trackpoint_table(self):
    #     query = """CREATE TABLE IF NOT EXISTS Trackpoint(
    #                 id INT PRIMARY KEY AUTO_INCREMENT,
    #                 activity_id INT,
    #                 latitude DOUBLE,
    #                 longitude DOUBLE,
    #                 altitude DOUBLE,
    #                 date_days DOUBLE,
    #                 date_time DATETIME,
    #                 FOREIGN KEY (activity_id) REFERENCES Activity(id));
    #             """
    #     self.cursor.execute(query)
    #     self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    activity_id INT,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    altitude DOUBLE,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id));
                """
        query = query.replace("%s", table_name)
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT NOT NULL,
                   has_labels BOOLEAN DEFAULT FALSE, 
                   PRIMARY KEY (id))
                """
        query = query.replace("%s", table_name)
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                     id INT PRIMARY KEY AUTO_INCREMENT,
                     user_id INT,
                     transportation_mode VARCHAR(255),
                     start_date_time DATETIME,
                     end_date_time DATETIME,
                     FOREIGN KEY (user_id) REFERENCES User(id));          
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_trackpoints(self, data_dir):
        # Traverse the data directory to find all trajectory files and insert data directly
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".plt"):
                    trajectory_file = os.path.join(root, file)
                    print(f"trajectory_file: {trajectory_file}")

                    if self.has_more_than_2500_lines(trajectory_file):
                        logging.info(f"Skipping {trajectory_file} because it has more than 2500 lines")
                        continue
                    
                    try:
                        print("Trying to open file")
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


    def insert_data(self, table_name, row):
        query = "INSERT INTO %s (id) VALUES (%s)"
        query = query.replace("%s", table_name).replace("%s", row)
        self.cursor.execute(query)
        self.db_connection.commit()
        print(f"Data {row} inserted successfully.")


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


    def format_label_data_from_file(self, file_path):
        lables_path = os.path.join(file_path, "labels.txt")

        # Open the file and read its contents
        with open(lables_path, 'r') as file:
            lines = file.readlines()
        
        # Skip the first line (header) and process the remaining lines
        for line in lines[1:]:
            parts = line.split()

            if len(parts) >= 5:  # Ensure the line has at least 5 parts (start date, start time, end date, end time, transportation mode)
                start_time = parts[0] + " " + parts[1]
                end_time = parts[2] + " " + parts[3]
                transportation_mode = parts[4]

                # Format the start and end times
                formatted_start_time = start_time.replace("/", "").replace(":", "").replace(" ", "")
                formatted_end_time = end_time.replace("/", "").replace(":", "").replace(" ", "")


                # # Print the formatted times and transportation mode
                # print(f"Start time: {formatted_start_time}")
                # print(f"End time: {formatted_end_time}")
                # print(f"Transportation mode: {transportation_mode}\n")

                #return formatted_start_time, formatted_end_time, transportation_mode
                

    # Function to insert and match activities by traversing the dataset and matching transportation modes
    def insert_and_match_activities(self):
        base_path = "./dataset/dataset/Data/010"  # Example path to user folder
        labels_path = os.path.join(base_path, "labels.txt")

        # Read the labels.txt file and ensure proper parsing
        if os.path.exists(labels_path):
            try:
                # Ensure that tabs or spaces are correctly handled with sep='\t' or sep=r'\s+'
                labels = pd.read_csv(labels_path, sep=r'\t|\s+', engine='python', header=None, 
                                     names=['start_time', 'end_time', 'transportation_mode'])
                
                print(f"Labels loaded from {labels_path}:")

                # Correctly print out the parsed data
                for index, row in labels.iterrows():
                    print(f"Start time: {row['start_time']}")
                    print(f"End time: {row['end_time']}")
                    print(f"Transportation mode: {row['transportation_mode']}")
                    print(f"Formatted start time: {self.format_time_string(row['start_time'])}")
                    print()  # For an empty line between each group

            except Exception as e:
                print(f"Failed to process labels.txt: {e}")
                return

        # Now, process the .plt files in the "Trajectory" folder
        trajectory_path = os.path.join(base_path, "Trajectory")
        if not os.path.exists(trajectory_path):
            print(f"Trajectory folder not found for user at {trajectory_path}")
            return

        # Loop through all .plt files in the Trajectory folder
        plt_files = os.listdir(trajectory_path)
        if len(plt_files) == 0:
            print(f"No .plt files found in {trajectory_path}")
            return

        print(f"Found {len(plt_files)} .plt files in {trajectory_path}")
        for plt_file in plt_files:
            if plt_file.endswith(".plt"):
                plt_file_name = plt_file[:-4]  # Remove the .plt extension
                print(f"Processing .plt file: {plt_file_name}")

                # Check if the .plt file matches any formatted start_time in labels.txt
                matched_label = labels[labels['start_time_fmt'] == plt_file_name]

                if not matched_label.empty:
                    # Extract matching data
                    transportation_mode = matched_label['transportation_mode'].values[0]
                    start_time = matched_label['start_time'].values[0]
                    end_time = matched_label['end_time'].values[0]

                    # Print match details
                    print(f"Matched: {plt_file_name}, Start: {start_time}, End: {end_time}, Mode: {transportation_mode}")
                else:
                    # Print no match details
                    print(f"No match for {plt_file_name}")


    # Function to insert activity data into the database
    def insert_activity(self, user_id, transportation_mode, start_time, end_time):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """
        # Set transportation_mode as None if there's no label
        self.cursor.execute(query, (user_id, transportation_mode, start_time, end_time))
        self.db_connection.commit()


    # def has_more_than_2500_lines(self, plt_file_path):
    #     with open(plt_file_path, 'r') as file:
    #         # Skip the first 6 lines (header)
    #         for _ in range(6):
    #             next(file, None)
    #         # Count remaining lines, return True if more than 2500
    #         return sum(1 for _ in file) > 2500


    def drop_table(self, table_name):
        try:
            print(f"Dropping table {table_name} if it exists...")
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.cursor.execute(query)
            self.db_connection.commit()
            print(f"Table {table_name} dropped successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")


    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        query = query.replace("%s", table_name)
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def find_matching_files(directory, pattern):
        # Iterate over all files in the directory
        for filename in os.listdir(directory):
            # Check if the filename matches the .plt pattern
            if fnmatch.fnmatch(filename, '*.plt'):
                # Check if the given string matches the filename (without extension)
                if pattern in filename:
                    return True                
                

def main():
    program = None
    try:
        program = Tasks()

        base_path = "./dataset/dataset"  # Path to the Data directory with user folders
        dataPath = f"{base_path}/Data"  # Path to the Data directory with user folders
        labeled_ids_path = f"{base_path}labeled_ids.txt"  # Path to labeled_ids.txt
            

        user = "User"
        trackpoint = "Trackpoint"
        activity = "Activity"

        program.drop_table(user)
        program.create_table(user)
        program.insert_users('./dataset/dataset/')
        program.update_user_table(labeled_ids_path)


        program.drop_table(activity)
        program.create_activity_table()
        program.insert_and_match_activities()
        
        program.drop_table(trackpoint)
        program.create_trackpoint_table(trackpoint)
        program.insert_trackpoints(data_dir=dataPath)



    except Exception as e:
        print("ERROR: Failed to use the database:", e)

    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
