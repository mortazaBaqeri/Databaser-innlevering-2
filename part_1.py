import os
import mysql.connector
import csv
import time
from Backup.DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd


class GeolifeProgram:
    def __init__(self):
        # Initialize the database connection
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        # Create the User, Activity, and TrackPoint tables
        user_table = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(50) PRIMARY KEY,
            has_labels BOOLEAN
        );
        """
        activity_table = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id VARCHAR(50),
            transportation_mode VARCHAR(50),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        );
        """
        trackpoint_table = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT PRIMARY KEY AUTO_INCREMENT,
            activity_id INT,
            latitude DOUBLE,
            longitude DOUBLE,
            altitude INT,
            date_days DOUBLE,
            date_time DATETIME,
            FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
        );
        """
        
        # Execute table creation queries
        for query in (user_table, activity_table, trackpoint_table):
            self.cursor.execute(query)
        self.db_connection.commit()

    def insert_users(self, data_dir):
        # Traverse folder structure in Data folder to find how many users there are and insert these into the User table
        user_folders = [folder for folder in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, folder))]
        user_data = [{'id': user_folder, 'has_labels': False} for user_folder in user_folders]

        # Update has_labels for users with labeled data
        labeled_ids_path = os.path.join(data_dir, "../labeled_ids.txt")
        if os.path.exists(labeled_ids_path):
            with open(labeled_ids_path, 'r') as labeled_file:
                labeled_ids = set(line.strip() for line in labeled_file if line.strip())
                for user in user_data:
                    user['has_labels'] = user['id'] in labeled_ids
        
        # Insert users into User table using MySQL
        user_query = "INSERT IGNORE INTO User (id, has_labels) VALUES (%s, %s)"
        user_values = [(user['id'], user['has_labels']) for user in user_data]
        self.cursor.executemany(user_query, user_values)
        self.db_connection.commit()

    def insert_activities(self, data_dir, batch_size=60000):
        # Traverse folder structure in Data folder to find users and insert their activities
        user_folders = [folder for folder in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, folder))]
        activity_data = []
        trackpoint_data = []

        for user_id in user_folders:
            user_path = os.path.join(data_dir, user_id, "Trajectory")
            labels_path = os.path.join(data_dir, user_id, "labels.txt")
            
            # Load transportation modes if labels.txt exists
            transportation_modes = {}
            if os.path.exists(labels_path):
                with open(labels_path, 'r') as labels_file:
                    next(labels_file)  # Skip the header line
                    for line in labels_file:
                        parts = line.strip().split('\t')
                        if len(parts) == 3:
                            start_time = parts[0].replace('/', '-')
                            end_time = parts[1].replace('/', '-')
                            mode = parts[2]
                            transportation_modes[(start_time, end_time)] = mode

            # Skip if the Trajectory folder does not exist
            if not os.path.exists(user_path):
                continue

            for plt_file in os.listdir(user_path):
                plt_path = os.path.join(user_path, plt_file)
                if not os.path.isfile(plt_path):
                    continue

                try:
                    with open(plt_path, 'r') as file:
                        reader = csv.reader(file)
                        for _ in range(6):
                            next(reader)  # Skip header lines

                        trackpoints = []
                        for row in reader:
                            latitude = float(row[0])
                            longitude = float(row[1])
                            altitude = int(float(row[3])) if int(float(row[3])) != -777 else None
                            date_days = float(row[4])
                            date_time = f"{row[5]} {row[6]}"
                            try:
                                # Convert date_time to a proper datetime format
                                date_time = pd.to_datetime(date_time, format="%Y-%m-%d %H:%M:%S")
                                trackpoints.append((latitude, longitude, altitude, date_days, date_time))
                            except ValueError as ve:
                                print(f"Skipping trackpoint due to datetime format error: {ve}")

                        if trackpoints and len(trackpoints) <= 2500:  # Only consider activities with <= 2500 trackpoints
                            # Determine transportation mode, if available
                            start_date = trackpoints[0][4]
                            end_date = trackpoints[-1][4]
                            transportation_mode = transportation_modes.get((str(start_date), str(end_date)))

                            # Add activity to list
                            activity_data.append((user_id, transportation_mode, start_date.strftime("%Y-%m-%d %H:%M:%S"), end_date.strftime("%Y-%m-%d %H:%M:%S")))

                            # Add trackpoints to list
                            activity_id = len(activity_data)  # Using length as a temporary activity_id reference
                            for tp in trackpoints:
                                trackpoint_data.append((activity_id, tp[0], tp[1], tp[2], tp[3], tp[4].strftime("%Y-%m-%d %H:%M:%S")))
                except Exception as e:
                    print(f"Error reading file {plt_path}: {e}")
        
        # Insert activities and trackpoints into their respective tables using MySQL
        if activity_data:
            activity_query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
            for i in range(0, len(activity_data), batch_size):
                self.cursor.executemany(activity_query, activity_data[i:i + batch_size])
                self.db_connection.commit()

        if trackpoint_data:
            trackpoint_query = "INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
            for i in range(0, len(trackpoint_data), batch_size):
                self.cursor.executemany(trackpoint_query, trackpoint_data[i:i + batch_size])
                self.db_connection.commit()

    def fetch_data(self, table_name):
        # Fetch and display all data from the specified table
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(f"Data from table {table_name}, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        # Drop the specified table
        try:
            print(f"Dropping table {table_name}...")
            query = f"SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS {table_name}; SET FOREIGN_KEY_CHECKS=1;"
            self.cursor.execute(query, multi=True)
        except mysql.connector.Error as err:
            print(f"Failed to drop table {table_name}: {err}")

    def show_tables(self):
        # Show all tables in the database
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

def main():
    start_time = time.time()  # Start the timer
    program = None
    try:
        program = GeolifeProgram()
        # Drop tables if they exist
        for table in ["TrackPoint", "Activity", "User"]:
            program.drop_table(table)
            time.sleep(2)  # Reduced wait time to make the process faster
        
        program.create_tables()  # Create the necessary tables
        program.insert_users(data_dir="./Dataset/dataset/Data")  # Insert data into User table
        program.insert_activities(data_dir="./Dataset/dataset/Data")  # Insert data into Activity and TrackPoint tables
        
        # Fetch and display data from tables
        for table in ["User", "Activity", "TrackPoint"]:
            program.fetch_data(table_name=table)
        
        program.show_tables()  # Display all tables in the database
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()  # Close the database connection
    end_time = time.time()  # End the timer
    total_minutes = (end_time - start_time) / 60
    print(f"Total execution time: {total_minutes:.2f} minutes")


if __name__ == '__main__':
    main()