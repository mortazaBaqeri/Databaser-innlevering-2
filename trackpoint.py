import os
import pandas as pd
import logging
import datetime
import csv
import os
from DbConnector import DbConnector


class Trackpoint:
    def __init__(self):
        # Initialize the database connection
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor




    def has_more_than_2500_lines(self, plt_file_path):
        with open(plt_file_path, 'r') as file:
            # Skip the first 6 lines (header)
            for _ in range(6):
                next(file, None)
            # Count remaining lines, return True if more than 2500
            return sum(1 for _ in file) > 2500


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
