import csv
import pandas as pd

class TrackpointManager:
    def __init__(self, cursor, db_connection):
        # Initialize with a cursor and database connection
        self.cursor = cursor
        self.db_connection = db_connection
        # List to store trackpoint data before batch insertion
        self.trackpoint_data = []

    def load_trackpoints_from_file(self, plt_path):
        # This function reads trackpoints from a .plt file and returns a list of valid trackpoints
        trackpoints = []
        try:
            with open(plt_path, 'r') as file:
                reader = csv.reader(file)
                # Skip the first 6 lines which are headers (based on the given file format)
                for _ in range(6):
                    next(reader)  

                # Read the actual trackpoint data from each row
                for row in reader:
                    latitude = float(row[0])  # Extract latitude from the row
                    longitude = float(row[1])  # Extract longitude from the row
                    altitude = int(float(row[3])) if int(float(row[3])) != -777 else None  # Handle altitude data, treat -777 as None
                    date_days = float(row[4])  # Extract date in day format
                    date_time = f"{row[5]} {row[6]}"  # Combine date and time strings

                    try:
                        # Convert date_time to a proper datetime object
                        date_time = pd.to_datetime(date_time, format="%Y-%m-%d %H:%M:%S")
                        # Add the parsed trackpoint to the list of trackpoints
                        trackpoints.append((latitude, longitude, altitude, date_days, date_time))
                    except ValueError as ve:
                        # If there's an issue with the date format, print an error and skip this trackpoint
                        print(f"Skipping trackpoint due to datetime format error: {ve}")
        except Exception as e:
            # Handle any other exceptions that occur while reading the file
            print(f"Error reading file {plt_path}: {e}")
        
        return trackpoints  # Return the list of trackpoints (could be empty if no valid trackpoints were found)

    def prepare_trackpoint_data(self, activity_id, trackpoints):
        # This function prepares trackpoint data for batch insertion into the database.
        # We append each trackpoint to a list, which will be used later for inserting all data in one go
        for tp in trackpoints:
            # Each trackpoint is associated with an activity_id to link it to a specific activity
            # Store the trackpoint details in the list for later batch insertion
            self.trackpoint_data.append((
                activity_id,  # Link to the corresponding activity
                tp[0],  # Latitude
                tp[1],  # Longitude
                tp[2],  # Altitude (could be None if not valid)
                tp[3],  # Date in day format
                tp[4].strftime("%Y-%m-%d %H:%M:%S")  # Convert datetime object to string for SQL insertion
            ))

    def insert_trackpoint_data(self, batch_size):
        # Insert all prepared trackpoint data into the TrackPoint table in batches.
        if self.trackpoint_data:
            # Prepare the SQL query for inserting trackpoint data
            trackpoint_query = """
                INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_days, date_time) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            # Insert data in batches to avoid overloading the database
            for i in range(0, len(self.trackpoint_data), batch_size):
                # Insert a chunk of trackpoints of size 'batch_size'
                self.cursor.executemany(trackpoint_query, self.trackpoint_data[i:i + batch_size])
                # Commit after each batch is inserted to make sure data is saved in case of issues
                self.db_connection.commit()
            
            # Once all trackpoints are inserted, clear the list
            self.trackpoint_data = []
