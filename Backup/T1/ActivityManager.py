import os
from Backup.T1.TrackpointManager import TrackpointManager

class ActivityManager:
    def __init__(self, cursor, db_connection):
        # Initialize with a cursor and database connection
        self.cursor = cursor
        self.db_connection = db_connection
        # Create an instance of TrackpointManager, since activities need trackpoints!
        self.trackpoint_manager = TrackpointManager(cursor, db_connection)

    def create_activity_table(self):
        # SQL query to create the Activity table if it doesn't exist already
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
        # Execute the query to create the Activity table
        self.cursor.execute(activity_table)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        # SQL query to create the TrackPoint table if it doesn't exist
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
        # Execute the query to create the TrackPoint table
        self.cursor.execute(trackpoint_table)
        self.db_connection.commit()

    def insert_activities(self, data_dir, batch_size=30000):
        # Get a list of user folders to process activities for each user
        user_folders = self._get_user_folders(data_dir)
        activity_data = []

        # Iterate over all users and process their data
        for user_id in user_folders:
            self._process_user_directory(user_id, data_dir, activity_data)

        # Insert all activities into the Activity table in batches
        self._insert_activity_data(activity_data, batch_size)

        # Insert trackpoints using TrackpointManager (which has prepared the data during processing)
        self.trackpoint_manager.insert_trackpoint_data(batch_size)

    def _get_user_folders(self, data_dir):
        # Helper function to get the list of folders for all users
        return [folder for folder in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, folder))]

    def _process_user_directory(self, user_id, data_dir, activity_data):
        # Construct paths to the Trajectory folder and labels.txt for the current user
        user_path = os.path.join(data_dir, user_id, "Trajectory")
        labels_path = os.path.join(data_dir, user_id, "labels.txt")

        # Load transportation modes if labels.txt exists
        transportation_modes = self._load_transportation_modes(labels_path)

        # If the Trajectory folder doesn't exist, skip this user
        if not os.path.exists(user_path):
            return

        # Loop over all trajectory files in the user's folder
        for plt_file in os.listdir(user_path):
            plt_path = os.path.join(user_path, plt_file)
            if not os.path.isfile(plt_path):
                continue  # Skip if it's not a valid file

            try:
                # Load the trackpoints from the current file
                trackpoints = self.trackpoint_manager.load_trackpoints_from_file(plt_path)
                
                # Only process if there are trackpoints and if they are within the limit of 2500
                if trackpoints and len(trackpoints) <= 2500:
                    self._prepare_activity_and_delegate_trackpoint_data(
                        user_id, trackpoints, transportation_modes, activity_data
                    )
            except Exception as e:
                # Catch any exception that occurs while reading the file
                print(f"Error reading file {plt_path}: {e}")

    def _load_transportation_modes(self, labels_path):
        # Helper function to load transportation modes from labels.txt
        transportation_modes = {}
        if os.path.exists(labels_path):
            with open(labels_path, 'r') as labels_file:
                next(labels_file)  # Skip the header line
                for line in labels_file:
                    parts = line.strip().split('\t')
                    if len(parts) == 3:
                        # Extract start time, end time, and transportation mode from each line
                        start_time = parts[0].replace('/', '-')
                        end_time = parts[1].replace('/', '-')
                        mode = parts[2]
                        # Store transportation modes in a dictionary with (start_time, end_time) as the key
                        transportation_modes[(start_time, end_time)] = mode
        return transportation_modes

    def _prepare_activity_and_delegate_trackpoint_data(self, user_id, trackpoints, transportation_modes, activity_data):
        # Get the start and end dates for the activity based on trackpoints
        start_date = trackpoints[0][4]
        end_date = trackpoints[-1][4]
        
        # Determine the transportation mode for this activity, if available
        transportation_mode = transportation_modes.get((str(start_date), str(end_date)))

        # Append activity data to the list, which will be inserted later
        activity_data.append((user_id, transportation_mode, start_date.strftime("%Y-%m-%d %H:%M:%S"), end_date.strftime("%Y-%m-%d %H:%M:%S")))

        # Use the length of activity_data as a temporary activity_id (since it will match after insertion)
        activity_id = len(activity_data)  

        # Delegate trackpoint preparation to TrackpointManager
        self.trackpoint_manager.prepare_trackpoint_data(activity_id, trackpoints)

    def _insert_activity_data(self, activity_data, batch_size):
        # Insert activities into the database in batches, using batch_size to avoid overload
        if activity_data:
            activity_query = """
                INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES (%s, %s, %s, %s)
            """
            # Execute the insertion in chunks to avoid overwhelming the database
            for i in range(0, len(activity_data), batch_size):
                self.cursor.executemany(activity_query, activity_data[i:i + batch_size])
                self.db_connection.commit()
