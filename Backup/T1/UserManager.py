import os

class UserManager:
    def __init__(self, cursor, db_connection):
        # Initialize with a cursor and database connection to perform database operations
        self.cursor = cursor
        self.db_connection = db_connection

    def create_user_table(self):
        # SQL query to create the User table if it doesn't already exist
        user_table = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(50) PRIMARY KEY,
            has_labels BOOLEAN
        );
        """
        # Execute the query to create the User table
        self.cursor.execute(user_table)
        self.db_connection.commit()  # Commit the changes to the database

    def insert_users(self, data_dir):
        # Get a list of user folders
        # Traverse the Data folder to find all user directories, which represent different users
        user_folders = [folder for folder in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, folder))]
        
        # Create a list of dictionaries to store user info, with 'has_labels' initially set to False
        user_data = [{'id': user_folder, 'has_labels': False} for user_folder in user_folders]

        # Update 'has_labels' field for users who have labeled data
        # The labeled_ids.txt file contains IDs of users with labeled data
        labeled_ids_path = os.path.join(data_dir, "../labeled_ids.txt")
        
        if os.path.exists(labeled_ids_path):
            # If the labeled_ids.txt file exists, read it to identify users with labels
            with open(labeled_ids_path, 'r') as labeled_file:
                # Create a set of labeled IDs from the labeled file
                labeled_ids = set(line.strip() for line in labeled_file if line.strip())
                
                # Update the 'has_labels' field in user_data if the user ID is in the labeled_ids set
                for user in user_data:
                    user['has_labels'] = user['id'] in labeled_ids

        # Insert users into the User table
        # SQL query for inserting user data (using "INSERT IGNORE" to avoid duplicate key errors)
        user_query = "INSERT IGNORE INTO User (id, has_labels) VALUES (%s, %s)"
        
        # Convert the user_data list of dictionaries into a list of tuples to be used in the SQL query
        user_values = [(user['id'], user['has_labels']) for user in user_data]
        
        # Execute the insertion for all users at once
        self.cursor.executemany(user_query, user_values)
        self.db_connection.commit()  # Commit changes to save users into the database
