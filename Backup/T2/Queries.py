
class QA:
    def __init__(self, cursor):
        # Initialize with a cursor to execute queries
        self.cursor = cursor

    def count_entities(self):
        # Query to count the number of users
        user_count_query = "SELECT COUNT(*) AS user_count FROM User;"
        self.cursor.execute(user_count_query)
        user_count = self.cursor.fetchone()[0]

        # Query to count the number of activities
        activity_count_query = "SELECT COUNT(*) AS activity_count FROM Activity;"
        self.cursor.execute(activity_count_query)
        activity_count = self.cursor.fetchone()[0]

        # Query to count the number of trackpoints
        trackpoint_count_query = "SELECT COUNT(*) AS trackpoint_count FROM TrackPoint;"
        self.cursor.execute(trackpoint_count_query)
        trackpoint_count = self.cursor.fetchone()[0]

        # Print the counts
        print(f"Number of Users: {user_count}")
        print(f"Number of Activities: {activity_count}")
        print(f"Number of Trackpoints: {trackpoint_count}")


        
