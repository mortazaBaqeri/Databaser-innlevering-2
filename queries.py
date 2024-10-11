import datetime
import math

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

    def average_activities_per_user(self):
        # Get the number of users
        user_count_query = "SELECT COUNT(*) AS user_count FROM User;"
        self.cursor.execute(user_count_query)
        user_count = self.cursor.fetchone()[0]

        # Get the number of activities
        activity_count_query = "SELECT COUNT(*) AS activity_count FROM Activity;"
        self.cursor.execute(activity_count_query)
        activity_count = self.cursor.fetchone()[0]

        # Calculate the average number of activities per user
        if user_count > 0:
            average_activities = activity_count / user_count
        else:
            average_activities = 0  # To handle division by zero

        # Print the result
        print(f"Average Number of Activities per User: {average_activities:.2f}")

    def top_20_users_by_activity(self):
        # Query to find the top 20 users by the number of activities
        top_users_query = """
        SELECT user_id, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20;
        """
        # Execute the query
        self.cursor.execute(top_users_query)
        top_users = self.cursor.fetchall()

        # Print the top 20 users with the highest number of activities
        print("Top 20 Users by Number of Activities:")
        print("--------------------------------------")
        for rank, (user_id, activity_count) in enumerate(top_users, start=1):
            print(f"{rank}. User ID: {user_id}, Activities: {activity_count}")

    def find_users_who_took_taxi(self):
        # Query to find all users who have taken a taxi
        taxi_users_query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'taxi';
        """
        # Execute the query
        self.cursor.execute(taxi_users_query)
        taxi_users = self.cursor.fetchall()

        # Print the list of users who have taken a taxi
        print("Users Who Have Taken a Taxi:")
        print("-----------------------------")
        for user in taxi_users:
            print(f"User ID: {user[0]}")

    def count_transportation_modes(self):
        # Query to find all types of transportation modes and count the activities for each mode
        transportation_mode_query = """
        SELECT transportation_mode, COUNT(*) AS activity_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode;
        """
        # Execute the query
        self.cursor.execute(transportation_mode_query)
        transportation_modes = self.cursor.fetchall()

        # Print the transportation modes and their counts
        print("Transportation Modes and Activity Counts:")
        print("-----------------------------------------")
        for mode, count in transportation_modes:
            print(f"Transportation Mode: {mode}, Activities: {count}")

    def year_with_most_activities(self):
        # Query to find the year with the most activities
        year_query = """
        SELECT YEAR(start_date_time) AS year, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY year
        ORDER BY activity_count DESC
        LIMIT 1;
        """
        # Execute the query
        self.cursor.execute(year_query)
        result = self.cursor.fetchone()

        # Store the year and print the result
        year, activity_count = result
        print(f"Year with the Most Activities: {year} ({activity_count} activities)")
        return year  # Return the year for further use

    def year_with_most_recorded_hours(self):
        # Query to find the year with the most recorded hours
        hours_query = """
        SELECT YEAR(start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM Activity
        GROUP BY year
        ORDER BY total_hours DESC
        LIMIT 1;
        """
        # Execute the query
        self.cursor.execute(hours_query)
        result = self.cursor.fetchone()

        # Store the year and total hours and print the result
        year, total_hours = result
        print(f"Year with the Most Recorded Hours: {year} ({total_hours} hours)")
        return year  # Return the year for comparison

    def compare_years_for_most_activities_and_hours(self):
        # Find the year with the most activities
        year_most_activities = self.year_with_most_activities()

        # Find the year with the most recorded hours
        year_most_hours = self.year_with_most_recorded_hours()

        # Compare the years and print the conclusion
        if year_most_activities == year_most_hours:
            print("The year with the most activities is also the year with the most recorded hours.")
        else:
            print("The year with the most activities is NOT the year with the most recorded hours.")

    
    def haversine(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great-circle distance between two points on the Earth's surface.
        Arguments are latitude and longitude in decimal degrees.
        Returns distance in kilometers.
        """
        R = 6371  # Radius of Earth in kilometers
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda / 2.0) ** 2

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def total_distance_walked_2008_user_112(self):
        # SQL Query to get trackpoints for user 112, walking activities in 2008
        query = """
        SELECT latitude, longitude, date_time, activity_id
        FROM TrackPoint
        JOIN Activity ON Activity.id = TrackPoint.activity_id
        WHERE Activity.user_id = '112' 
          AND Activity.transportation_mode = 'walk'
          AND YEAR(Activity.start_date_time) = 2008
        ORDER BY activity_id, date_time;
        """
        
        # Execute the query
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        # Calculate total distance walked
        total_distance = 0.0
        previous_point = None

        for trackpoint in trackpoints:
            latitude, longitude, date_time, activity_id = trackpoint

            if previous_point is None:
                # This is the first point, just set it as previous and continue
                previous_point = (latitude, longitude)
                continue

            # Calculate distance from the previous point to the current point
            prev_lat, prev_lon = previous_point
            distance = self.haversine(prev_lat, prev_lon, latitude, longitude)
            total_distance += distance

            # Update previous point
            previous_point = (latitude, longitude)

        # Print the total distance walked
        print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")


    
    def top_20_users_by_altitude_gain(self):
        # SQL Query to get valid trackpoints, ordered by user_id, activity_id, and date_time
        query = """
        SELECT user_id, altitude, activity_id, date_time
        FROM TrackPoint
        JOIN Activity ON Activity.id = TrackPoint.activity_id
        WHERE altitude != -777
        ORDER BY user_id, activity_id, date_time;
        """
        
        # Execute the query
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        # Dictionary to store altitude gains for each user
        altitude_gain_per_user = {}
        previous_point = None
        current_user = None

        # Iterate over trackpoints to calculate altitude gains
        for trackpoint in trackpoints:
            user_id, altitude, activity_id, date_time = trackpoint

            if user_id != current_user:
                # If we encounter a new user, reset previous point and update current user
                previous_point = None
                current_user = user_id

            if previous_point is None:
                # Set the first trackpoint as the previous point and continue
                previous_point = altitude
                continue

            # Calculate altitude gain
            if altitude > previous_point:
                gain = altitude - previous_point
                # Add the gain to the user's total
                if user_id not in altitude_gain_per_user:
                    altitude_gain_per_user[user_id] = 0
                altitude_gain_per_user[user_id] += gain

            # Update the previous point to the current altitude
            previous_point = altitude

        # Sort users by total altitude gain in descending order
        sorted_users_by_gain = sorted(altitude_gain_per_user.items(), key=lambda x: x[1], reverse=True)

        # Get the top 20 users
        top_20_users = sorted_users_by_gain[:20]

        # Print the top 20 users with the highest altitude gain
        print("Top 20 Users by Altitude Gain:")
        print("--------------------------------")
        print(f"{'User ID':<10} {'Total Altitude Gain (m)':<20}")
        for user_id, total_gain in top_20_users:
            print(f"{user_id:<10} {total_gain:<20.2f}")


    def find_users_with_invalid_activities(self):
        # SQL Query to get trackpoints, ordered by user_id, activity_id, and date_time
        query = """
        SELECT user_id, activity_id, date_time
        FROM TrackPoint
        JOIN Activity ON Activity.id = TrackPoint.activity_id
        ORDER BY user_id, activity_id, date_time;
        """
        
        # Execute the query
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        # Dictionary to store the number of invalid activities per user
        invalid_activities_per_user = {}
        previous_point = None
        current_activity = None
        current_user = None
        invalid_activity_ids = set()  # To store invalid activities that have already been flagged

        # Iterate over trackpoints to determine invalid activities
        for trackpoint in trackpoints:
            user_id, activity_id, date_time = trackpoint

            # Convert the date_time string to a datetime object
            date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

            # New activity or user
            if activity_id != current_activity or user_id != current_user:
                previous_point = date_time  # Set the first point in the new activity
                current_activity = activity_id
                current_user = user_id
                continue

            # Calculate time difference from previous trackpoint
            time_difference = (date_time - previous_point).total_seconds() / 60.0  # Time difference in minutes

            # If the time difference is greater than or equal to 5 minutes, mark the activity as invalid
            if time_difference >= 5:
                if activity_id not in invalid_activity_ids:
                    # If the activity hasn't already been flagged as invalid, mark it
                    invalid_activity_ids.add(activity_id)

                    # Update the count of invalid activities for the user
                    if user_id not in invalid_activities_per_user:
                        invalid_activities_per_user[user_id] = 0
                    invalid_activities_per_user[user_id] += 1

            # Update the previous point to the current date_time
            previous_point = date_time

        # Print the users with their number of invalid activities
        print("Users with Invalid Activities:")
        print("-------------------------------")
        print(f"{'User ID':<10} {'Invalid Activities':<20}")
        for user_id, invalid_count in invalid_activities_per_user.items():
            print(f"{user_id:<10} {invalid_count:<20}")

    def find_users_in_forbidden_city(self):
        # Define the Forbidden City coordinates
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        threshold_distance_km = 0.1  # 100 meters in kilometers

        # SQL Query to get all trackpoints
        query = """
        SELECT user_id, latitude, longitude, activity_id
        FROM TrackPoint
        JOIN Activity ON Activity.id = TrackPoint.activity_id;
        """
        
        # Execute the query
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        # Set to store unique users who have trackpoints in the Forbidden City
        users_in_forbidden_city = set()

        # Iterate over trackpoints to determine if they are within the Forbidden City
        for trackpoint in trackpoints:
            user_id, latitude, longitude, activity_id = trackpoint

            # Calculate distance from the Forbidden City coordinates
            distance = self.haversine(latitude, longitude, forbidden_city_lat, forbidden_city_lon)

            # Check if the distance is within the threshold (100 meters)
            if distance <= threshold_distance_km:
                users_in_forbidden_city.add(user_id)

        # Print the users who have tracked activities in the Forbidden City
        print("Users who have tracked activities in the Forbidden City:")
        print("--------------------------------------------------------")
        for user_id in users_in_forbidden_city:
            print(f"User ID: {user_id}")


    def find_users_most_used_transportation_mode(self):
        # SQL Query to get transportation mode counts for each user
        query = """
        SELECT user_id, transportation_mode, COUNT(*) AS mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY user_id, transportation_mode
        ORDER BY user_id;
        """
        
        # Execute the query
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        # Dictionary to store the most used transportation mode for each user
        user_transportation_modes = {}

        # Iterate over results to determine the most used transportation mode per user
        for row in results:
            user_id, transportation_mode, mode_count = row

            # If user_id is not yet in the dictionary, add it with the current mode
            if user_id not in user_transportation_modes:
                user_transportation_modes[user_id] = (transportation_mode, mode_count)
            else:
                # If the user already exists, compare current mode_count with the stored one
                _, current_max_count = user_transportation_modes[user_id]
                if mode_count > current_max_count:
                    # Update if the current mode has more activities
                    user_transportation_modes[user_id] = (transportation_mode, mode_count)
                elif mode_count == current_max_count:
                    # In case of a tie, choose the current mode (could be improved for consistency)
                    user_transportation_modes[user_id] = (transportation_mode, mode_count)

        # Sort the users by user_id and print the result
        sorted_users = sorted(user_transportation_modes.items())

        # Print the result in the required format
        print("Users and Their Most Used Transportation Mode:")
        print("-------------------------------------------------")
        print(f"{'User ID':<10} {'Most Used Transportation Mode':<30}")
        for user_id, (most_used_mode, _) in sorted_users:
            print(f"{user_id:<10} {most_used_mode:<30}")


