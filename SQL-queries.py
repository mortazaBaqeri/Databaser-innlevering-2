import datetime
import math
from DbConnector import DbConnector
from datetime import datetime
import os

class QA:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # Task 2.1
    def count_entities(self):
        user_count_query = "SELECT COUNT(*) AS user_count FROM User;"
        self.cursor.execute(user_count_query)
        user_count = self.cursor.fetchone()[0]

        activity_count_query = "SELECT COUNT(*) AS activity_count FROM Activity;"
        self.cursor.execute(activity_count_query)
        activity_count = self.cursor.fetchone()[0]

        trackpoint_count_query = "SELECT COUNT(*) AS trackpoint_count FROM Trackpoint;"
        self.cursor.execute(trackpoint_count_query)
        trackpoint_count = self.cursor.fetchone()[0]

        print(f"Number of Users: {user_count}")
        print(f"Number of Activities: {activity_count}")
        print(f"Number of Trackpoints: {trackpoint_count}")

    # Task 2.2
    def average_activities_per_user(self):
        user_count_query = "SELECT COUNT(*) AS user_count FROM User;"
        self.cursor.execute(user_count_query)
        user_count = self.cursor.fetchone()[0]

        activity_count_query = "SELECT COUNT(*) AS activity_count FROM Activity;"
        self.cursor.execute(activity_count_query)
        activity_count = self.cursor.fetchone()[0]

        # Calculate the average number of activities per user
        if user_count > 0:
            average_activities = activity_count / user_count
        else:
            average_activities = 0  # To handle division by zero
        
        print(f"Average Number of Activities per User: {average_activities:.2f}")

    # Task 2.3
    def top_20_users_by_activity(self):
        # Query to find the top 20 users by the number of activities
        top_users_query = """
        SELECT user_id, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20;
        """
        self.cursor.execute(top_users_query)
        top_users = self.cursor.fetchall()

        print("Top 20 Users by Number of Activities:")
        print("--------------------------------------")
        for rank, (user_id, activity_count) in enumerate(top_users, start=1):
            print(f"{rank}. User ID: {user_id}, Activities: {activity_count}")

    # Task 2.4
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

    # Task 2.5
    def count_transportation_modes(self):
        # Query to find all types of transportation modes and count the activities for each mode
        transportation_mode_query = """
        SELECT transportation_mode, COUNT(*) AS activity_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode;
        """
        self.cursor.execute(transportation_mode_query)
        transportation_modes = self.cursor.fetchall()

        print("Transportation Modes and Activity Counts:")
        print("-----------------------------------------")
        for mode, count in transportation_modes:
            print(f"Transportation Mode: {mode}, Activities: {count}")

    # Task 2.6.a)
    def year_with_most_activities(self):
        # Query to find the year with the most activities
        year_query = """
        SELECT YEAR(start_date_time) AS year, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY year
        ORDER BY activity_count DESC
        LIMIT 1;
        """
        self.cursor.execute(year_query)
        result = self.cursor.fetchone()

        year, activity_count = result
        print(f"Year with the Most Activities: {year} ({activity_count} activities)")
        return year  

    
    def year_with_most_recorded_hours(self):
        # Query to find the year with the most recorded hours
        hours_query = """
        SELECT YEAR(start_date_time) AS year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM Activity
        GROUP BY year
        ORDER BY total_hours DESC
        LIMIT 1;
        """
        self.cursor.execute(hours_query)
        result = self.cursor.fetchone()

        year, total_hours = result
        print(f"Year with the Most Recorded Hours: {year} ({total_hours} hours)")
        return year  

    # Task 2.6.b)
    def compare_years_for_most_activities_and_hours(self):
        year_most_activities = self.year_with_most_activities()
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

    # Task 2.7
    def total_distance_walked_2008_user_112(self):
        # SQL Query to get trackpoints for user 112, walking activities in 2008
        query = """
        SELECT latitude, longitude, date_time, activity_id
        FROM Trackpoint
        JOIN Activity ON Activity.id = Trackpoint.activity_id
        WHERE Activity.user_id = '112' 
          AND Activity.transportation_mode = 'walk'
          AND YEAR(Activity.start_date_time) = 2008
        ORDER BY activity_id, date_time;
        """
        
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        total_distance = 0.0
        previous_point = None

        for trackpoint in trackpoints:
            latitude, longitude, date_time, activity_id = trackpoint

            if previous_point is None:
                previous_point = (latitude, longitude)
                continue

            prev_lat, prev_lon = previous_point
            distance = self.haversine(prev_lat, prev_lon, latitude, longitude)
            total_distance += distance

            previous_point = (latitude, longitude)

        print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")


    
    def top_20_users_by_altitude_gain(self):
        # SQL Query to get valid trackpoints, ordered by user_id, activity_id, and date_time
        query = """
        SELECT user_id, altitude, activity_id, date_time
        FROM Trackpoint
        JOIN Activity ON Activity.id = Trackpoint.activity_id
        WHERE altitude != -777
        ORDER BY user_id, activity_id, date_time;
        """
        
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()
        altitude_gain_per_user = {}
        previous_point = None
        current_user = None
        for trackpoint in trackpoints:
            user_id, altitude, activity_id, date_time = trackpoint
            if user_id != current_user:
                previous_point = None
                current_user = user_id

            if previous_point is None:
                previous_point = altitude
                continue

            if altitude > previous_point:
                gain = altitude - previous_point
                if user_id not in altitude_gain_per_user:
                    altitude_gain_per_user[user_id] = 0
                altitude_gain_per_user[user_id] += gain

            previous_point = altitude
        sorted_users_by_gain = sorted(altitude_gain_per_user.items(), key=lambda x: x[1], reverse=True)
        top_20_users = sorted_users_by_gain[:20]
        print("Top 20 Users by Altitude Gain:")
        print("--------------------------------")
        print(f"{'User ID':<10} {'Total Altitude Gain (m)':<20}")
        for user_id, total_gain in top_20_users:
            print(f"{user_id:<10} {total_gain:<20.2f}")


    def find_users_with_invalid_activities(self):
        # SQL Query to get trackpoints, ordered by user_id, activity_id, and date_time
        query = """
        SELECT a.user_id, COUNT(a.id) AS invalid_activity_count
        FROM Activity a
        JOIN Trackpoint tp1 ON a.id = tp1.activity_id
        JOIN Trackpoint tp2 ON tp1.id = tp2.id - 1 AND tp1.activity_id = tp2.activity_id
        WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5
        GROUP BY a.user_id
        HAVING invalid_activity_count > 0;
        """
        
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()
        invalid_activities_per_user = {}
        previous_point = None
        current_activity = None
        current_user = None
        invalid_activity_ids = set()  # To store invalid activities that have already been flagged

        for trackpoint in trackpoints:
            user_id, activity_id, date_time = trackpoint
            date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
            if activity_id != current_activity or user_id != current_user:
                previous_point = date_time  # Set the first point in the new activity
                current_activity = activity_id
                current_user = user_id
                continue
            time_difference = (date_time - previous_point).total_seconds() / 60.0  # Time difference in minutes
            if time_difference >= 5:
                if activity_id not in invalid_activity_ids:
                    invalid_activity_ids.add(activity_id)
                    if user_id not in invalid_activities_per_user:
                        invalid_activities_per_user[user_id] = 0
                    invalid_activities_per_user[user_id] += 1
            previous_point = date_time
        print("Users with Invalid Activities:")
        print("-------------------------------")
        print(f"{'User ID':<10} {'Invalid Activities':<20}")
        for user_id, invalid_count in invalid_activities_per_user.items():
            print(f"{user_id:<10} {invalid_count:<20}")

    def find_users_with_invalid_activities(self):
        # SQL Query to get the count of invalid activities per user
        query = """
        SELECT a.user_id, COUNT(DISTINCT a.id) AS invalid_activity_count
        FROM Activity a
        JOIN Trackpoint tp1 ON a.id = tp1.activity_id
        JOIN Trackpoint tp2 ON tp1.id = tp2.id - 1 AND tp1.activity_id = tp2.activity_id
        WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) >= 5
        GROUP BY a.user_id
        HAVING invalid_activity_count > 0;
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        print("Users with Invalid Activities:")
        print("-------------------------------")
        print(f"{'User ID':<10} {'Invalid Activities':<20}")
        for user_id, invalid_count in results:
            print(f"{user_id:<10} {invalid_count:<20}")

    def find_users_in_forbidden_city(self):
        # Define the Forbidden City coordinates
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        threshold_distance_km = 0.1  # 100 meters in kilometers

        # SQL Query to get all trackpoints
        query = """
        SELECT user_id, latitude, longitude, activity_id
        FROM Trackpoint
        JOIN Activity ON Activity.id = Trackpoint.activity_id;
        """
        
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        users_in_forbidden_city = set()

        for trackpoint in trackpoints:
            user_id, latitude, longitude, activity_id = trackpoint

            distance = self.haversine(latitude, longitude, forbidden_city_lat, forbidden_city_lon)

            if distance <= threshold_distance_km:
                users_in_forbidden_city.add(user_id)

        print("Users who have tracked activities in the Forbidden City:")
        print("--------------------------------------------------------")
        for user_id in users_in_forbidden_city:
            print(f"User ID: {user_id}")


    def find_users_most_used_transportation_mode(self):
        # SQL Query to get transportation mode counts for each user, excluding NULL and empty values
        query = """
        SELECT user_id, transportation_mode, COUNT(*) AS mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL AND transportation_mode != ''
        GROUP BY user_id, transportation_mode
        ORDER BY user_id;
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        user_transportation_modes = {}

        for row in results:
            user_id, transportation_mode, mode_count = row

            if user_id not in user_transportation_modes:
                user_transportation_modes[user_id] = (transportation_mode, mode_count)
            else:
                _, current_max_count = user_transportation_modes[user_id]
                if mode_count > current_max_count:
                    user_transportation_modes[user_id] = (transportation_mode, mode_count)
        
        sorted_users = sorted(user_transportation_modes.items())

        print("Users and Their Most Used Transportation Mode:")
        print("-------------------------------------------------")
        print(f"{'User ID':<10} {'Most Used Transportation Mode':<30}")
        for user_id, (most_used_mode, _) in sorted_users:
            # Only print users with a valid transportation mode
            if most_used_mode and most_used_mode.strip().lower() != 'null':
                print(f"{user_id:<10} {most_used_mode:<30}")


def main():
    program = None
    try:
        program = QA()

        while True:
            width = os.get_terminal_size().columns 
            print('-' * width)
            print("\nSelect a task to run:")
            print("1. Count Entities (Task 2.1)")
            print("2. Average Activities per User (Task 2.2)")
            print("3. Top 20 Users by Activity (Task 2.3)")
            print("4. Find Users Who Took a Taxi (Task 2.4)")
            print("5. Count Transportation Modes (Task 2.5)")
            print("6. Year with Most Activities (Task 2.6a)")
            print("7. Year with Most Recorded Hours (Task 2.6a)")
            print("8. Compare Years for Most Activities and Hours (Task 2.6b)")
            print("9. Total Distance Walked in 2008 by User 112 (Task 2.7)")
            print("10. Top 20 Users by Altitude Gain (Task 2.8)")
            print("11. Find Users with Invalid Activities (Task 2.9)")
            print("12. Find Users in Forbidden City (Task 2.10)")
            print("13. Find Users' Most Used Transportation Mode (Task 2.11)")
            print("0. Exit")

            choice = input("\nEnter the number of the task to run: ")
            width = os.get_terminal_size().columns 
            print('-' * width)

            if choice == "1":
                program.count_entities()
            elif choice == "2":
                program.average_activities_per_user()
            elif choice == "3":
                program.top_20_users_by_activity()
            elif choice == "4":
                program.find_users_who_took_taxi()
            elif choice == "5":
                program.count_transportation_modes()
            elif choice == "6":
                program.year_with_most_activities()
            elif choice == "7":
                program.year_with_most_recorded_hours()
            elif choice == "8":
                program.compare_years_for_most_activities_and_hours()
            elif choice == "9":
                program.total_distance_walked_2008_user_112()
            elif choice == "10":
                program.top_20_users_by_altitude_gain()
            elif choice == "11":
                program.find_users_with_invalid_activities()
            elif choice == "12":
                program.find_users_in_forbidden_city()
            elif choice == "13":
                program.find_users_most_used_transportation_mode()
            elif choice == "0":
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please try again.")

    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
