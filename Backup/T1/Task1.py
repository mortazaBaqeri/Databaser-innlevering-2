import time
from DbConnector import DbConnector
from Backup.T1.UserManager import UserManager
from Backup.T1.ActivityManager import ActivityManager
from Backup.T1.DataFetcher import DataFetcher
from Backup.T2.Queries import QA  # Import the QuestionSolver class
from Constants import USER_TABLE, ACTIVITY_TABLE, TRACKPOINT_TABLE, TABLE_CREATION_ORDER, TABLE_DELETION_ORDER

def time_execution(task_name, func, *args, **kwargs):
    """Utility function to measure the execution time of a given task."""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    total_minutes = (end_time - start_time) / 60
    print(f"{task_name} - Total execution time: {total_minutes:.2f} minutes")
    return result

def main():
    program = None
    try:
        # Database connection
        program = DbConnector()
        
        # Managers
        user_manager = UserManager(program.cursor, program.db_connection)
        activity_manager = ActivityManager(program.cursor, program.db_connection)
        data_fetcher = DataFetcher(program.cursor)
        qa = QA(program.cursor)  # Instantiate the QuestionSolver with the cursor

        # Drop tables if they exist in the correct order to avoid foreign key issues
        for table in TABLE_DELETION_ORDER:
            data_fetcher.cursor.execute(f"SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS {table}; SET FOREIGN_KEY_CHECKS=1;", multi=True)
        
        # Create tables in the correct order to respect dependencies
        for table in TABLE_CREATION_ORDER:
            if table == USER_TABLE:
                user_manager.create_user_table()
            elif table == ACTIVITY_TABLE:
                activity_manager.create_activity_table()
            elif table == TRACKPOINT_TABLE:
                activity_manager.create_trackpoint_table()

        # Execute tasks with time tracking
        time_execution("Insert Users", user_manager.insert_users, data_dir="./Dataset/dataset/Data")
        time_execution("Insert Activities", activity_manager.insert_activities, data_dir="./Dataset/dataset/Data")

        # Question Solver - Fetch counts of Users, Activities, and Trackpoints
        time_execution("Fetch Entity Counts", qa.count_entities)

        # Fetch and display data from tables
        for table in [USER_TABLE, ACTIVITY_TABLE, TRACKPOINT_TABLE]:
            time_execution(f"Fetch Data from {table}", data_fetcher.fetch_data, table)
        
        time_execution("Show Tables", data_fetcher.show_tables)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.close_connection()

if __name__ == '__main__':
    time_execution("Entire Program", main)
