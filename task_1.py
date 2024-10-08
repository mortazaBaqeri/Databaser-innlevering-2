from DbConnector import DbConnector
from tabulate import tabulate
import os

class Tasks:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

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
                     id INT PRIMARY KEY,
                     user_id INT,
                     transportation_mode VARCHAR(255),
                     start_date_time DATETIME,
                     end_date_time DATETIME,
                     FOREIGN KEY (user_id) REFERENCES User(id));          
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    # def insert_data(self, table_name):
    #     # Define the path to the 'labeled_ids.txt' file
    #     file_path = os.path.join( './dataset/dataset/labeled_ids.txt')
        
    #     if not os.path.exists(file_path):
    #         print(f"File not found: {file_path}")
    #         return

    #     with open(file_path, 'r') as file:
    #         # Assuming the file contains names (one per line)
    #         lines = file.readlines()

    #     # Strip whitespace characters like \n from each line
    #     names = [line.strip() for line in lines]

    #     for name in names:
    #         query = "INSERT INTO %s (name) VALUES ('%s')"
    #         query = query.replace("%s", table_name).replace("%s", name)
    #         self.cursor.execute(query)

    #     self.db_connection.commit()
    #     print(f"Data from {file_path} inserted successfully.")



    def insert_data_from_folders(self, root_path, table_name='User'):
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

def main():
    program = None
    try:
        program = Tasks()
            
        program.drop_table('User')
        program.create_table('User')
        program.insert_data_from_folders('./dataset/dataset/')
        program.update_user_table('./dataset/dataset/labeled_ids.txt')
        program.create_activity_table()

        # fetch_choice = input(f"Do you want to fetch data from {table_name}? (yes/no): ")
        # if fetch_choice.lower() == 'yes':
        #     _ = program.fetch_data(table_name=table_name)

        # drop_choice = input(f"Do you want to drop the table {table_name}? (yes/no): ")
        # if drop_choice.lower() == 'yes':
        #     program.drop_table(table_name=table_name)

        # Showing the remaining tables in the database
        # show_choice = input("Do you want to list the current tables? (yes/no): ")
        # if show_choice.lower() == 'yes':
        #     program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use the database:", e)

    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
