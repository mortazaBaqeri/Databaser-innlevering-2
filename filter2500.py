def has_more_than_2500_lines(plt_file_path):
    with open(plt_file_path, 'r') as file:
        # Skip the first 6 lines
        for _ in range(6):
            next(file, None)
        # Start counting from line 7
        return sum(1 for _ in file) > 2500


# sjekket filnavnet til .plt "2023898989"




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


    # # Function to process the .plt file and get the first and last timestamps
    # def get_plt_start_end_time(self, plt_file_path):
    #     with open(plt_file_path, 'r') as plt_file:
    #         lines = plt_file.readlines()[6:]  # Skipping the first 6 header lines

    #     # Extract the start and end times from the .plt file
    #     start_time = lines[0].strip().split(',')[-1]
    #     end_time = lines[-1].strip().split(',')[-1]

    #     # Convert to datetime
    #     start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    #     end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

    #     return start_time, end_time

    # Function to check if a .plt file has more than 2500 lines


       # # Function to insert and match activities by traversing the dataset and matching transportation modes
    # def insert_and_match_activities(self, base_path, labeled_ids_path):
    #     # Load the labeled_ids.txt to know which users have transportation modes
    #     with open(labeled_ids_path, 'r') as f:
    #         labeled_ids = [line.strip() for line in f.readlines()]

    #     # Traverse the dataset using os.walk() to find all .plt files and labels.txt
    #     for root, dirs, files in os.walk(base_path):
    #         # Extract the user folder name (user_id)
    #         user_id = os.path.basename(root)
    #         print(f"Processing user {user_id}...")

    #         # Skip if user is not in labeled_ids.txt
    #         if user_id not in labeled_ids:
    #             continue

    #         # Look for labels.txt in the current user's directory
    #         labels = None
    #         if "labels.txt" in files:
    #             labels_path = os.path.join(root, "labels.txt")
    #             print(f"labels path: ${labels_path}")
                
    #             # Load the labels.txt
    #             labels = pd.read_csv(labels_path, delim_whitespace=True, header=None,
    #                                  names=['start_time', 'end_time', 'transportation_mode'])
                
    #             print(f"labels ${labels}")
                
    #             # Convert the start_time and end_time in labels.txt to the .plt format
    #             labels['start_time_fmt'] = labels['start_time'].apply(self.format_time_string)
    #             labels['end_time_fmt'] = labels['end_time'].apply(self.format_time_string)

    #         # Now check for .plt files in the Trajectory directory
    #         trajectory_path = os.path.join(root, "Trajectory")
    #         if os.path.exists(trajectory_path):
    #             for plt_file in os.listdir(trajectory_path):
    #                 if plt_file.endswith(".plt"):
    #                     plt_file_path = os.path.join(trajectory_path, plt_file)

    #                     # Skip files with more than 2500 lines
    #                     if self.has_more_than_2500_lines(plt_file_path):
    #                         print(f"Skipping {plt_file} for user {user_id}: More than 2500 lines.")
    #                         continue

    #                     # Get the start and end times from the .plt file
    #                     start_time, end_time = self.get_plt_start_end_time(plt_file_path)

    #                     transportation_mode = None  # Default to None (NULL in SQL)

    #                     # If labels exist, check if the .plt matches any start and end times
    #                     if labels is not None:
    #                         plt_file_name = plt_file[:-4]  # Remove the .plt extension
    #                         matched_label = labels[(labels['start_time_fmt'] == plt_file_name) & 
    #                                                (labels['end_time_fmt'] == plt_file_name)]

    #                         if not matched_label.empty:
    #                             transportation_mode = matched_label['transportation_mode'].values[0]

    #                     # Insert the activity into the Activity table
    #                     self.insert_activity(user_id=user_id,
    #                                          transportation_mode=transportation_mode,
    #                                          start_time=start_time,
    #                                          end_time=end_time)

    #                     print(f"Inserted activity for user {user_id}, file {plt_file}, transport: {transportation_mode}")

    
    
    # 1: Åpne fil dataset/dataset/Data/010
    # 2: Les gjennom alle .plt mappe under Trajectory
    # 3. Åpne filen labled_ids.txt
    # 4. Ha en løkke som leser start_time og se om det matcher med en av .plt filnavnene i Trajectory filen 