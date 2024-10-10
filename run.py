import os
from trackpoint import Trackpoint
from task_1 import Tasks


class Run:
    def __init__(self):
        self.trackpoint = Trackpoint()
        self.task = Tasks()

    def run(self, data_dir):

        # Trackpoints
        self.task.drop_table("Trackpoint")
        self.trackpoint.create_trackpoint_table()
        self.trackpoint.insert_trackpoints(data_dir)


if __name__ == '__main__':
    data_dir = "./Dataset/dataset/Data"
    run = Run()
    run.run(data_dir)

