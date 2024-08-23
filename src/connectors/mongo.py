from bson import ObjectId

from pymongo import MongoClient
from pymongo.database import Database


class MongoConnector:

    client: MongoClient
    db: Database

    def __init__(self, url: str):
        self.client = MongoClient(url)
        self.db = self.client.get_database("nicoarch")
        self.tasks = self.db.get_collection("tasks")
        self.videos = self.db.get_collection("videos")
        self.users = self.db.get_collection("users")
        self.comments = self.db.get_collection("comments")

    def update_task_status(self, task_id: str, status: str, additional: dict = {}) -> dict:
        task = self.tasks.find_one_and_update({
            "_id": ObjectId(task_id)
        }, {"$set": {
            "status": status,
            **additional
        }})
        return task

    def insert_user(self, user: dict):
        return self.users.insert_one(user)

    def insert_video(self, video: dict):
        return self.videos.insert_one(video)

    def insert_comments(self, comments: list[dict]):
        return self.comments.insert_many(comments)

    def delete_video(self, task_id: str):
        return self.videos.delete_one({
            "taskId": ObjectId(task_id)
        })

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
