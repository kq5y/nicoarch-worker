from bson import ObjectId
from datetime import datetime

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

    def get_task(self, task_id: str):
        return self.tasks.find_one({
            "_id": ObjectId(task_id)
        })

    def get_video(self, watch_id: str):
        return self.videos.find_one({
            "watchId": watch_id
        })

    def get_latest_comment(self, video_id, fork: str):
        return self.comments.find_one({
            "videoId": ObjectId(str(video_id)),
            "fork": fork
        }, sort=[("no", -1)])

    def update_task_status(self, task_id: str, status: str, *, additional: dict = {}) -> dict:
        return self.tasks.find_one_and_update({
            "_id": ObjectId(str(task_id))
        }, {"$set": {
            "status": status,
            "updatedAt": datetime.now(),
            **additional
        }})

    def update_user(self, user_id: int, data: dict):
        return self.users.update_one({
            "userId": user_id
        }, {"$set": {
            "updatedAt": datetime.now(),
            **data
        }})

    def update_video(self, watch_id: str, data: dict) -> dict:
        return self.videos.find_one_and_update({
            "watchId": watch_id
        }, {"$set": {
            "updatedAt": datetime.now(),
            **data
        }})

    def insert_user(self, user: dict):
        return self.users.insert_one({
            **user,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        })

    def insert_video(self, video: dict):
        return self.videos.insert_one({
            **video,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        })

    def replace_video(self, watch_id: str, video: dict):
        return self.videos.replace_one({
            "watchId": watch_id
        }, video)

    def delete_video(self, watch_id: str):
        return self.videos.delete_one({
            "watchId": watch_id
        })

    def insert_comments(self, comments: list[dict]):
        return self.comments.insert_many(comments)

    def delete_comments(self, video_id, start_time):
        return self.comments.delete_many({
            "videoId": ObjectId(str(video_id)),
            "createdAt": {"$gte": start_time}
        })

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
