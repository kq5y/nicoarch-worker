from bson import ObjectId

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.client_session import ClientSession


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

    def get_latest_comment(self, video_id, fork: str):
        return self.comments.find_one({
            "videoId": video_id,
            "fork": fork
        }, sort=[("no", -1)])

    def update_task_status(self, task_id: str, status: str, *, additional: dict = {}, session: ClientSession | None = None) -> dict:
        return self.tasks.find_one_and_update({
            "_id": ObjectId(task_id)
        }, {"$set": {
            "status": status,
            **additional
        }}, session=session)

    def update_user(self, user_id: int, data: dict, *, session: ClientSession | None = None):
        return self.users.update_one({
            "userId": user_id
        }, {"$set": data}, session=session)

    def update_video(self, watch_id: str, data: dict, *, session: ClientSession | None = None):
        return self.videos.update_one({
            "watchId": watch_id
        }, {"$set": data}, session=session)

    def insert_user(self, user: dict, *, session: ClientSession | None = None):
        return self.users.insert_one(user, session=session)

    def insert_video(self, video: dict, *, session: ClientSession | None = None):
        return self.videos.insert_one(video, session=session)

    def insert_comments(self, comments: list[dict], *, session: ClientSession | None = None):
        return self.comments.insert_many(comments, session=session)

    def delete_comments(self, video_id, start_time, *, session: ClientSession | None = None):
        return self.comments.delete_many({
            "videoId": video_id,
            "createdAt": {"$gte": start_time}
        }, session=session)

    def start_session(self) -> ClientSession:
        return self.client.start_session()

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
