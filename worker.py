import os
import uuid
import time
from datetime import datetime
import requests

from niconico import NicoNico
from niconico.exceptions import CommentAPIError
from pymongo import MongoClient
from redis.client import Redis
from bson.objectid import ObjectId

MONGO_URL = os.environ.get('MONGO_URL')
REDIS_URL = os.environ.get('REDIS_URL')
NICONICO_MAIL = os.environ.get('NICONICO_MAIL')
NICONICO_PASSWORD = os.environ.get('NICONICO_PASSWORD')

if not MONGO_URL:
    raise ValueError('MONGO_URL is not set')
if not REDIS_URL:
    raise ValueError('REDIS_URL is not set')
if not NICONICO_MAIL:
    raise ValueError('NICONICO_MAIL is not set')
if not NICONICO_PASSWORD:
    raise ValueError('NICONICO_PASSWORD is not set')

if os.path.exists("/contents") is False:
    os.makedirs("/contents")
if os.path.exists("/contents/image") is False:
    os.makedirs("/contents/image")
if os.path.exists("/contents/image/icon") is False:
    os.makedirs("/contents/image/icon")
if os.path.exists("/contents/image/thumbnail") is False:
    os.makedirs("/contents/image/thumbnail")
if os.path.exists("/contents/video") is False:
    os.makedirs("/contents/video")

niconico_client = NicoNico()
niconico_client.login_with_mail(NICONICO_MAIL, NICONICO_PASSWORD)

redis_client = Redis.from_url(REDIS_URL)

mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client.get_database("nicoarch")
mongo_tasks = mongo_db.get_collection("tasks")
mongo_videos = mongo_db.get_collection("videos")
mongo_users = mongo_db.get_collection("users")
mongo_comments = mongo_db.get_collection("comments")

def fetch(task_id):
    task = mongo_tasks.find_one_and_update({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "fetching"
    }})
    watchId = task.get("watchId")
    watchUUID = uuid.uuid3(uuid.NAMESPACE_URL, watchId)
    videoData = niconico_client.video.get_video(watchId)
    if videoData is None:
        raise ValueError("Video not found")
    watchData = niconico_client.video.watch.get_watch_data(watchId)
    userData = niconico_client.user.get_user(str(watchData.owner.id_))
    ownerId = None
    if userData is not None:
        userUUID = uuid.uuid3(uuid.NAMESPACE_URL, str(userData.id_))
        user_res = mongo_users.insert_one({
            "userId": userData.id_,
            "nickname": userData.nickname,
            "description": userData.description,
            "registeredVersion": userData.registered_version,
            "contentId": str(userUUID)
        })
        with open(f'/contents/image/icon/{str(userUUID)}.jpg', 'wb') as f:
            b = requests.get(userData.icons.large)
            f.write(b.content)
        ownerId = user_res.inserted_id
    video = mongo_videos.insert_one({
        "title": watchData.video.title,
        "watchId": watchId,
        "registeredAt": watchData.video.registered_at,
        "count": {
            "view": watchData.video.count.view,
            "comment": watchData.video.count.comment,
            "mylist": watchData.video.count.mylist,
            "like": watchData.video.count.like
        },
        "ownerId": ownerId,
        "duration": watchData.video.duration,
        "description": watchData.video.description,
        "shortDescription": videoData.short_description,
        "taskId": ObjectId(task_id),
        "contentId": str(watchUUID)
    })
    return watchData, watchUUID, video.inserted_id

def download(task_id, watchData, watchUUID, videoId):
    mongo_tasks.update_one({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "downloading",
        "videoId": videoId
    }})
    with open(f'/contents/image/thumbnail/{str(watchUUID)}.jpg', 'wb') as f:
        b = requests.get(watchData.video.thumbnail.ogp)
        f.write(b.content)
    outputs = niconico_client.video.watch.get_outputs(watchData)
    best_output = next(iter(outputs))
    niconico_client.video.watch.download_video(watchData, best_output, "/contents/video/"+str(watchUUID)+".%(ext)s")

def insert_comments(comments, videoId, threadId, threadFork):
    if len(comments) <= 0:
        return
    mongo_comments.insert_many([{
        "commentId": comment.id_,
        "body": comment.body,
        "commands": comment.commands,
        "isPremium": comment.is_premium,
        "nicoruCount": comment.nicoru_count,
        "no": comment.no,
        "postedAt": comment.posted_at,
        "score": comment.score,
        "source": comment.source,
        "userId": comment.user_id,
        "vposMs": comment.vpos_ms,
        "videoId": videoId,
        "threadId": threadId,
        "fork": threadFork,
    } for comment in comments])

def getting_comments(task_id, watchData, videoId):
    mongo_tasks.update_one({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "comment"
    }})
    when_unix = int(time.time())
    main_min_no = 0
    easy_min_no = 0
    owner_comments_fecthed = False
    is_finished = False
    comment_count = 0
    failed_count = 0
    thread_key = None
    while not is_finished:
        comment_res = None
        try:
            comment_res = niconico_client.video.watch.get_comments(watchData, when=when_unix, thread_key=thread_key)
        except CommentAPIError as e:
            if e.message == "EXPIRED_TOKEN":
                thread_key = niconico_client.video.watch.get_thread_key(videoId)
                time.sleep(1)
                continue
        if comment_res is None:
            if failed_count >= 5:
                raise ValueError("Failed to get comments")
            failed_count += 1
            time.sleep(60)
            continue
        for thread in comment_res.threads:
            if thread.fork == "owner":
                if owner_comments_fecthed:
                    continue
                owner_comments_fecthed = True
                comment_count += len(thread.comments)
                insert_comments(thread.comments, videoId, thread.id_, thread.fork)
            elif thread.fork == "easy":
                if easy_min_no == 0:
                    easy_min_no = thread.comments[-1].no + 1
                comment_index = len(thread.comments) - 1
                while comment_index >= 0:
                    if thread.comments[comment_index].no < easy_min_no:
                        break
                    comment_index -= 1
                comments = thread.comments[:comment_index+1]
                if len(comments) <= 0:
                    continue
                comment_count += len(comments)
                insert_comments(comments, videoId, thread.id_, thread.fork)
                easy_min_no = thread.comments[0].no
            else:
                if main_min_no == 0:
                    main_min_no = thread.comments[-1].no + 1
                comment_index = len(thread.comments) - 1
                while comment_index >= 0:
                    if thread.comments[comment_index].no < main_min_no:
                        break
                    comment_index -= 1
                comments = thread.comments[:comment_index+1]
                if len(comments) <= 0:
                    is_finished = True
                    continue
                comment_count += len(comments)
                insert_comments(comments, videoId, thread.id_, thread.fork)
                main_min_no = thread.comments[0].no
                when_unix = int(datetime.fromisoformat(thread.comments[0].posted_at).timestamp())
        mongo_tasks.update_one({
            "_id": ObjectId(task_id)
        }, {"$set": {
            "commentCount": comment_count
        }})
        time.sleep(1)
    return comment_count


def finish(task_id):
    mongo_tasks.update_one({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "completed"
    }})

def error(task_id, e):
    mongo_tasks.update_one({
        "_id": ObjectId(task_id)
    }, {"$set": {
        "status": "failed",
        "error": str(e)
    }})
    mongo_videos.delete_one({
        "taskId": ObjectId(task_id)
    })

def main():
    print("nicoarch worker started")
    while True:
        task = redis_client.lpop("tasks")
        if task is None:
            time.sleep(10)
            continue
        task_id = task.decode("utf-8")
        try:
            print(f"Fetching task {task_id}")
            watchData, watchUUID, videoId = fetch(task_id)
            print(f"Downloading task {task_id}")
            download(task_id, watchData, watchUUID, videoId)
            print(f"Getting Comments task {task_id}")
            getting_comments(task_id, watchData, videoId)
            print(f"Finishing task {task_id}")
            finish(task_id)
        except Exception as e:
            print(f"Error task {task_id}", e)
            error(task_id, e)

if __name__ == "__main__":
    main()
