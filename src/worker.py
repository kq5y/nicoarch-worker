import os
import uuid
import json
import time
from datetime import datetime

import requests
from bson import ObjectId

from niconico import NicoNico
from niconico.exceptions import CommentAPIError, LoginFailureError

from connectors.mongo import MongoConnector
from connectors.redis import RedisConnector


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

if os.path.exists("/app/session") is False:
    os.makedirs("/app/session")


niconico_client = NicoNico()

def login_with_mail():
    niconico_client.login_with_mail(NICONICO_MAIL, NICONICO_PASSWORD)
    with open("/app/session/nico.json", "w") as f:
        f.write(json.dumps({
            "user_session": niconico_client.get_user_session(),
        }))

if os.path.exists("/app/session/nico.json") is False:
    login_with_mail()
else:
    with open("/app/session/nico.json", "r") as f:
        session_data = json.load(f)
        user_session = session_data.get("user_session")
        if user_session is None:
            login_with_mail()
        else:
            try:
                niconico_client.login_with_session(user_session)
            except LoginFailureError:
                login_with_mail()


mongo_connector = MongoConnector(MONGO_URL)
redis_connector = RedisConnector(REDIS_URL)


def save_video_data(task_id, watch_id):
    mongo_connector.update_task_status(task_id, "fetching")
    watch_uuid = uuid.uuid4()
    video_data = niconico_client.video.get_video(watch_id)
    if video_data is None:
        raise ValueError("Video not found")
    watch_data = niconico_client.video.watch.get_watch_data(watch_id)
    user_data = niconico_client.user.get_user(str(watch_data.owner.id_))
    owner_id = None
    if user_data is not None:
        user_uuid = uuid.uuid4()
        user_res = mongo_connector.insert_user({
            "userId": user_data.id_,
            "nickname": user_data.nickname,
            "description": user_data.description,
            "registeredVersion": user_data.registered_version,
            "contentId": str(user_uuid)
        })
        with open(f'/contents/image/icon/{str(user_uuid)}.jpg', 'wb') as f:
            b = requests.get(user_data.icons.large)
            f.write(b.content)
        owner_id = user_res.inserted_id
    inseted_video = mongo_connector.insert_video({
        "title": watch_data.video.title,
        "watchId": watch_id,
        "registeredAt": watch_data.video.registered_at,
        "count": {
            "view": watch_data.video.count.view,
            "comment": watch_data.video.count.comment,
            "mylist": watch_data.video.count.mylist,
            "like": watch_data.video.count.like
        },
        "ownerId": owner_id,
        "duration": watch_data.video.duration,
        "description": watch_data.video.description,
        "shortDescription": video_data.short_description,
        "taskId": ObjectId(task_id),
        "contentId": str(watch_uuid)
    })
    return watch_data, watch_uuid, inseted_video.inserted_id


def update_video_data(task_id, watch_id):
    mongo_connector.update_task_status(task_id, "fetching")
    video_data = niconico_client.video.get_video(watch_id)
    if video_data is None:
        raise ValueError("Video not found")
    watch_data = niconico_client.video.watch.get_watch_data(watch_id)
    user_data = niconico_client.user.get_user(str(watch_data.owner.id_))
    if user_data is not None:
        user_uuid = uuid.uuid4()
        mongo_connector.update_user(user_data.id_, {
            "nickname": user_data.nickname,
            "description": user_data.description,
            "registeredVersion": user_data.registered_version
        })
        with open(f'/contents/image/icon/{str(user_uuid)}.jpg', 'wb') as f:
            b = requests.get(user_data.icons.large)
            f.write(b.content)
    updated_video = mongo_connector.update_video(watch_id, {
        "title": watch_data.video.title,
        "registeredAt": watch_data.video.registered_at,
        "count": {
            "view": watch_data.video.count.view,
            "comment": watch_data.video.count.comment,
            "mylist": watch_data.video.count.mylist,
            "like": watch_data.video.count.like
        },
        "duration": watch_data.video.duration,
        "description": watch_data.video.description,
        "shortDescription": video_data.short_description,
        "taskId": ObjectId(task_id)
    })
    return watch_data, updated_video.get("_id")


def download_video(task_id, watch_data, watch_uuid, video_id):
    mongo_connector.update_task_status(task_id, "downloading", additional={"videoId": video_id})
    with open(f'/contents/image/thumbnail/{str(watch_uuid)}.jpg', 'wb') as f:
        b = requests.get(watch_data.video.thumbnail.ogp)
        f.write(b.content)
    outputs = niconico_client.video.watch.get_outputs(watch_data)
    best_output = next(iter(outputs))
    niconico_client.video.watch.download_video(watch_data, best_output, "/contents/video/"+str(watch_uuid)+".%(ext)s")


def insert_comments(comments, video_id, thread_id, thread_fork):
    if len(comments) <= 0:
        return
    mongo_connector.insert_comments([{
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
        "videoId": video_id,
        "threadId": thread_id,
        "fork": thread_fork,
        "createdAt": datetime.now(),
        "updatedAt": datetime.now()
    } for comment in comments])


def get_comments(task_id, watch_data, video_id):
    mongo_connector.update_task_status(task_id, "comment")
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
            comment_res = niconico_client.video.watch.get_comments(watch_data, when=when_unix, thread_key=thread_key)
        except CommentAPIError as e:
            if e.message == "EXPIRED_TOKEN":
                thread_key = niconico_client.video.watch.get_thread_key(watch_data.video.id_)
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
                insert_comments(thread.comments, video_id, thread.id_, thread.fork)
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
                insert_comments(comments, video_id, thread.id_, thread.fork)
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
                insert_comments(comments, video_id, thread.id_, thread.fork)
                main_min_no = thread.comments[0].no
                when_unix = int(datetime.fromisoformat(thread.comments[0].posted_at).timestamp())
        mongo_connector.update_task_status(task_id, "comment", additional={"commentCount": comment_count})
        time.sleep(1)
    return comment_count


def update_comments(task_id, watch_data, video_id):
    mongo_connector.update_task_status(task_id, "comment")
    when_unix = int(time.time())
    main_latest_comment = mongo_connector.get_latest_comment(video_id, "main")
    if main_latest_comment is not None:
        main_max_no = main_latest_comment.get("no")
    else:
        main_max_no = 0
    easy_latest_comment = mongo_connector.get_latest_comment(video_id, "easy")
    if easy_latest_comment is not None:
        easy_max_no = easy_latest_comment.get("no")
    else:
        easy_max_no = 0
    main_min_no = 0
    easy_min_no = 0
    is_finished = False
    comment_count = 0
    failed_count = 0
    thread_key = None
    print("main_max_no={}, easy_max_no={}".format(main_max_no, easy_max_no))
    while not is_finished:
        comment_res = None
        try:
            comment_res = niconico_client.video.watch.get_comments(watch_data, when=when_unix, thread_key=thread_key)
        except CommentAPIError as e:
            if e.message == "EXPIRED_TOKEN":
                thread_key = niconico_client.video.watch.get_thread_key(watch_data.video.id_)
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
                continue
            elif thread.fork == "easy":
                if easy_min_no == 0:
                    easy_min_no = thread.comments[-1].no + 1
                comments = []
                for comment in thread.comments:
                    if comment.no >= easy_min_no:
                        break
                    if comment.no <= easy_max_no:
                        continue
                    comments.append(comment)
                if len(comments) <= 0:
                    continue
                comment_count += len(comments)
                insert_comments(comments, video_id, thread.id_, thread.fork)
                easy_min_no = comments[0].no
            else:
                if main_min_no == 0:
                    main_min_no = thread.comments[-1].no + 1
                comments = []
                for comment in thread.comments:
                    if comment.no >= main_min_no:
                        break
                    if comment.no <= main_max_no:
                        continue
                    comments.append(comment)
                if len(comments) <= 0:
                    is_finished = True
                    continue
                comment_count += len(comments)
                insert_comments(comments, video_id, thread.id_, thread.fork)
                main_min_no = comments[0].no
                when_unix = int(datetime.fromisoformat(comments[0].posted_at).timestamp())
        mongo_connector.update_task_status(task_id, "comment", additional={"commentCount": comment_count})
        time.sleep(1)
    return comment_count


def finish(task_id):
    mongo_connector.update_task_status(task_id, "completed")


def error(task_id, e):
    mongo_connector.update_task_status(task_id, "failed", additional={"error": str(e)})


def main():
    print("nicoarch worker started")
    while True:
        task = redis_connector.pop_tasks()
        if task is None:
            time.sleep(10)
            continue
        task_id = task.decode("utf-8")
        try:
            print(f"Starting task {task_id}")
            task = mongo_connector.get_task(task_id)
            task_type = task.get("type")
            watch_id = task.get("watchId")
            if task_type == "new":
                print(f"Saving video data task {task_id}")
                watch_data, watch_uuid, video_id = save_video_data(task_id, watch_id)
                print(f"Downloading video task {task_id}")
                download_video(task_id, watch_data, watch_uuid, video_id)
            else:
                print(f"Updating video data task {task_id}")
                video_data = mongo_connector.get_video(watch_id)
                if video_data is None:
                    raise ValueError("Video not found")
                watch_data, video_id = update_video_data(task_id, watch_id)
            print(f"Video id: {video_id}")
        except Exception as e:
            print(f"Error task {task_id}", e)
            error(task_id, e)
            if task_type == "new":
                mongo_connector.delete_video(watch_id)
            else:
                if video_data is not None:
                    mongo_connector.replace_video(watch_id, video_data)
            continue
        try:
            task_start_time = datetime.now()
            if task_type == "new":
                print(f"Getting Comments task {task_id}")
                get_comments(task_id, watch_data, video_id)
            else:
                print(f"Updating comments task {task_id}")
                update_comments(task_id, watch_data, video_id)
            print(f"Finishing task {task_id}")
            finish(task_id)
        except Exception as e:
            print(f"Error task {task_id}", e)
            error(task_id, e)
            try:
                mongo_connector.delete_comments(video_id, task_start_time)
            except Exception as e:
                pass


if __name__ == "__main__":
    main()
