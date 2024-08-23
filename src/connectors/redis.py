from redis.client import Redis

class RedisConnector():

    client: Redis

    def __init__(self, url: str):
        self.client = Redis.from_url(url)

    def pop_tasks(self):
        return self.client.lpop("tasks")

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
