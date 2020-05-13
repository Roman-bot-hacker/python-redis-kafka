from datetime import datetime


class RedisNotifier:

    def __init__(self, redis_connection):
        """
        :type redis_connection: redis.Redis
        """
        super(RedisNotifier, self).__init__()
        self.redis_connection = redis_connection

    def notify_start_record(self, filename):
        if not self.redis_connection.lrange(filename, -1, -1):  # filename hasn't been written
            self.redis_connection.lpush(filename, f'started: {datetime.now()}')
            return True
        else:  # filename already exists
            self.notify_duplicate_attempt(filename)
            return False

    def notify_read_lines(self, filename, read_lines):
        """
        :type: read_lines: str
        :type: filename: str
        """
        self.redis_connection.lpush(filename, read_lines)

    def notify_duplicate_attempt(self, filename):
        self.redis_connection.lpush(filename, "duplicate_attempt")

    def notify_completed(self, filename):
        self.redis_connection.lpush(filename, "completed")
