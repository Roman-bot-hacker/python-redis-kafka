import logging

from workers.RedisNotifier import RedisNotifier


class ConsoleWriter:

    def __init__(self, redis_notifier, redis_write_step=100):
        """
        :type redis_notifier: RedisNotifier
        :type redis_write_step: int
        """
        super(ConsoleWriter, self).__init__()
        self.redis_notifier = redis_notifier
        self.redis_write_step = redis_write_step

    def write_to_console(self, filename, data):

        write_allowed = self.redis_notifier.notify_start_record(filename)

        if write_allowed:

            for idx, line in enumerate(data, start=1):
                logging.info(line)  # writing to console
                if idx % self.redis_write_step == 0:  # write to redis every n lines
                    self.redis_notifier.notify_read_lines(filename, f"{idx-100}..{idx}")

            self.redis_notifier.notify_completed(filename)

        else:
            logging.info(f"=====Attempted duplicate write for file: {filename} =====")
