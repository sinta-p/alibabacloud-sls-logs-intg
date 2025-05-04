from aliyun.log.consumer import *
from aliyun.log.pulllog_response import PullLogResponse
import socket
import logging
import time
from multiprocessing import current_process


# === CONFIGURATION ===
ALI_SLS_ENDPOINT = "<your-sls-endpoint>"
ALI_ACCESS_KEY_ID = "<your-access-key-id>"
ALI_ACCESS_KEY_SECRET = "<your-access-key-secret>"
PROJECT_NAME = "<your-project-name>"
LOGSTORE_NAME = "<your-logstore-name>"
CONSUMER_GROUP = "datadog-forwarder-group"
CONSUMER_NAME = "{0}-{1}".format(CONSUMER_GROUP, current_process().pid)


DATADOG_HOST = "127.0.0.1"   # Change to your Datadog Agent IP
DATADOG_PORT = 10518         # Port where Datadog Agent is listening for logs
USE_TCP = True


# === OPTIONS ===
LOG_PULL_INTERVAL = 2  # seconds
LOG_BATCH_SIZE = 100   # max 100 per API call

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sls-datadog")

class DatadogForwarder(ConsumerProcessorBase):
    def __init__(self):
        super(DatadogForwarder, self).__init__()
        if USE_TCP:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((DATADOG_HOST, DATADOG_PORT))
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def process(self, log_groups, check_point_tracker):
        logs = PullLogResponse.loggroups_to_flattern_list(
            log_groups, time_as_str=True, decode_bytes=True
        )
        logger.info("Shard %s: Processing %d logs", self.shard_id, len(logs))

        for log in logs:
            # Extract and move __time__ into 'timestamp' field
            log_time = log.pop("__time__", None)
            if log_time:
                log["local_timestamp"] = log_time

            # Optional: add __topic__ to log if needed
            topic = log.pop("__topic__", None)
            if topic:
                log["topic"] = topic

            # Convert log to JSON string
            try:
                log_json = json.dumps(log, ensure_ascii=False)
                data = (log_json + "\n").encode("utf-8")
                logger.info(data)

                if USE_TCP:
                    self.sock.sendall(data)
                else:
                    self.sock.sendto(data, (DATADOG_HOST, DATADOG_PORT))
            except Exception as e:
                logger.warning("Failed to send log to Datadog: %s", e)

        self.save_checkpoint(check_point_tracker)

    def shutdown(self, check_point_tracker):
        logger.info("Shutting down processor for shard %s", self.shard_id)
        self.sock.close()

def main():
    config = LogHubConfig(
        ALI_SLS_ENDPOINT,
        ALI_ACCESS_KEY_ID,
        ALI_ACCESS_KEY_SECRET,
        PROJECT_NAME,
        LOGSTORE_NAME,
        CONSUMER_GROUP,
        CONSUMER_NAME,
        CursorPosition.BEGIN_CURSOR
    )


    worker = ConsumerWorker(DatadogForwarder, config)
    worker.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Interrupted. Exiting.")
        worker.shutdown()

if __name__ == "__main__":
    main()
