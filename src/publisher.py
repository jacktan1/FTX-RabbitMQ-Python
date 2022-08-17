import json
import pika
import requests
import time
from datetime import datetime
from .logger import initialize_logger


class Publisher:
    def __init__(self, ticker_list: list,
                 exchange_name: str, queue_name: str, routing_key: str,
                 logger_name: str = "publisher", log_save_dir: str = "logs", log_file: str = "publisher.log"):
        """
        Initialize logger and publisher thread (declare exchange, declare queue, bind exchange to queue)

        :param ticker_list: list of tickers to publish messages
        :param exchange_name: exchange name to publish message
        :param queue_name: queue name for message destination
        :param routing_key: routing key to bind exchange to queue
        :param logger_name: name of logger
        :param log_save_dir: directory to write logs
        :param log_file: logs filename
        """

        #
        # Attributes
        #

        self.routing_key = routing_key
        self.exchange = exchange_name
        self.queue = queue_name
        self.ticker_list = ticker_list
        self.api_uri = "https://ftx.com/api/markets/"
        # Max number of retries for GET request
        self.max_retries = 5
        # Seconds to wait between retries
        self.sleep = 1
        # Seconds to wait between job iterations
        self.publish_interval = 5

        #
        # Initialize logger
        #

        self.logger = initialize_logger(logger_name, log_save_dir, log_file)

        #
        # Initialize publisher
        #

        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()

        # Create exchange if needed
        self.channel.exchange_declare(exchange=self.exchange,
                                      exchange_type="direct")

        # Create queue if needed
        self.channel.queue_declare(queue=self.queue)

        # Bind queue to exchange
        self.channel.queue_bind(queue=self.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_key)

        self.logger.info(f"Bound exchange `{exchange_name}` to queue `{queue_name}` with routing key `{routing_key}`")

    def start(self):
        """
        Start publisher job thread.

        :return: None
        """
        while True:
            for ticker in self.ticker_list:
                # Initialize bookkeeping variables
                num_retries = 0
                success_response = False
                while not success_response:
                    try:
                        resp = requests.get(f"https://ftx.com/api/markets/{ticker}")

                        # Successful GET request
                        if resp.status_code == 200:
                            success_response = True
                            resp_dict = json.loads(resp.content)["result"]
                            # Add timestamp
                            resp_dict["publish timestamp"] = datetime.utcnow().timestamp()

                            # Publish
                            self.channel.basic_publish(exchange=self.exchange,
                                                       routing_key=self.routing_key,
                                                       body=json.dumps(resp_dict))
                        # Unsuccessful GET request
                        else:
                            self.logger.info(f"Status code: {resp.status_code} \n {resp.text}")

                    except requests.exceptions.Timeout or requests.exceptions.SSLError:
                        self.logger.info(f"Request failed!")

                    if not success_response:
                        # Max retries for ticker reached
                        if num_retries >= self.max_retries:
                            self.logger.warn(f"No successful response for ticker `{ticker}` "
                                             f"after {self.max_retries} attempts! Skipping...")
                            break
                        else:
                            num_retries += 1
                            self.logger.info(f"Wait {self.sleep} seconds before retrying...")
                            time.sleep(self.sleep)

            # Finished iterating through all tickers, wait for `self.publish_interval` seconds
            time.sleep(self.publish_interval)
