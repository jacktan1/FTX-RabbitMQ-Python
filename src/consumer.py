import json
import os
import pika
import threading
import time
import pandas as pd
from datetime import datetime
from .logger import initialize_logger


class Consumer:
    def __init__(self, ticker_list: list,
                 queue_name: str,
                 logger_name: str = "consumer", log_save_dir: str = "logs", log_file: str = "consumer.log",
                 output_dir: str = "output"):
        """
        Initialize logger, output files, consumer thread, garbage collection thread,
        and price change calculation thread.

        :param ticker_list: list of tickers to publish messages
        :param queue_name: queue name for message source
        :param logger_name: name of logger
        :param log_save_dir: directory to write logs
        :param log_file: logs filename
        :param output_dir: output directory
        """

        #
        # Attributes
        #

        self.queue = queue_name
        # Target number of seconds for calculating change in price
        self.time_target = 60
        # Margin of error (seconds) allowed for calculation
        self.time_margin = 10
        # Seconds between price change calculation job
        self.calc_interval = 5
        # Timestamp of last price change calculation run
        self.last_calc_run = datetime.utcnow().timestamp()
        # How long to keep historical messages
        self.keep_sec = 600
        # Seconds between historical message prune job
        self.clean_interval = 20
        # Fields of consumed messages
        self.resp_cols = ["ticker", "publish timestamp", "consume timestamp", "price"]
        # Dataframe to store historical consumed messages
        self.resp_df = pd.DataFrame(columns=self.resp_cols)
        # Output directory
        self.output_dir = output_dir
        # Output columns
        self.output_cols = ["ticker",
                            "datetime (UTC)",
                            "price",
                            f"price change in last {self.time_target} sec ($)"]

        #
        # Initialize logger
        #

        self.logger = initialize_logger(logger_name, log_save_dir, log_file)

        #
        # Create output directory / files if needed
        #

        # Directory
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

        # Output files with column names
        for ticker in ticker_list:
            if not os.path.isfile(os.path.join(self.output_dir, f"{ticker}.csv")):
                pd.DataFrame(columns=self.output_cols).to_csv(os.path.join(self.output_dir, f"{ticker}.csv"),
                                                              index=False)

        #
        # Initialize consumer
        #

        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()

        #
        # Initialize garbage collection thread
        #

        self.garbage_collect = threading.Thread(target=self.clean_hist_df)
        self.garbage_collect.start()

        #
        # Initialize price change calculation thread
        #

        self.start_calc = threading.Thread(target=self.calc_change)
        self.start_calc.start()

    def clean_hist_df(self):
        """
        Prune messages in `self.resp_df` with published timestamp older than `self.keep_sec` seconds.

        :return: None
        """
        while True:
            old_num_msgs = self.resp_df.shape[0]
            self.resp_df = self.resp_df[self.resp_df["publish timestamp"] >=
                                        (datetime.utcnow().timestamp() - self.keep_sec)].reset_index(drop=True)
            self.logger.info(f"Pruned {old_num_msgs - self.resp_df.shape[0]} messages from history")
            time.sleep(self.clean_interval)

    def calc_change(self):
        """
        For all new messages consumed since last `self.last_calc_run`, attempt to
        calculate change in ticker price over the last `self.time_target` seconds.

        :return: None
        """
        while True:
            time_now = datetime.utcnow().timestamp()

            # New messages received since last run
            new_resps = self.resp_df[(self.resp_df["consume timestamp"] > self.last_calc_run) &
                                     (self.resp_df["consume timestamp"] <= time_now)].copy()

            for n in range(new_resps.shape[0]):
                new_msg = new_resps.iloc[n]
                msg_ticker = new_msg["ticker"]
                msg_pub_timestamp = new_msg["publish timestamp"]
                msg_price = new_msg["price"]

                # Find all messages (self.time_target +/- self.time_margin) seconds prior to message timestamp
                temp_df = self.resp_df[
                    (self.resp_df["ticker"] == msg_ticker) &
                    (self.resp_df["publish timestamp"] > msg_pub_timestamp - (self.time_target + self.time_margin)) &
                    (self.resp_df["publish timestamp"] < msg_pub_timestamp - (self.time_target - self.time_margin))
                    ].sort_values(ascending=True,
                                  by="publish timestamp",
                                  key=lambda t: abs(t - (msg_pub_timestamp - self.time_target))).copy()

                if temp_df.shape[0]:
                    # Take price at timestamp closest to self.time_target seconds prior to message timestamp
                    old_price = temp_df.iloc[0]["price"]
                    price_change = round(msg_price - old_price, 4)

                    # Write out results
                    pd.DataFrame(columns=self.output_cols,
                                 data=[[msg_ticker,
                                        datetime.fromtimestamp(msg_pub_timestamp),
                                        msg_price,
                                        price_change]]).to_csv(
                        os.path.join(os.path.join(self.output_dir, f"{msg_ticker}.csv")),
                        header=False, index=False, mode="a")

            # Finished iterating through all new messages
            self.last_calc_run = time_now
            time.sleep(self.calc_interval)

    def start(self):
        """
        Start consumer job thread.

        :return: None
        """
        self.channel.basic_consume(queue=self.queue,
                                   auto_ack=True,
                                   on_message_callback=self.add_to_resp_df)

        self.channel.start_consuming()

    def add_to_resp_df(self, ch, method, properties, body):
        """
        Callable function for consuming new messages.
        Adds appropriate key-value pairs to `self.resp_df`.

        :param ch:
        :param method:
        :param properties:
        :param body: message body (bytes)
        :return: None
        """
        resp_dict = json.loads(body)

        self.resp_df = pd.concat([self.resp_df, pd.DataFrame(columns=self.resp_cols,
                                                             data=[[resp_dict["name"],
                                                                    resp_dict["publish timestamp"],
                                                                    datetime.utcnow().timestamp(),
                                                                    resp_dict["price"]]])],
                                 ignore_index=True)
