import json
import os.path
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
        #
        # Initialize logger
        #

        self.logger = initialize_logger(logger_name, log_save_dir, log_file)

        #
        # Attributes
        #

        self.queue = queue_name

        self.keep_sec = 600
        self.time_target = 60
        self.time_margin = 10
        self.clean_interval = 20
        self.calc_interval = 5

        self.resp_cols = ["ticker", "publish timestamp", "consume timestamp", "price"]
        self.resp_df = pd.DataFrame(columns=self.resp_cols)

        self.last_calc_run = datetime.utcnow().timestamp()

        self.output_dir = output_dir

        #
        # Initialize price change output files
        #

        self.output_cols = ["ticker", "datetime (UTC)",
                            "price", f"price change in last {self.time_target} sec ($)"]
        for ticker in ticker_list:
            # If output files don't exist already
            if not os.path.isfile(os.path.join(self.output_dir, f"{ticker}.csv")):
                pd.DataFrame(columns=self.output_cols).to_csv(os.path.join(self.output_dir, f"{ticker}.csv"),
                                                              index=False)

        #
        # Initialize consumer
        #

        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()

        #
        # Initialize garbage collection
        #

        self.garbage_collect = threading.Thread(target=self.clean_hist_df)
        self.garbage_collect.start()

        #
        # Initialize price change calculation
        #

        self.start_calc = threading.Thread(target=self.calc_change)
        self.start_calc.start()

    def clean_hist_df(self):
        while True:
            old_num_msgs = self.resp_df.shape[0]
            self.resp_df = self.resp_df[self.resp_df["publish timestamp"] >=
                                        (datetime.utcnow().timestamp() - self.keep_sec)].reset_index(drop=True)
            self.logger.info(f"Pruned {old_num_msgs - self.resp_df.shape[0]} messages from history")
            time.sleep(self.clean_interval)

    def calc_change(self):
        while True:
            time_now = datetime.utcnow().timestamp()

            new_resps = self.resp_df[(self.resp_df["consume timestamp"] > self.last_calc_run) &
                                     (self.resp_df["consume timestamp"] <= time_now)].copy()

            for n in range(new_resps.shape[0]):
                new_msg = new_resps.iloc[n]
                msg_ticker = new_msg["ticker"]
                msg_pub_timestamp = new_msg["publish timestamp"]
                msg_price = new_msg["price"]

                temp_df = self.resp_df[
                    (self.resp_df["ticker"] == msg_ticker) &
                    (self.resp_df["publish timestamp"] > msg_pub_timestamp - (self.time_target + self.time_margin)) &
                    (self.resp_df["publish timestamp"] < msg_pub_timestamp - (self.time_target - self.time_margin))
                    ].sort_values(ascending=True,
                                  by="publish timestamp",
                                  key=lambda t: abs(t - msg_pub_timestamp)).copy()

                if temp_df.shape[0]:
                    # Best reference price
                    old_price = temp_df.iloc[0]["price"]
                    price_change = round(msg_price - old_price, 4)

                    pd.DataFrame(columns=self.output_cols,
                                 data=[[msg_ticker,
                                        datetime.fromtimestamp(msg_pub_timestamp),
                                        msg_price,
                                        price_change]]).to_csv(
                        os.path.join(os.path.join(self.output_dir, f"{msg_ticker}.csv")),
                        header=False, index=False, mode="a")

            # Finished run
            self.last_calc_run = time_now
            time.sleep(self.calc_interval)

    def start(self):
        self.channel.basic_consume(queue=self.queue,
                                   auto_ack=True,
                                   on_message_callback=self.add_to_resp_df)

        self.channel.start_consuming()

    def add_to_resp_df(self, ch, method, properties, body):
        resp_dict = json.loads(body)

        self.resp_df = pd.concat([self.resp_df, pd.DataFrame(columns=self.resp_cols,
                                                             data=[[resp_dict["name"],
                                                                    resp_dict["publish timestamp"],
                                                                    datetime.utcnow().timestamp(),
                                                                    resp_dict["price"]]])],
                                 ignore_index=True)
