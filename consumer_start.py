from src import Consumer


def run():
    #
    # User variables
    #

    ticker_list = ["BTC-PERP", "ETH-PERP", "AVAX-PERP", "BNB-PERP", "FTT-PERP", "SOL-PERP", "ADA-PERP", "ATOM-PERP",
                   "LINK-PERP", "MATIC-PERP", "XMR-PERP", "FTM-PERP", "NEAR-PERP", "SAND-PERP", "DOT-PERP", "OP-PERP",
                   "AXS-PERP", "APE-PERP", "WAVES-PERP", "CEL-PERP", "CRV-PERP",
                   "GRT-PERP", "AAVE-PERP", "SNX-PERP"]
    queue_name = "ftx_queue_python"

    #
    # Start consumer
    #

    my_consumer = Consumer(ticker_list, queue_name)

    my_consumer.start()


if __name__ == "__main__":
    run()
