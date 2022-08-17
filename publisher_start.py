from src import Publisher


def run():
    #
    # User variables
    #

    ticker_list = ["BTC-PERP", "ETH-PERP", "AVAX-PERP", "BNB-PERP", "FTT-PERP", "SOL-PERP", "ADA-PERP", "ATOM-PERP",
                   "LINK-PERP", "MATIC-PERP", "XMR-PERP", "FTM-PERP", "NEAR-PERP", "SAND-PERP", "DOT-PERP", "OP-PERP",
                   "AXS-PERP", "APE-PERP", "WAVES-PERP", "CEL-PERP", "CRV-PERP",
                   "GRT-PERP", "AAVE-PERP", "SNX-PERP"]
    exchange_name = "ftx_exchange"
    queue_name = "ftx_queue_python"
    routing_key = "ftx"

    #
    # Start publisher
    #

    my_publisher = Publisher(ticker_list, exchange_name, queue_name, routing_key)

    my_publisher.start()


if __name__ == "__main__":
    run()
