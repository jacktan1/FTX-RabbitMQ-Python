# Price Tracking using RabbitMQ

## Usage

**Pull RabbitMQ image (if needed)**:

```
docker pull rabbitmq:3-management
```

**Start RabbitMQ server on localhost:**

```
docker run -d --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

**RabbitMQ UI:** `http://localhost:15672/`

**Start publisher job:**

```
python publisher_start.py
```

**Start consumer job:**

```
python consumer_start.py
```

## Design

### Publisher

- Given a list of tickers, periodically obtain price quotes via sending
  `https://ftx.com/api/markets/{TICKER}` GET requests.
- Publish ticker's retrieval time and price to RabbitMQ exchange / queue.

### Consumer

- Consume and store messages from RabbitMQ queue.
- Periodically attempt to calculate ~1 minute change in price of various tickers
  by comparing newly consumed messages with historical messages.
    - E.g. any message published `60 +/- 10 seconds` ago are considered candidates.
    - Take message closest to 60 second threshold, if available.
- Calculate and write ticker price change to external database (`./output/TICKER.csv`).
- Periodically prune historical messages.
    - Otherwise, monotonic degradation of performance longer consumer is kept alive.

### Notes

- Do not use a "locking" application (e.g. Microsoft Excel) to open ticker outputs
  while consumer script is running. Will lead to `PermissionError` when "calculate
  price change" thread writes out results.
- Publisher and Consumer logs are in `./logs/`

### Sample Output

!["BTC"](/img/BTC_sample.png)

### Tickers

```
"BTC-PERP", "ETH-PERP", "AVAX-PERP", "BNB-PERP", "FTT-PERP", "SOL-PERP",
"ADA-PERP", "ATOM-PERP", "LINK-PERP", "MATIC-PERP", "XMR-PERP", "FTM-PERP",
"NEAR-PERP", "SAND-PERP", "DOT-PERP", "OP-PERP", "AXS-PERP", "APE-PERP",
"WAVES-PERP", "CEL-PERP", "CRV-PERP", "GRT-PERP", "AAVE-PERP", "SNX-PERP"
```