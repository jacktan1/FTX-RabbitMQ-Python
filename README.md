## Publishing and Consuming Using RabbitMQ

### Design

**Publisher**

- Given a list of tickers*, periodically obtain price quotes via sending GET requests to
  `https://ftx.com/api/markets/{TICKER}`.
- Publish ticker's retrieval time and price to RabbitMQ exchange / queue.

**Consumer**

- Consume and store messages from RabbitMQ queue.
- Periodically attempt to calculate `~1 minute` change in price of various tickers
  by comparing new messages with historical messages.
    - E.g. any message published 60 +/- 10 seconds ago are candidates.
- Calculate and write ticker price change to external database (`./output/TICKER.csv`).
- Perform periodic pruning of historical messages.
    - Otherwise, monotonic degradation of performance longer consumer is kept alice.

**Note**

- Do not use a "locking" application (e.g. Microsoft Excel) to open ticker outputs
  while consumer script is running. Will lead to `PermissionError` when "calculate
  price change" thread writes out results.
- Publisher and Consumer logs are in `./logs/`

**Sample Output**

!["BTC"](/img/BTC_sample.png)

**Tickers**

```
"BTC-PERP", "ETH-PERP", "AVAX-PERP", "BNB-PERP", "FTT-PERP",
"SOL-PERP", "ADA-PERP", "ATOM-PERP", "LINK-PERP", "MATIC-PERP",
"XMR-PERP", "FTM-PERP", "NEAR-PERP", "SAND-PERP", "DOT-PERP",
"OP-PERP", "AXS-PERP", "APE-PERP", "WAVES-PERP", "CEL-PERP",
"CRV-PERP", "GRT-PERP", "AAVE-PERP", "SNX-PERP"
```