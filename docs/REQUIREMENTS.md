# scala-ibclient requirements

Plain-language requirements for each feature, added one feature at a time.
"TWS" means Interactive Brokers' Trader Workstation, the desktop app this library talks to.

## 1. Paper account by default

**Why:** TWS listens on one port for the paper (practice) account and another for the live (real-money)
account. Connecting to the wrong one by mistake could place real trades.

**What it does:**

- When you create a client without saying which port to use, it connects to **7497**, the paper-trading port.
- The live ports (**7496** for TWS and **4001** for IB Gateway) are refused unless you add an explicit,
  hard-to-miss opt-in: `liveTrading = LiveTrading.IUnderstandThisIsARealMoneyAccount`.
- The check happens before any network connection is opened, so a refused port never touches the account.
- Because TWS lets you change its port numbers, the port alone is not proof. Right after connecting, the client
  asks TWS which accounts it manages and disconnects unless they are all paper accounts (IB paper account ids
  start with "D", for example DU1234567). The same opt-in is needed to use a live account.
- Tests that need a running TWS ("integration tests") use the default settings, so they can only reach the
  paper account. They are skipped automatically when the paper API on port 7497 cannot be reached.

**How we check it (unit tests, no TWS needed):** the default port is 7497; the default settings pass the
guard; 7496 and 4001 are refused without the opt-in and accepted only with it; other ports such as 4002
(IB Gateway paper) are allowed; paper account lists pass, while live, mixed or empty account lists are refused
without the opt-in; and `make` itself refuses a live port before connecting.

## 2. Streaming market data

**Why:** to follow a stock's price live: bid, ask, last trade, volume and so on.

**What it does:**

- `reqMktData(tickerId, contract)` asks TWS for market data on one instrument and returns a stream of updates
  ("ticks") for that request only. Each tick is one of: a price (with its size when TWS sends one), a size,
  a text value (for example the last trade time), a number (for example "halted"), or a note saying whether
  the data is real-time, frozen or delayed.
- Stopping the stream (for example with `take(10)`) automatically tells TWS to stop sending data
  (`cancelMktData`). A one-off snapshot (`snapshot = true`) ends on its own when TWS says it is complete.
- If TWS reports an error for the request (for example "not subscribed to this market data"), the stream fails
  with that error instead of waiting forever. Warnings such as "displaying delayed data" are ignored.
- If the connection to TWS or to IB's servers is lost (TWS notices 1100, 1101, 1300, or TWS closing the
  connection), the stream fails so the program can resubscribe, instead of silently receiving nothing.
- Option greeks, EFP data, request parameters (such as the minimum price step) and news headlines for the
  request are part of the stream too.
- Note: asking for a regulatory snapshot (`regulatorySnapshot = true`) costs USD 0.01 per request, even on
  the paper account.
- `reqMarketDataType` chooses real-time, frozen, delayed or delayed-frozen data. Paper accounts without data
  subscriptions can use delayed data.
- The subscription is opened before the request is sent, so the first ticks are never missed.

**Fixes made along the way:** delayed tick types (66 and above) were decoded as the wrong type; the
"market data type" and error messages were read one field off; a message ending in an empty field could crash
decoding; combo (BAG) contracts were sent without their legs.

**How we check it (unit tests, no TWS needed):** the request, cancel and data-type messages are compared field
by field with the TWS API format; canned tickPrice / tickSize / tickString / tickGeneric / marketDataType /
error messages are decoded from raw bytes; the tick stream keeps only its own ticker, ends at the snapshot end,
fails on errors, and cancels the subscription when stopped. One integration test (skipped without TWS) streams
three delayed ticks for IBM from the paper account.

## 3. Open orders

**Why:** to see which orders are still working (not yet filled or cancelled) on the account.

**What it does:**

- `reqOpenOrders` lists the open orders placed by this program; `reqAllOpenOrders` lists the open orders of
  every program connected to TWS and of TWS itself.
- Each one arrives as an "open order": the instrument, the order details (buy/sell, quantity, order type, limit
  price, time in force, account, ...) and its current state (for example "PreSubmitted").
- The stream finishes by itself when TWS says the list is complete ("open order end").
- If TWS ever sends a message the library cannot read, the live connection reports it and keeps going. If that
  message was an open order, the open orders stream fails with an error instead of returning an incomplete list.
- TWS's open-order replies do not say which request they answer, so on one connection only one open-orders
  request runs at a time; a second one waits for the first to finish.
- Note: if the program connects with client id 0, TWS also lets it take over (modify) orders typed into TWS by
  hand. The default client id is 10.

**Fixes made along the way:** the open order message was read with a "version" field that recent TWS versions
no longer send, which shifted every field; the two "condition" flags were read even when an order has no
conditions; several multi-part values (basis points, soft dollar tier, algo parameters, delta-neutral contract,
pegged-to-benchmark details) were read as a single field; "unset" numbers were confused with zero; values such as
an unset order type ("None"), hedge type letters, unknown algo names, time in force and security type no longer
crash the decoder.

**How we check it (unit tests, no TWS needed):** the two request messages are compared with the TWS API format;
a canned open order message (laid out like TWS 10.19+) is decoded for server versions 176 and 177, with and
without a price condition and with an algo strategy, and the last fields are checked so any misalignment shows;
the open orders stream ends at "open order end"; an unreadable message is passed on without stopping the
connection, makes the open orders stream fail when it was an open order, and is ignored otherwise; a second
open-orders request waits while one is running.

## 4. Order status

**What it does:** after an order is placed, changed or cancelled, TWS keeps reporting its progress: waiting,
submitted, partly filled, filled, cancelled. The program can follow these updates as a stream.

- `orderStatuses` gives the updates of every order this program sees; `orderStatus(orderId)` follows one order.
- Each update says the order's state, how much is filled and how much remains, the average and last fill price,
  the order's permanent id and, if TWS is holding the order, why.
- Following one order finishes by itself when the order is cancelled or completely filled.
- Listening starts as soon as the stream is opened, so open it before placing the order to see every update.
  TWS may send the same update twice; both are passed on.
- Following orders never silently stops: if an update cannot be read, or the connection to TWS or IB is lost, the
  stream fails with an error.
- No message is sent to TWS: these updates arrive by themselves (also in reply to an open-orders request).

**Fixes made along the way:** the order's permanent id can be larger than a regular whole number allows and is
now kept as a long number.

**How we check it (unit tests, no TWS needed):** canned order status messages are decoded for recent servers
(no version field, with the market cap price) and for an older one (with a version field), including fractional
quantities, very large permanent ids, an unknown state and dropped empty fields at the end; following one order
ends at "cancelled" or "filled with nothing remaining" and ignores other orders; unreadable updates and connection
loss make the stream fail; nothing is sent to TWS.
