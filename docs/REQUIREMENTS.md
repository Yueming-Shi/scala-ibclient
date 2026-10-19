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
- Following one order finishes by itself when the order is cancelled, completely filled, or made inactive by IB
  (for example rejected). An order that is inactive only while IB looks for shares to borrow for a short sale is
  still followed.
- TWS does not promise an update for every change: an order that fills immediately may get no status update at
  all. So following one order should have a time limit, and to be sure an order filled, also follow its
  executions (feature 5).
- Listening starts as soon as the stream is opened, so open it before placing the order to see every update.
  TWS may send the same update twice; both are passed on.
- Following orders never silently stops: if an update cannot be read, or the connection to TWS or IB is lost, the
  stream fails with an error.
- No message is sent to TWS: these updates arrive by themselves (also in reply to an open-orders request).

**Fixes made along the way:** the order's permanent id can be larger than a regular whole number allows and is
now kept as a long number, the same way in order status, open order and execution messages. When the
connection to TWS ends without a goodbye message (socket closed or failed), every listener is now told, so
order status and open order streams fail instead of waiting forever.

**How we check it (unit tests, no TWS needed):** canned order status messages are decoded for recent servers
(no version field, with the market cap price) and for an older one (with a version field), including fractional
quantities, very large permanent ids, an unknown state and dropped empty fields at the end; following one order
ends at "cancelled", "filled with nothing remaining" or "inactive" (but not "inactive" while locating shares) and
ignores other orders; unreadable updates and connection loss make the stream fail; a socket that ends or fails
is turned into "connection closed" and makes the order status and open orders streams fail; large permanent ids
decode in open orders too; nothing is sent to TWS.

## 5. Executions (fills)

**What it does:** the program can ask TWS which of today's orders were filled ("executions"), optionally
narrowed down by a filter, and get the answer as a list that ends by itself.

- `reqExecutions(requestId, filter)` asks for the executions. The filter can narrow by client id, account, time
  (only executions after it), symbol, kind of security, exchange and side (buy/sell); an empty filter means all.
- Each execution says which order and instrument it belongs to, the execution id, time, account, exchange,
  side, quantity and price, the order's permanent id, the running total and average price, and whether the fill
  added or removed liquidity.
- The commission IB charged for an execution comes as a separate "commission report" (amount, currency,
  realized profit/loss) and is passed on when it arrives. TWS can send commission reports after it says the list
  of executions is complete, so the list then waits for the missing ones, at most 2 seconds by default
  (adjustable), and ends as soon as every execution has its commission report.
- The list of executions ends when TWS says it is complete ("executions end"). It fails with an error if TWS
  reports an error for the request, if one of its executions or their commission reports cannot be read, or if
  the connection is lost. An unreadable message that belongs to another request does not affect it.
- Not yet supported: the newest TWS versions (server version 178 and later) add fields to executions, such as
  a "pending price revision" flag. This library tells TWS it speaks at most version 177, so TWS does not send
  them.
- This only reads information; it never places or changes an order.

**Fixes made along the way:** the execution message was read with a "version" field that recent TWS versions no
longer send and with a "market name" field that TWS never sends there, which shifted every field; the kind of
security in the filter was sent as a number instead of its name (an empty kind is now sent as an empty field);
an unknown liquidity value no longer crashes the decoder.

**How we check it (unit tests, no TWS needed):** the request is compared with the TWS API format with and without
a filter; canned execution messages are decoded for a recent server (no version field, with liquidity) and an
older one (with a version field); "executions end" and commission reports decode; the stream keeps only its own
executions and their commission reports, keeps commission reports that arrive after "executions end", ends as
soon as all of them arrived or after the wait, and fails on a TWS error, an unreadable execution or commission
report of its own (but not of another request), and connection loss. An integration test (read only, skipped without the paper
TWS on port 7497) lists today's executions.

## 6. Portfolio (read only)

These features only read the account. None of them places, changes or cancels an order.

### 6a. Positions

**What it does:** the program can ask TWS what the account holds right now and get the answer as a list that
ends by itself.

- `reqPositions` asks for the positions of every account this TWS session manages. Each position says the
  account, the instrument (id, symbol, kind of security, exchange, currency, ...), the quantity (which can be
  fractional, or negative for a short position) and the average cost.
- The list ends when TWS says it is complete ("position end"). The positions are handed over together at that
  point, one per account and instrument: if TWS sends a change while it is still listing, the newer one replaces
  the older one. TWS would keep sending changes after the end, so when the list ends, or when the program stops
  it early, the library tells TWS to stop (`cancelPositions`).
- The list never silently misses something: if a position cannot be read, if TWS reports an error (positions
  requests have no request number, so this means an error without one, other than notices such as "market data
  farm connection is OK" or "connectivity restored"), or if the connection to TWS or IB is lost, it fails with an
  error.
- Position replies do not say which request they answer, so on one connection only one positions request runs at
  a time; a second one waits for the first to finish. If the first one was stopped before its "position end"
  arrived, the second one also waits for that late "position end" (at most 2 seconds by default), so it cannot
  mistake it for the end of its own list.

**Fixes made along the way:** `reqPositions` used to return every position message ever received without
stopping at "position end" and without cancelling the subscription. After review: a rejected positions request
(an error and no "position end") no longer waits forever, and a request stopped early can no longer leave a late
"position end" behind that would end the next request's list too soon.

**How we check it (unit tests, no TWS needed):** the request and cancel messages are compared with the TWS API
format; canned position messages (message version 3, server version 177) are decoded, including fractional and
short quantities, and "position end" decodes; the stream keeps the positions up to "position end", cancels the
subscription both at the end and when stopped early, completes for an empty account, replaces a position updated
while listing, fails on an unreadable position (but not on other unreadable messages), on a TWS error without
request number (but not on notices or other requests' errors) and on connection loss; a second request waits while
one is running, and a request stopped before "position end" listens for that end from before it cancels and keeps
the lock until the end arrives or the wait is over. An integration test (read only, skipped without the paper TWS on port 7497) lists the paper account's
positions and prints only how many there are.

### 6b. Account values and portfolio

**What it does:** the program can follow one account's values (cash, net liquidation value, buying power, ...)
and its portfolio (every holding with its market price and value, average cost and profit or loss), or take one
snapshot of them.

- `reqAccountUpdates(account)` subscribes to one account (for example "DU1234567"; an empty account code means
  the only account of a normal, non-advisor login). TWS first sends the whole account, then says it is done
  ("account download end"), then sends changes about every 3 minutes. The stream passes on:
  - account values: a name (for example `NetLiquidation`), a value, a currency and the account;
  - portfolio lines: the instrument, the position, market price, market value, average cost, unrealized and
    realized profit or loss, and the account;
  - the time of TWS's last update, and the "account download end" marker.
- Messages for another account are left out. The stream never ends by itself; when the program stops it, the
  library tells TWS to stop sending (unsubscribe).
- `accountSnapshot(account)` is the convenient way to get one picture of the account: it collects everything up
  to "account download end", keeping the latest value of each name and currency and the latest line of each
  instrument, then unsubscribes. TWS sends nothing at all for an unknown account, so put a time limit on it.
- It fails with an error if an account update cannot be read, if TWS reports an error without request number
  (other than notices such as "market data farm connection is OK"), if TWS says this program was unsubscribed
  from the account (2100) or refused the subscription because another program follows a different account (2101),
  or if the connection is lost.
- TWS keeps only one account subscription per connection, so only one runs at a time; a second one waits for the
  first. If the first was stopped before its "account download end" arrived, the second one also waits for that
  late marker (at most 2 seconds by default), so it cannot end the second snapshot too early. The library starts
  listening for it before it unsubscribes, so it cannot slip through in between. A marker that comes later than the
  wait could still end the next snapshot early; this is unlikely, since TWS sends it right after the account.

**Fixes made along the way:** account values were read one field off (the message version was skipped twice), so
every account value failed to decode. After review: TWS's notices 2100 and 2101 now end the subscription with an
error instead of being ignored like other notices, and the late-marker listener (also for positions) starts before
the cancel is sent.

**How we check it (unit tests, no TWS needed):** subscribe and unsubscribe messages are compared with the TWS API
format; canned account value, portfolio (message version 8, server version 177), update time and download end
messages are decoded; the stream keeps only its account's updates and unsubscribes when stopped; the snapshot
keeps the latest value and portfolio line of each, ignores what comes after "account download end", and takes the
account name from that marker when the account code is empty; the stream fails on an unreadable account update
(not on other unreadable messages), on a TWS error without request number (not on notices) and on connection loss;
2100 and 2101 make it fail; subscriptions run one at a time, and one stopped before its end listens for that end
from before it unsubscribes and keeps the lock until the end arrives or the wait is over (another account's end
does not count). An integration test (read only, skipped without the paper TWS on port 7497) takes a snapshot of the paper
account and prints only counts and the net liquidation value.

### 6c. Account summary

**What it does:** the program can ask for a short summary of its accounts, such as net liquidation value, cash,
buying power and margin, and get the answer as a list that ends by itself.

- `reqAccountSummary(requestId, group, tags)` asks for the summary of the accounts in a group ("All", the
  default, means every account of this login) and for the chosen tags (by default every tag the API knows, for
  example `NetLiquidation`, `TotalCashValue`, `BuyingPower`, `AvailableFunds`). Each line says the account, the
  tag, its value and its currency.
- The list ends when TWS says the summary is complete ("account summary end"). TWS would resend the summary every
  3 minutes, so when the list ends, or when the program stops it early, the library cancels the request
  (`cancelAccountSummary`).
- The replies carry the request number, so several summaries can run side by side (TWS allows at most two at a
  time and reports an error for a third).
- Lines are passed on as TWS sends them. TWS may send the same line twice (the paper TWS did, for every tag), so a
  program that wants one value per tag should keep the latest per account, tag and currency.
- It fails with an error if TWS reports an error for the request, if one of its lines cannot be read, or if the
  connection is lost. Warnings and errors of other requests do not affect it.

**Fixes made after review:** the default tag list misspelled `PreviousDayEquityWithLoanValue`, so TWS left that
value out.

**How we check it (unit tests, no TWS needed):** the request and cancel messages are compared with the TWS API
format; the default tag list is exactly the official API's list of 29 tags; canned summary line and "account summary end"
messages are decoded; the stream keeps only its own lines up to its own end, passes repeated lines on, cancels at
the end, when stopped early and when it fails, and fails on its own errors and unreadable lines (not on warnings or other requests') and on connection
loss. An integration test (read only, skipped without the paper TWS on port 7497) reads the paper account summary
and prints only counts, the tag names and the net liquidation value.

### 6d. Profit and loss (PnL)

**What it does:** the program can follow an account's profit and loss for the day, or that of one position, as
TWS updates it (about once a second while it changes).

- `reqPnL(requestId, account)` streams the account's daily, unrealized and realized profit and loss (optionally
  for one model, `modelCode`).
- `reqPnLSingle(requestId, account, contractId)` streams the same for one position (identified by its IB contract
  id, `conId`), plus its size and market value. An optional model code can be given, the same way as for `reqPnL`.
- TWS sends a special "unset" number (the largest possible number) for a value it does not have yet, for example
  the realized PnL before any trade today; `ProfitAndLoss.known(value)` tells whether a value is real.
- The streams never end by themselves; when the program stops one, the library cancels the request
  (`cancelPnL` / `cancelPnLSingle`). They fail if TWS reports an error for the request, if one of its updates
  cannot be read, or if the connection is lost. Warnings and other requests' errors do not affect them.

**Fixes made after review:** the requirements used to say "instrumentId" instead of the IB contract id (`conId`);
the decoder tests now also cover the versions between 129 and 134, where TWS sends the unrealized PnL but not yet
the realized one.

**How we check it (unit tests, no TWS needed):** the four request messages are compared with the TWS API format
(these have no message version); canned PnL messages are decoded for server version 177, for an older server (128)
without unrealized and realized PnL, and for versions 129–134 (unrealized only), and single-position PnL decodes
the same way; each stream keeps only its own updates and
cancels when stopped; the stream fails on its own errors and unreadable updates (not on warnings or other
requests') and on connection loss. An integration test (read only, skipped without the paper TWS on port 7497)
waits for the paper account's first PnL update and prints only which values TWS knows, not the amounts.

## 7. Placing and cancelling orders (paper only)

**Why:** to send a practice order to the paper account and follow it until it is filled or cancelled. A mistake
on the live account would spend real money, so this feature refuses anything but the paper TWS.

**What it does:**

- `placeOrder(orderId, contract, order)` sends the order and returns a stream of that order's events: open-order
  snapshots, status updates (PreSubmitted, Submitted, Filled, Cancelled, ...) and TWS errors for that order (for
  example 202 "Order Canceled"). The stream ends when the order is done (cancelled, completely filled or inactive),
  the same way as feature 4, or on a non-warning TWS error other than 202. Use `nextValidOrderId` to get a free
  order id from TWS before placing.
- `cancelOrder(orderId)` cancels an order this program placed. On recent TWS versions it also sends an optional
  "manual cancel time" (empty by default).
- `reqGlobalCancel` cancels every open order in TWS, including ones typed in by hand.
- All three refuse unless this session is paper-only: connected to the paper TWS port **7497**, without the live
  trading opt-in, and every managed account id starts with "D". The IB Gateway paper port 4002 is not enough.
- The placeOrder message follows the TWS API at server version 177 (no message version field; FA profile field
  omitted; manual order time and peg offsets included when set).

**Fixes made along the way:** the place-order encoder wrote the auxiliary price twice and crashed on an ordinary
limit order that has no volatility or hedge fields; it also skipped the "randomize size / price" flags. Cancel
on recent servers now sends the manual-cancel-time field.

**How we check it (unit tests, no TWS needed):** the paper-session guard accepts only port 7497 with paper
accounts and no live opt-in; cancelOrder encodes with and without the manual-cancel-time field; reqGlobalCancel
and reqIds match the TWS API; a 1-share LMT DAY placeOrder starts with the expected contract and order fields and
contains no market order; the events stream keeps one order's openOrder / status / error until Cancelled or
Filled, ignores other orders, and fails on connection loss. An integration test (paper TWS on 7497 only) places
a 1-share SPY LMT BUY far below the market (so it cannot fill), waits for Submitted or PreSubmitted, cancels it,
checks Cancelled, and cancels again in a finalizer if anything goes wrong. Never a market order, never port 7496.
