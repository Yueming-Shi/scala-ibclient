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
