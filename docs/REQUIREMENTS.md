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
- Tests that need a running TWS ("integration tests") use the default settings, so they can only reach the
  paper account. They are skipped automatically when nothing listens on port 7497.

**How we check it (unit tests, no TWS needed):** the default port is 7497; the default settings pass the
guard; 7496 and 4001 are refused without the opt-in and accepted only with it; other ports such as 4002
(IB Gateway paper) are allowed.
