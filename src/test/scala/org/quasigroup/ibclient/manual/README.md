# Manual IB paper probes (not weaver tests)

These are `IOApp` entry points under `src/test` for ad-hoc paper Gateway checks on port 7497.
They are compiled with tests but are **not** discovered by Weaver and do not run during `sbt test`.

| Tool | Command |
|------|---------|
| AAPL MD modes + historical | `sbt "Test/runMain org.quasigroup.ibclient.manual.AaplMktCheck"` |
| ES front-month resolve + MD | `sbt "Test/runMain org.quasigroup.ibclient.manual.EsFrontMonthProbe"` |
| SPY paper fill BUY then SELL | `IB_PAPER_FILL_TEST=1 sbt "Test/runMain org.quasigroup.ibclient.manual.PaperFillTest"` |
| Historical AAPL+ESZ6 bars | `sbt "Test/runMain org.quasigroup.ibclient.manual.HistoricalBarsProbe"` |
| ESZ6 keepUpToDate ~2m | `sbt "Test/runMain org.quasigroup.ibclient.manual.HistoricalBarsKeepUpToDate"` |

`PaperFillTest` refuses to place orders unless `IB_PAPER_FILL_TEST=1`.
