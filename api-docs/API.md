API: Architecture 

Interface

Implementation

Establishing an API connection
 
Client will be obtained as a resource via

```Scala
val client: Resource[F, IBClient[F]] = IBSocketClientCats.make[F]()
```

with defaults as followed (`IBSocketClientCats.Settings`), override to you envirment as needed.
```Scala
    host: Host = host"127.0.0.1",
    port: Port = PaperPort, // port"7497", the TWS paper-trading port
    clientId: Int = 10,
    optionalCapabilities: Option[String] = None,
    liveTrading: LiveTrading = LiveTrading.Refused
```

Live ports (7496 for TWS, 4001 for IB Gateway) are refused unless you pass
`liveTrading = LiveTrading.IUnderstandThisIsARealMoneyAccount`.


Request Only

Request and Response

Request for Data Stream