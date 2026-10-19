package org.quasigroup.ibclient

import org.quasigroup.ibclient.types.*
import org.quasigroup.ibclient.types.TypesCodec.given
import org.quasigroup.ibclient.encoder.Encoder
import org.quasigroup.ibclient.encoder.Encoder.{*, given}
import org.quasigroup.ibclient.decoder.Decoder
import org.quasigroup.ibclient.decoder.Decoder.{*, given}
import org.quasigroup.ibclient.request.RequestMsg
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.{*, given}
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.response.MsgDecoders.given
import cats.syntax.all.*
import cats.effect.kernel.Resource
import cats.effect.{Clock, IO, SyncIO}
import cats.effect.std.Random
import org.quasigroup.ibclient.impl.IBSocketClientCats
import weaver.{Expectations, IOSuite}

import scala.language.postfixOps
import scala.concurrent.duration._

/** INTEGRATION tests: they talk to a running Trader Workstation on the PAPER port 7497.
  *
  * The shared client is created with the default settings (paper port, live ports and live accounts refused) using
  * the real API handshake. When that fails or times out (TWS not running, API disabled), every test here is reported
  * as ignored with the reason, so `sbt test` stays green on any machine.
  */
object IBSocketClientCatsSuite extends IOSuite with ContractSpec with OrderSpec:

  override type Res = Either[String, (IBClient[IO], Random[IO])]

  override def sharedResource: Resource[IO, Res] =
    Resource
      .make(IBSocketClientCats.make[IO]().allocated.timeout(20.seconds).attempt)(_.fold(_ => IO.unit, _._2))
      .evalMap(
        _.leftMap(e => s"TWS paper API not available on 127.0.0.1:7497 ($e)")
          .traverse { case (client, _) => Random.scalaUtilRandom[IO].map(client -> _) }
      )

  def integrationTest(name: String)(body: ((IBClient[IO], Random[IO])) => IO[Expectations]): Unit =
    test(name)(_.fold(ignore(_), body))

  integrationTest("obtain histogram") { case (ibclient, random) =>
    for {
      tickerId <- random.nextInt
      _ <- ibclient.requestOnly[ReqHistogramData](
        ReqHistogramData(tickerId = tickerId, contract = USStock, useRTH = false, timePeriod = "3 days")
      )
      //
      // _ <- ibclient.requestOnly[CancelHistogramData](CancelHistogramData(tickerId = tickerId))
      _ <- IO.sleep(60 seconds)
    } yield {
      success
    }
  }

  integrationTest("stream market data (delayed if not subscribed)") { case (ibclient, random) =>
    for
      tickerId <- random.nextIntBounded(1000000)
      _ <- ibclient.reqMarketDataType(MktDataType.Delayed)
      ticks <- ibclient.reqMktData(tickerId, USStockAtSmart).take(3).compile.toList.timeout(30.seconds)
    yield expect(ticks.size == 3)
  }

//    test("obtain historical ticks") { case (ibclient, random) =>
//     for {
//       reqId1 <- random.nextInt
//       req1 <- ReqHistoricalTicks(
//         reqId = reqId1,
//         contract = USStockAtSmart,
//         startDateTime = "20220808 10:00:00 US/Eastern",
//         endDateTime = "",
//         numberOfTicks = 10,
//         whatToShow = WhatToShow.TRADES,
//         useRTH = 1,
//         ignoreSize = true,
//         miscOptions = Nil
//       ).pure
//       _ <- ibclient.requestOnly[ReqHistoricalTicks](req1)
//       reqId2 <- random.nextInt
//       _ <- ibclient.requestOnly[ReqHistoricalTicks](req1.copy(reqId = reqId2, whatToShow = WhatToShow.BID_ASK))
//       reqId3 <- random.nextInt
//       _ <- ibclient.requestOnly[ReqHistoricalTicks](req1.copy(reqId = reqId3, whatToShow = WhatToShow.MIDPOINT))
//     } yield {
//       success
//     }
//   }

// test("obtain pnls") { case (ibclient, random) =>
//   for {
//     reqId <- random.nextInt
//     _ <- ibclient.requestOnly[ReqPnL](ReqPnL(reqId = reqId, account = "DUD00029", modelCode = ""))
//     _ <- ibclient.requestOnly[CancelPnL](CancelPnL(reqId = reqId))
//   } yield {
//     success
//   }
// }

// test("obtain pnl single") { case (ibclient, random) =>
//   for {
//     reqId <- random.nextInt
//     _ <- ibclient.requestOnly[ReqPnLSingle](
//       ReqPnLSingle(reqId = reqId, account = "DUD00029", modelCode = "", conId = 268084)
//     )
//     _ <- ibclient.requestOnly[CancelPnLSingle](CancelPnLSingle(reqId = reqId))
//   } yield {
//     success
//   }
// }

//  test("obtain news bulletins") { case (ibclient, random) =>
//    for {
//      // reqId <- random.nextInt
//      ReqNewsBulletins <- ibclient
//        .requestStreamResponse[ReqNewsBulletins, UpdateNewsBulletin](ReqNewsBulletins(allMsgs = true))
//        .take(100)
//        .compile
//        .toList
//      _ <- ibclient.requestOnly[CancelNewsBulletins](CancelNewsBulletins())
//    } yield {
//      success
//    }
//  }

// test("tick Data Operations") { case (ibclient, random) =>
//   for {
//     // reqId <- random.nextInt
//      _ <- ibclient.requestOnly[ReqMarketDataType](ReqMarketDataType(marketDataType = MktDataType.DelayedFrozen))

//   } yield {
//     success
//   }
// }

// test("tick Option Computations") { case (ibclient, random) =>
//   for {
//     // reqId <- random.nextInt
//      _ <- ibclient.requestOnly[ReqMarketDataType](ReqMarketDataType(marketDataType = MktDataType.DelayedFrozen))

//   } yield {
//     success
//   }
// }

// test("realTime Bars") { case (ibclient, random) =>
//   for {
//     // reqId <- random.nextInt
//     tickerId <- random.nextInt
//     _ <- ibclient.requestOnly[ReqRealTimeBars](ReqRealTimeBars(tickerId = tickerId, contract = EurGbpFx, barSize = 5, whatToShow = WhatToShow.MIDPOINT, useRTH = true, realTimeBarsOptions = Nil))
//    // _ <- ibclient.requestOnly[CancelRealTimeBars](CancelRealTimeBars(tickerId = tickerId))

//   } yield {
//     success
//   }
// }

// test("account Operations") { case (ibclient, random) =>
//   for {
//     reqId <- random.nextInt
//     _ <- ibclient.requestOnly[ReqPositionsMulti](
//       ReqPositionsMulti(reqId = reqId, account = "DU74649", modelCode = "EUstocks")
//     )
//     //
//    // _ <- ibclient.requestOnly[CancelHistogramData](CancelHistogramData(tickerId = reqId))
//     accounts <- ibclient.reqManagedAccts
//     familycodes <- ibclient.reqFamilyCodes
//     positions <- ibclient.reqPositions.compile.toList
//             _ <- ibclient.cancelPositions
//   } yield {
//     expect(familycodes.familyCodes.length >= 1) and
//     expect(accounts.accountsList.nonEmpty) and
//     expect(positions.nonEmpty)
//   }
// }

//   test("ibclient can request for current time") { case (ibclient,random) =>
//     for {
//       now <- IO.realTimeInstant
//       serverTime <- ibclient.reqCurrentTime
//     } yield {
//       expect(serverTime.time <= now.getEpochSecond)
//     }
//   }
