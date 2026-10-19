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
      // Price ticks only. Meta replies (TickReqParams, MarketDataType) do not count: paper TWS with
      // delayed MD disabled sends those (and 10167/10168) but never prices.
      result <- ibclient
        .reqMktData(tickerId, USStockAtSmart)
        .collect { case t: TickPrice => t; case t: TickSize => t }
        .take(3)
        .compile
        .toList
        .timeout(30.seconds)
        .attempt
      expectations <- result match
        case Right(ticks) if ticks.nonEmpty => IO.pure(expect(ticks.size == 3))
        case Right(_) =>
          ignore(
            "no delayed price ticks from TWS (meta only). Enable delayed market data in Account Management / TWS (error 10168)."
          )
        case Left(_: java.util.concurrent.TimeoutException) =>
          ignore(
            "timed out waiting for delayed price ticks. Enable delayed market data in Account Management / TWS (error 10168)."
          )
        case Left(e) => IO.pure(failure(e.toString))
    yield expectations
  }
  integrationTest("list today's executions (read only)") { case (ibclient, random) =>
    for
      reqId <- random.nextIntBounded(1000000)
      reports <- ibclient.reqExecutions(reqId).compile.toList.timeout(30.seconds)
    yield expect(reports.forall {
      case e: ExecDetails => e.reqId == reqId
      case _              => true
    })
  }

  integrationTest("list the paper account's positions (read only)") { case (ibclient, _) =>
    for
      positions <- ibclient.reqPositions.compile.toList.timeout(30.seconds)
      // only counts are printed, never the positions themselves
      _ <- IO.println(s"[integration] positions: ${positions.size} in ${positions.map(_.account).distinct.size} account(s)")
    yield expect(positions.forall(p => IBSocketClientCats.isPaperAccount(p.account)))
  }

  /** The first account this paper TWS session manages. */
  private def firstAccount(ibclient: IBClient[IO]): IO[String] =
    ibclient
      .requestThrough[ReqManagedAccts, String](ReqManagedAccts())(_.collectFirst { case ManagedAccounts(a) => a })
      .compile
      .lastOrError
      .timeout(10.seconds)
      .map(_.split(',').map(_.trim).filter(_.nonEmpty).head)

  integrationTest("take a snapshot of the paper account's values and portfolio (read only)") { case (ibclient, _) =>
    for
      account <- firstAccount(ibclient)
      snap <- ibclient.accountSnapshot(account).timeout(30.seconds)
      netLiquidation = snap.values.collect { case (("NetLiquidation", currency), v) => s"$v $currency" }
      // only counts and the net liquidation value are printed, not the account's other details
      _ <- IO.println(s"[integration] account snapshot: ${snap.values.size} values, ${snap.portfolio.size} " +
        s"portfolio lines, NetLiquidation ${netLiquidation.mkString(", ")}, paper account: " +
        IBSocketClientCats.isPaperAccount(snap.account))
    yield expect.same(account, snap.account) and expect(snap.values.nonEmpty) and expect(netLiquidation.nonEmpty) and
      expect(snap.portfolio.forall(_.accountName == account))
  }

  integrationTest("read the paper account summary (read only)") { case (ibclient, random) =>
    for
      reqId <- random.nextIntBounded(1000000)
      lines <- ibclient.reqAccountSummary(reqId).compile.toList.timeout(30.seconds)
      tags = lines.map(_.tag).distinct
      netLiquidation = lines.filter(_.tag == "NetLiquidation").map(l => s"${l.value} ${l.currency}")
      // only counts, the tag names and the net liquidation value are printed, not the account's other details
      _ <- IO.println(s"[integration] account summary: ${lines.size} lines, ${lines.map(_.account).distinct.size} " +
        s"account(s), tags ${tags.mkString(",")}, NetLiquidation ${netLiquidation.mkString(", ")}")
    yield expect(lines.nonEmpty) and expect(lines.forall(_.reqId == reqId)) and expect(netLiquidation.nonEmpty) and
      expect(lines.forall(l => IBSocketClientCats.isPaperAccount(l.account)))
  }

  integrationTest("read the paper account's daily profit and loss (read only)") { case (ibclient, random) =>
    for
      account <- firstAccount(ibclient)
      reqId <- random.nextIntBounded(1000000)
      first <- ibclient.reqPnL(reqId, account).take(1).compile.lastOrError.timeout(30.seconds)
      known = (first.dailyPnL, first.unrealizedPnL, first.realizedPnL).toList.map(ProfitAndLoss.known(_).isDefined)
      // only which values TWS knows is printed, not the amounts
      _ <- IO.println(s"[integration] PnL update received; known daily/unrealized/realized: ${known.mkString("/")}")
    yield expect.same(reqId, first.reqId)
  }

  integrationTest("place and cancel a far-below-market paper LMT (1 share SPY @ $1 DAY)") { case (ibclient, _) =>
    val contract =
      Contract(symbol = "SPY", secType = SecType.STK, currency = "USD", exchange = "SMART")
    val order = Order(
      action = Action.BUY,
      totalQuantity = Decimal.ONE,
      orderType = Order.Type.LMT,
      lmtPrice = 1.0,
      auxPrice = 0.0,
      tif = TimeInForce.DAY,
      openClose = "",
      discretionaryAmt = 0.0
    )
    def isWorking(s: Order.Status): Boolean =
      s == Order.Status.PreSubmitted || s == Order.Status.Submitted || s == Order.Status.PendingSubmit

    for
      orderId <- ibclient.nextValidOrderId.timeout(15.seconds)
      // cancel in a finalizer even if the assertions fail or the wait times out
      result <- ibclient
        .placeOrder(orderId, contract, order)
        .evalTap {
          case s: OrderStatusMsg if isWorking(s.status) =>
            ibclient.cancelOrder(orderId)
          case e: ErrorDetail if e.errorCode == 321 && e.errorMsg.toLowerCase.contains("read-only") =>
            IO.raiseError(
              new IllegalStateException(
                "TWS API is in Read-Only mode; uncheck Read-Only API in Global Configuration → API → Settings to place paper orders"
              )
            )
          case _ => IO.unit
        }
        .compile
        .toList
        .timeout(60.seconds)
        .guarantee(ibclient.cancelOrder(orderId).timeout(5.seconds).attempt.void)
      statuses = result.collect { case s: OrderStatusMsg => s.status }
      _ <- IO.println(
        s"[integration] paper orderId=$orderId statuses=${statuses.map(_.toString).mkString(" -> ")} " +
          s"(events=${result.size})"
      )
    yield expect(statuses.exists(isWorking)) and expect(statuses.contains(Order.Status.Cancelled)) and
      expect(!statuses.contains(Order.Status.Filled)) and
      expect(result.forall {
        case o: OpenOrder      => o.orderId == orderId
        case s: OrderStatusMsg => s.orderId == orderId
        case e: ErrorDetail    => e.id == orderId
      })
  }
