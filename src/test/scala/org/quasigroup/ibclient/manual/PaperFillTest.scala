package org.quasigroup.ibclient.manual

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.Orders
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.{IO, IOApp}
import cats.effect.std.Console
import cats.syntax.all.*
import scala.concurrent.duration.*

/** One-shot paper fill: MKT BUY 1 SPY then MKT SELL 1 SPY on port 7497.
  * Run: IB_PAPER_FILL_TEST=1 sbt "Test/runMain org.quasigroup.ibclient.manual.PaperFillTest"
  * Gated by IB_PAPER_FILL_TEST=1. Not part of the default test suite.
  */
object PaperFillTest extends IOApp.Simple:

  private val spy =
    Contract(symbol = "SPY", secType = SecType.STK, currency = "USD", exchange = "SMART")

  private def mkt(action: Action): Order =
    Order(
      action = action,
      totalQuantity = Decimal.ONE,
      orderType = Order.Type.MKT,
      lmtPrice = 0.0,
      auxPrice = 0.0,
      tif = TimeInForce.DAY,
      openClose = "",
      discretionaryAmt = 0.0
    )

  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[$t] $msg"))

  private def placeUntilDone(ib: IBClient[IO], action: Action): IO[(Int, List[Orders.OrderEvent])] =
    for
      orderId <- ib.nextValidOrderId.timeout(15.seconds)
      _ <- log(s"placing MKT $action 1 SPY orderId=$orderId")
      events <- ib
        .placeOrder(orderId, spy, mkt(action))
        .evalTap(e => log(s"  event: $e"))
        .compile
        .toList
        .timeout(90.seconds)
      statuses = events.collect { case s: OrderStatusMsg => s }
      errors = events.collect { case e: ErrorDetail => e }
      _ <- errors.traverse_(e => log(s"  ERROR ${e.errorCode}: ${e.errorMsg}"))
      filled = statuses.find(s => s.status == Order.Status.Filled && s.remaining.isZero)
      _ <- filled match
        case Some(s) =>
          log(s"FILLED orderId=$orderId avgFillPrice=${s.avgFillPrice} filled=${s.filled} remaining=${s.remaining}")
        case None =>
          log(s"NOT FILLED orderId=$orderId statuses=${statuses.map(_.status).mkString("->")}") *>
            IO.raiseError(
              new RuntimeException(
                s"order $orderId did not fill; last errors=${errors.map(e => s"${e.errorCode}:${e.errorMsg}")}"
              )
            )
    yield (orderId, events)

  def run: IO[Unit] =
    IO(sys.env.get("IB_PAPER_FILL_TEST")).flatMap {
      case Some("1") => runFill
      case _ =>
        Console[IO].println(
          "PaperFillTest skipped: set IB_PAPER_FILL_TEST=1 to place paper MKT BUY/SELL 1 SPY on 7497. " +
            "Run: sbt \"Test/runMain org.quasigroup.ibclient.manual.PaperFillTest\""
        )
    }

  private def runFill: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 77))
      .use { ib =>
        for
          // make() already required D* paper accounts; re-fetch with subscribe-before-request
          acctStr <- ib match
            case c: IBSocketClientCats[IO] => c.managedAccounts.timeout(20.seconds)
            case _ =>
              ib.subscribe.use(msgs =>
                ib.requestOnly(ReqManagedAccts()) *>
                  msgs.collectFirst { case ManagedAccounts(a) => a }.compile.lastOrError
              ).timeout(20.seconds)
          list = acctStr.split(",").map(_.trim).filter(_.nonEmpty).toList
          _ <- log(s"managedAccounts=$list")
          _ <- IO.raiseUnless(list.nonEmpty && list.forall(_.startsWith("D")))(
            new IllegalStateException(s"refusing: not all accounts start with D: $list")
          )
          buy <- placeUntilDone(ib, Action.BUY)
          (buyId, _) = buy
          buyReports <- ib.reqExecutions(9001, ExecutionFilter(symbol = "SPY")).compile.toList.timeout(30.seconds)
          _ <- log(s"executions after BUY (${buyReports.size}):")
          _ <- buyReports.traverse_ {
            case e: ExecDetails =>
              log(s"  ExecDetails reqId=${e.reqId} orderId=${e.execution.orderId} execId=${e.execution.execId} " +
                s"side=${e.execution.side} shares=${e.execution.shares} price=${e.execution.price} time=${e.execution.time}")
            case c: CommissionReportMsg =>
              log(s"  CommissionReport execId=${c.commissionReport.execId} commission=${c.commissionReport.commission} " +
                s"currency=${c.commissionReport.currency} realizedPNL=${c.commissionReport.realizedPNL}")
          }
          sell <- placeUntilDone(ib, Action.SELL)
          (sellId, _) = sell
          sellReports <- ib.reqExecutions(9002, ExecutionFilter(symbol = "SPY")).compile.toList.timeout(30.seconds)
          _ <- log(s"executions after SELL (${sellReports.size}):")
          _ <- sellReports.traverse_ {
            case e: ExecDetails =>
              log(s"  ExecDetails reqId=${e.reqId} orderId=${e.execution.orderId} execId=${e.execution.execId} " +
                s"side=${e.execution.side} shares=${e.execution.shares} price=${e.execution.price} time=${e.execution.time}")
            case c: CommissionReportMsg =>
              log(s"  CommissionReport execId=${c.commissionReport.execId} commission=${c.commissionReport.commission} " +
                s"currency=${c.commissionReport.currency} realizedPNL=${c.commissionReport.realizedPNL}")
          }
          positions <- ib.reqPositions.compile.toList.timeout(30.seconds)
          spyPos = positions.filter(p => p.contract.symbol == "SPY")
          _ <- log(s"SPY positions after flatten: $spyPos")
          _ <- IO.raiseUnless(spyPos.forall(p => p.pos.isZero))(
            new IllegalStateException(s"expected 0 SPY after flatten, got $spyPos")
          )
          _ <- log(s"OK paper fill test complete buyOrderId=$buyId sellOrderId=$sellId")
        yield ()
      }
      .handleErrorWith { e =>
        log(s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}") *> IO.raiseError(e)
      }
