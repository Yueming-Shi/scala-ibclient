package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.given
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

import scala.concurrent.duration.*

/** Unit tests (no TWS needed) for executions: reqExecutions encoding, execDetails / execDetailsEnd /
  * commissionReport decoding and the executions stream.
  */
object ExecutionsSpec extends SimpleIOSuite:

  private val Max = "1.7976931348623157E308"
  private val ExecId = "0000e0d5.6530a8a1.01.01"

  /** A TWS execDetails message for a paper fill: bought 100 AAPL at 150.25. */
  private def execFields(serverVersion: Int, reqId: String = "5"): List[String] =
    List("11") ++ (if serverVersion < 136 then List("10") else Nil) ++ // msgId (message version before 136)
      List(reqId, "7", "265598", "AAPL", "STK", "", "0.0", "", "", "ISLAND", "USD", "AAPL", "NMS") ++ // contract
      List(ExecId, "20261016 10:00:00 US/Eastern", "DU1234567", "ISLAND", "BOT", "100", "150.25") ++
      List("3000000001", "10", "0", "100", "150.25", "my-ref", "", "", "") ++ // permId ... evMultiplier, modelCode
      (if serverVersion >= 136 then List("2") else Nil) // last liquidity: removed liquidity

  private def decodeWith(serverVersion: Int, fields: List[String]): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  test("reqExecutions without a filter is encoded like the TWS API (msg 7, version 3, empty filter)") {
    encoded[ReqExecutions](ReqExecutions(reqId = 5, filter = ExecutionFilter())).map(fields =>
      expect.same(List("7", "3", "5", "0", "", "", "", "", "", ""), fields)
    )
  }

  test("reqExecutions with a filter sends client id, account, time, symbol, security type, exchange and side") {
    val filter = ExecutionFilter(10, "DU1234567", "20261016-09:30:00", "AAPL", SecType.STK, "SMART", "BUY")
    encoded[ReqExecutions](ReqExecutions(reqId = 5, filter = filter)).map(fields =>
      expect.same(List("7", "3", "5", "10", "DU1234567", "20261016-09:30:00", "AAPL", "STK", "SMART", "BUY"), fields)
    )
  }

  private def checkFill(e: ExecDetails) =
    expect.same((5, 7, 265598, "AAPL", SecType.STK, "ISLAND", "USD"),
      (e.reqId, e.execution.orderId, e.contract.conId, e.contract.symbol, e.contract.secType, e.contract.exchange,
        e.contract.currency)) and
      expect.same((ExecId, "20261016 10:00:00 US/Eastern", "DU1234567", "BOT", BigDecimal(100), 150.25),
        (e.execution.execId, e.execution.time, e.execution.acctNumber, e.execution.side, e.execution.shares.value,
          e.execution.price)) and
      expect.same((3000000001L, 10, BigDecimal(100), 150.25, "my-ref"),
        (e.execution.permId, e.execution.clientId, e.execution.cumQty.value, e.execution.avgPrice,
          e.execution.orderRef))

  test("execDetails decodes (server 176: no message version, with last liquidity)") {
    decodeWith(176, execFields(176)).map {
      case e: ExecDetails => checkFill(e) and expect.same(Liquidities.Removed, e.execution.lastLiquidity)
      case other          => failure(s"unexpected $other")
    }
  }

  test("execDetails from an older server (130) reads the message version and has no last liquidity") {
    decodeWith(130, execFields(130)).map {
      case e: ExecDetails => checkFill(e) and expect.same(Liquidities.None, e.execution.lastLiquidity)
      case other          => failure(s"unexpected $other")
    }
  }

  test("an unknown last liquidity value decodes as None instead of failing") {
    decodeWith(176, execFields(176).dropRight(1) :+ "9").map {
      case e: ExecDetails => expect.same(Liquidities.None, e.execution.lastLiquidity)
      case other          => failure(s"unexpected $other")
    }
  }

  test("execDetailsEnd and commissionReport decode") {
    for
      end <- decodeWith(176, List("55", "1", "5"))
      commission <- decodeWith(176, List("59", "1", ExecId, "1.0", "USD", Max, Max, ""))
    yield expect.same(ExecDetailsEnd(5), end) and
      (commission match
        case CommissionReportMsg(c) =>
          expect.same((ExecId, 1.0, "USD", Double.MaxValue, 0), (c.execId, c.commission, c.currency, c.realizedPNL,
            c.yieldRedemptionDat))
        case other => failure(s"unexpected $other"))
  }

  private def fill(reqId: Int, execId: String) =
    ExecDetails(reqId, Contract(symbol = "AAPL"), Execution(orderId = 7, execId = execId))
  private def commission(execId: String) =
    CommissionReportMsg(CommissionReport(execId, 1.0, "USD", Double.MaxValue, Double.MaxValue, 0))

  test("reqExecutions sends the request and streams its fills with their commissions until the end") {
    val msgs = List(
      fill(5, "a.01"),
      fill(6, "b.01"), // another request's fill
      commission("a.01"),
      commission("z.01"), // a commission of a fill this request did not return
      fill(5, "c.01"),
      ExecDetailsEnd(6),
      ExecDetailsEnd(5),
      fill(5, "late.01"), // fills after the end marker are not part of the reply
      commission("c.01") // a commission report after the end marker still is
    )
    for
      client <- fakeClient(msgs*)
      reports <- client.reqExecutions(5).compile.toList
      sent <- client.sent.get
    yield expect.same(List(fill(5, "a.01"), commission("a.01"), fill(5, "c.01"), commission("c.01")), reports) and
      expect.same(Vector(List("7", "3", "5", "0", "", "", "", "", "", "")), sent)
  }

  test("the executions stream fails on a TWS error for its request, but not on warnings") {
    for
      client <- fakeClient(ErrorDetail(5, 2104, "Market data farm connection is OK", ""), fill(5, "a.01"),
        ErrorDetail(5, 321, "Error validating request", ""), ExecDetailsEnd(5))
      result <- client.reqExecutions(5).compile.toList.attempt
    yield expect.same(Left(Executions.RequestFailed(5, 321, "Error validating request")), result)
  }

  private def run(msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(Executions.reports(5)).compile.toList.attempt

  test("the executions stream fails on an unreadable fill or commission of its own, not on other messages") {
    for
      other <- run(Unreadable(List("1", "6", "x"), "boom", 176), fill(5, "a.01"), ExecDetailsEnd(5))
      badFill <- run(Unreadable(List("11", "5", "x"), "boom", 176), ExecDetailsEnd(5))
      oldServerFill <- run(Unreadable(List("11", "10", "5", "x"), "boom", 130), ExecDetailsEnd(5))
      badCommission <- run(fill(5, "a.01"), Unreadable(List("59", "1", "a.01", "x"), "boom", 176), ExecDetailsEnd(5))
    yield expect.same(Right(List(fill(5, "a.01"))), other) and
      expect.same(Left(Executions.Unreadable(5, List("11", "5", "x"), "boom")), badFill) and
      expect(oldServerFill.isLeft) and expect(badCommission.isLeft)
  }

  test("unreadable fills of other requests and commissions of other fills do not fail the executions stream") {
    for
      result <- run(
        Unreadable(List("11", "6", "x"), "boom", 176),
        Unreadable(List("11", "10", "6", "x"), "boom", 130),
        fill(5, "a.01"),
        Unreadable(List("59", "1", "z.01", "x"), "boom", 176),
        ExecDetailsEnd(5)
      )
    yield expect.same(Right(List(fill(5, "a.01"))), result)
  }

  test("commission reports arriving after the end marker are kept, and the stream completes once all arrived") {
    val msgs = Stream(fill(5, "a.01"), ExecDetailsEnd(5), commission("a.01")) ++ Stream.never[IO]
    msgs
      .through(Executions.reports(5, commissionWait = 1.hour))
      .compile
      .toList
      .timeout(5.seconds)
      .map(reports => expect.same(List(fill(5, "a.01"), commission("a.01")), reports))
  }

  test("the executions stream stops waiting for missing commission reports after the commission wait") {
    val msgs = Stream(fill(5, "a.01"), ExecDetailsEnd(5)) ++ Stream.never[IO]
    msgs
      .through(Executions.reports(5, commissionWait = 100.millis))
      .compile
      .toList
      .timeout(5.seconds)
      .map(reports => expect.same(List(fill(5, "a.01")), reports))
  }

  test("an empty executions reply completes at the end marker without waiting") {
    (Stream(ExecDetailsEnd(5)) ++ Stream.never[IO])
      .through(Executions.reports(5, commissionWait = 1.hour))
      .compile
      .toList
      .timeout(5.seconds)
      .map(reports => expect(reports.isEmpty))
  }

  test("the executions stream fails instead of stalling when the connection is lost") {
    for
      lost <- run(fill(5, "a.01"), ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(fill(5, "a.01"), ConnectionClosed)
    yield expect.same(Left(Executions.ConnectionLost(5, "[1100] Connectivity lost")), lost) and
      expect.same(Left(Executions.ConnectionLost(5, "TWS closed the connection")), closed)
  }
