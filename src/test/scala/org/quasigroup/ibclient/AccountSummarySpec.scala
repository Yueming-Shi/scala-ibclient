package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.given
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests (no TWS needed) for the account summary: reqAccountSummary / cancelAccountSummary encoding,
  * accountSummary / accountSummaryEnd decoding and the summary stream.
  */
object AccountSummarySpec extends SimpleIOSuite:

  private val Account = "DU1234567"

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  private def line(reqId: Int, tag: String, value: String) = AccountSummary(reqId, Account, tag, value, "USD")

  test("reqAccountSummary and cancelAccountSummary are encoded like the TWS API") {
    for
      req <- encoded[ReqAccountSummary](ReqAccountSummary(reqId = 9, group = "All", tags = "NetLiquidation,BuyingPower"))
      cancel <- encoded[CancelAccountSummary](CancelAccountSummary(reqId = 9))
    yield expect.same(List("62", "1", "9", "All", "NetLiquidation,BuyingPower"), req) and
      expect.same(List("63", "1", "9"), cancel)
  }

  pureTest("the default tag list is exactly the official API's AccountSummaryTags.AllTags") {
    val official = List("AccountType", "NetLiquidation", "TotalCashValue", "SettledCash", "AccruedCash", "BuyingPower",
      "EquityWithLoanValue", "PreviousDayEquityWithLoanValue", "GrossPositionValue", "ReqTEquity", "ReqTMargin", "SMA",
      "InitMarginReq", "MaintMarginReq", "AvailableFunds", "ExcessLiquidity", "Cushion", "FullInitMarginReq",
      "FullMaintMarginReq", "FullAvailableFunds", "FullExcessLiquidity", "LookAheadNextChange",
      "LookAheadInitMarginReq", "LookAheadMaintMarginReq", "LookAheadAvailableFunds", "LookAheadExcessLiquidity",
      "HighestSeverity", "DayTradesRemaining", "Leverage")
    expect.same(official.mkString(","), Portfolio.AccountSummaryTags.All)
  }

  test("accountSummary and accountSummaryEnd decode (server version 177)") {
    for
      summary <- decodeWith(177, "63", "1", "9", Account, "NetLiquidation", "1000000.00", "USD")
      end <- decodeWith(177, "64", "1", "9")
    yield expect.same(AccountSummary(9, Account, "NetLiquidation", "1000000.00", "USD"), summary) and
      expect.same(AccountSummaryEnd(9), end)
  }

  test("reqAccountSummary sends the request, streams its lines until accountSummaryEnd, then cancels") {
    val msgs = List(
      line(9, "NetLiquidation", "100"),
      line(8, "NetLiquidation", "5"), // another request
      AccountSummaryEnd(8),
      line(9, "BuyingPower", "400"),
      AccountSummaryEnd(9),
      line(9, "NetLiquidation", "101") // TWS resends after the end; not part of this summary
    )
    for
      client <- fakeClient(msgs*)
      lines <- client.reqAccountSummary(9, tags = "NetLiquidation,BuyingPower").compile.toList
      sent <- client.sent.get
    yield expect.same(List(line(9, "NetLiquidation", "100"), line(9, "BuyingPower", "400")), lines) and
      expect.same(Vector(List("62", "1", "9", "All", "NetLiquidation,BuyingPower"), List("63", "1", "9")), sent)
  }

  test("stopping the summary early also cancels it") {
    for
      client <- fakeClient(line(9, "NetLiquidation", "100"), line(9, "BuyingPower", "400"), AccountSummaryEnd(9))
      first <- client.reqAccountSummary(9).take(1).compile.toList
      sent <- client.sent.get
    yield expect.same(List(line(9, "NetLiquidation", "100")), first) and
      expect.same(List("63", "1", "9"), sent.last)
  }

  test("a line TWS sends twice is passed on twice (the summary is a subscription, not a de-duplicated snapshot)") {
    for
      client <- fakeClient(line(9, "NetLiquidation", "100"), line(9, "NetLiquidation", "100"), AccountSummaryEnd(9))
      lines <- client.reqAccountSummary(9).compile.toList
    yield expect.same(List.fill(2)(line(9, "NetLiquidation", "100")), lines)
  }

  test("the summary is cancelled when it fails too") {
    for
      client <- fakeClient(line(9, "NetLiquidation", "100"), ErrorDetail(9, 322, "Maximum number exceeded", ""))
      result <- client.reqAccountSummary(9).compile.toList.attempt
      sent <- client.sent.get
    yield expect(result.isLeft) and expect.same(List("63", "1", "9"), sent.last)
  }

  private def run(msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(Portfolio.accountSummary(9)).compile.toList.attempt

  test("the summary fails on a TWS error for its request, but not on warnings or other requests' errors") {
    for
      failed <- run(ErrorDetail(9, 322, "Maximum number of account summary requests exceeded", ""))
      fine <- run(ErrorDetail(9, 2104, "Market data farm connection is OK", ""), ErrorDetail(8, 322, "x", ""),
        line(9, "NetLiquidation", "100"), AccountSummaryEnd(9))
    yield expect.same(
      Left(Portfolio.RequestFailed("account summary 9", 322, "Maximum number of account summary requests exceeded")),
      failed
    ) and expect.same(Right(List(line(9, "NetLiquidation", "100"))), fine)
  }

  test("the summary fails on an unreadable line of its own request, not of another one") {
    for
      mine <- run(Unreadable(List("63", "1", "9", "x"), "boom", 177), AccountSummaryEnd(9))
      other <- run(Unreadable(List("63", "1", "8", "x"), "boom", 177), AccountSummaryEnd(9))
    yield expect.same(Left(Portfolio.Unreadable("account summary 9", List("63", "1", "9", "x"), "boom")), mine) and
      expect.same(Right(Nil), other)
  }

  test("the summary fails instead of stalling when the connection is lost") {
    for
      lost <- run(line(9, "NetLiquidation", "100"), ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(ConnectionClosed)
    yield expect.same(Left(Portfolio.ConnectionLost("account summary 9", "[1100] Connectivity lost")), lost) and
      expect.same(Left(Portfolio.ConnectionLost("account summary 9", "TWS closed the connection")), closed)
  }
