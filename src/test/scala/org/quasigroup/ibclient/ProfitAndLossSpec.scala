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

/** Unit tests (no TWS needed) for profit and loss: reqPnL / reqPnLSingle and their cancels encoding, pnl /
  * pnlSingle decoding and the PnL streams.
  */
object ProfitAndLossSpec extends SimpleIOSuite:

  private val Account = "DU1234567"
  private val Max = "1.7976931348623157E308"

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  test("reqPnL, cancelPnL, reqPnLSingle and cancelPnLSingle are encoded like the TWS API (no message version)") {
    for
      pnl <- encoded[ReqPnL](ReqPnL(reqId = 9, account = Account, modelCode = ""))
      cancel <- encoded[CancelPnL](CancelPnL(reqId = 9))
      single <- encoded[ReqPnLSingle](ReqPnLSingle(reqId = 10, account = Account, modelCode = "", conId = 265598))
      cancelSingle <- encoded[CancelPnLSingle](CancelPnLSingle(reqId = 10))
    yield expect.same(List("92", "9", Account, ""), pnl) and expect.same(List("93", "9"), cancel) and
      expect.same(List("94", "10", Account, "", "265598"), single) and expect.same(List("95", "10"), cancelSingle)
  }

  test("pnl decodes (server version 177: with unrealized and realized PnL)") {
    decodeWith(177, "94", "9", "12.5", "-3.25", Max).map(msg =>
      expect.same(PnL(9, 12.5, -3.25, Double.MaxValue), msg) and expect.same(None, ProfitAndLoss.known(Double.MaxValue))
    )
  }

  test("pnl from an older server (128) has only the daily PnL") {
    decodeWith(128, "94", "9", "12.5").map(msg => expect.same(PnL(9, 12.5, Double.MaxValue, Double.MaxValue), msg))
  }

  test("pnl from server versions 129–134 has unrealized PnL but no realized PnL") {
    decodeWith(129, "94", "9", "12.5", "-3.25").map(msg =>
      expect.same(PnL(9, 12.5, -3.25, Double.MaxValue), msg)
    )
  }

  test("pnlSingle from server versions 129–134 has unrealized PnL but no realized PnL") {
    decodeWith(129, "95", "10", "100", "12.5", "75.0", "15100.0").map {
      case p: PnLSingle =>
        expect.same((10, BigDecimal(100), 12.5, 75.0, Double.MaxValue, 15100.0),
          (p.reqId, p.pos.value, p.dailyPnL, p.unrealizedPnL, p.realizedPnL, p.value))
      case other => failure(s"unexpected $other")
    }
  }

  test("pnlSingle decodes (server version 177)") {
    decodeWith(177, "95", "10", "100", "12.5", "75.0", "0.0", "15100.0").map {
      case p: PnLSingle =>
        expect.same((10, BigDecimal(100), 12.5, 75.0, 0.0, 15100.0),
          (p.reqId, p.pos.value, p.dailyPnL, p.unrealizedPnL, p.realizedPnL, p.value))
      case other => failure(s"unexpected $other")
    }
  }

  test("reqPnL streams its own updates and cancels when stopped") {
    for
      client <- fakeClient(PnL(8, 1, 1, 1), PnL(9, 1, 2, 3), PnL(9, 2, 2, 3), PnL(9, 3, 2, 3))
      updates <- client.reqPnL(9, Account).take(2).compile.toList
      sent <- client.sent.get
    yield expect.same(List(PnL(9, 1, 2, 3), PnL(9, 2, 2, 3)), updates) and
      expect.same(Vector(List("92", "9", Account, ""), List("93", "9")), sent)
  }

  test("reqPnLSingle streams its own updates and cancels when stopped") {
    def single(reqId: Int): PnLSingle = PnLSingle(reqId, Decimal(BigDecimal(100)), 1, 2, 3, 4)
    val update = single(10)
    for
      client <- fakeClient(single(11), update)
      updates <- client.reqPnLSingle(10, Account, 265598).take(1).compile.toList
      sent <- client.sent.get
    yield expect.same(List(update), updates) and
      expect.same(Vector(List("94", "10", Account, "", "265598"), List("95", "10")), sent)
  }

  private def run(msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(ProfitAndLoss.pnl(9)).compile.toList.attempt

  test("the PnL stream fails on its own errors and unreadable updates, not on warnings or other requests'") {
    for
      failed <- run(ErrorDetail(9, 321, "Error validating request", ""))
      unreadable <- run(Unreadable(List("94", "9", "x"), "boom", 177))
      fine <- run(ErrorDetail(9, 2104, "Market data farm connection is OK", ""), ErrorDetail(8, 321, "x", ""),
        Unreadable(List("94", "8", "x"), "boom", 177), Unreadable(List("95", "9", "x"), "boom", 177), PnL(9, 1, 2, 3))
    yield expect.same(Left(Portfolio.RequestFailed("PnL 9", 321, "Error validating request")), failed) and
      expect.same(Left(Portfolio.Unreadable("PnL 9", List("94", "9", "x"), "boom")), unreadable) and
      expect.same(Right(List(PnL(9, 1, 2, 3))), fine)
  }

  test("the PnL stream fails instead of stalling when the connection is lost") {
    for
      lost <- run(ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(ConnectionClosed)
    yield expect.same(Left(Portfolio.ConnectionLost("PnL 9", "[1100] Connectivity lost")), lost) and
      expect.same(Left(Portfolio.ConnectionLost("PnL 9", "TWS closed the connection")), closed)
  }
