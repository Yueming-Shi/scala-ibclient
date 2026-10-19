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

/** Unit tests (no TWS needed) for positions: reqPositions / cancelPositions encoding, position / positionEnd
  * decoding and the positions stream.
  */
object PositionsSpec extends SimpleIOSuite:

  /** A TWS position message (version 3): 100 AAPL at an average cost of 150.25 in a paper account. */
  private def positionFields(account: String = "DU1234567", quantity: String = "100"): List[String] =
    List("61", "3", account, "265598", "AAPL", "STK", "", "0.0", "", "", "NASDAQ", "USD", "AAPL", "NMS", quantity,
      "150.25")

  private def decodeWith(serverVersion: Int, fields: List[String]): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  private def position(account: String, symbol: String, quantity: Int): PositionMsg =
    PositionMsg(account, Contract(symbol = symbol), Decimal(BigDecimal(quantity)), 10.0)

  test("reqPositions and cancelPositions are encoded like the TWS API") {
    for
      req <- encoded[ReqPositions](ReqPositions())
      cancel <- encoded[CancelPositions](CancelPositions())
    yield expect.same(List("61", "1"), req) and expect.same(List("64", "1"), cancel)
  }

  test("position decodes (server version 177, message version 3)") {
    decodeWith(177, positionFields()).map {
      case p: PositionMsg =>
        expect.same(
          ("DU1234567", 265598, "AAPL", SecType.STK, "NASDAQ", "USD", "AAPL", "NMS", BigDecimal(100), 150.25),
          (p.account, p.contract.conId, p.contract.symbol, p.contract.secType, p.contract.exchange,
            p.contract.currency, p.contract.localSymbol, p.contract.tradingClass, p.pos.value, p.avgCost)
        )
      case other => failure(s"unexpected $other")
    }
  }

  test("position decodes fractional and short quantities") {
    for
      fractional <- decodeWith(177, positionFields(quantity = "0.5"))
      short <- decodeWith(177, positionFields(quantity = "-20"))
    yield (fractional, short) match
      case (f: PositionMsg, s: PositionMsg) =>
        expect.same((BigDecimal("0.5"), BigDecimal(-20)), (f.pos.value, s.pos.value))
      case other => failure(s"unexpected $other")
  }

  test("positionEnd decodes") {
    decodeOne("62", "1").map(msg => expect.same(PositionEnd, msg))
  }

  test("reqPositions sends the request, streams the positions until positionEnd, then cancels") {
    val msgs = List(
      position("DU1234567", "AAPL", 100),
      OpenOrderEnd, // other messages are ignored
      position("DU7654321", "IBM", -5),
      PositionEnd,
      position("DU1234567", "MSFT", 1) // updates after the end are not part of the list
    )
    for
      client <- fakeClient(msgs*)
      positions <- client.reqPositions.compile.toList
      sent <- client.sent.get
    yield expect.same(List("AAPL", "IBM"), positions.map(_.contract.symbol)) and
      expect.same(Vector(List("61", "1"), List("64", "1")), sent)
  }

  test("stopping the positions stream early also cancels the subscription") {
    for
      client <- fakeClient(position("DU1234567", "AAPL", 100), position("DU1234567", "IBM", 1), PositionEnd)
      first <- client.reqPositions.take(1).compile.toList
      sent <- client.sent.get
    yield expect.same(List("AAPL"), first.map(_.contract.symbol)) and
      expect.same(Vector(List("61", "1"), List("64", "1")), sent)
  }

  test("an empty account completes at positionEnd") {
    for
      client <- fakeClient(PositionEnd)
      positions <- client.reqPositions.compile.toList
    yield expect(positions.isEmpty)
  }

  private def run(msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(Portfolio.positions).compile.toList.attempt

  test("the positions stream fails on an unreadable position, but not on other unreadable messages") {
    for
      bad <- run(position("DU1234567", "AAPL", 100), Unreadable(List("61", "3", "x"), "boom", 177), PositionEnd)
      other <- run(Unreadable(List("5", "7", "x"), "boom", 177), position("DU1234567", "AAPL", 100), PositionEnd)
    yield expect.same(Left(Portfolio.Unreadable("positions", List("61", "3", "x"), "boom")), bad) and
      expect.same(Right(List("AAPL")), other.map(_.map(_.contract.symbol)))
  }

  test("the positions stream fails instead of stalling when the connection is lost") {
    for
      lost <- run(position("DU1234567", "AAPL", 100), ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(ConnectionClosed)
      warning <- run(ErrorDetail(-1, 2104, "Market data farm connection is OK", ""), PositionEnd)
    yield expect.same(Left(Portfolio.ConnectionLost("positions", "[1100] Connectivity lost")), lost) and
      expect.same(Left(Portfolio.ConnectionLost("positions", "TWS closed the connection")), closed) and
      expect.same(Right(Nil), warning)
  }

  test("positions requests on one connection run one at a time") {
    for
      client <- fakeClient(position("DU1234567", "AAPL", 100), PositionEnd)
      blocked <- client.positionsLock.lock.surround(client.reqPositions.compile.toList.timeout(200.millis).attempt)
      free <- client.reqPositions.compile.toList
      sent <- client.sent.get
    yield expect(blocked.isLeft) and expect.same(List("AAPL"), free.map(_.contract.symbol)) and
      expect.same(Vector(List("61", "1"), List("64", "1")), sent)
  }
