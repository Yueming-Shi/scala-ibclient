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
    PositionMsg(account, Contract(conId = symbol.hashCode.abs, symbol = symbol), Decimal(BigDecimal(quantity)), 10.0)

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

  test("stopping the positions stream after the first position also cancels the subscription") {
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
    Stream.emits(msgs).covary[IO].through(Portfolio.positions(IO.unit)).compile.toList.attempt

  test("an update sent while listing replaces the earlier position of the same account and instrument") {
    val aapl = position("DU1234567", "AAPL", 100)
    val ibm = position("DU1234567", "IBM", 5)
    val otherAccount = position("DU7654321", "AAPL", 1)
    run(aapl, ibm, otherAccount, aapl.copy(pos = Decimal(BigDecimal(120))), PositionEnd).map(result =>
      expect.same(Right(List(aapl.copy(pos = Decimal(BigDecimal(120))), ibm, otherAccount)), result)
    )
  }

  test("the positions stream fails on a TWS error without request id, but not on notices") {
    for
      failed <- run(ErrorDetail(-1, 321, "Error validating request", ""), PositionEnd)
      notices <- run(ErrorDetail(-1, 2104, "Market data farm connection is OK", ""),
        ErrorDetail(-1, 1102, "Connectivity restored - data maintained", ""), ErrorDetail(7, 202, "Order cancelled", ""),
        position("DU1234567", "AAPL", 100), PositionEnd)
    yield expect.same(Left(Portfolio.RequestFailed("positions", 321, "Error validating request")), failed) and
      expect.same(Right(List("AAPL")), notices.map(_.map(_.contract.symbol)))
  }

  test("a positions request stopped before positionEnd keeps the lock until that positionEnd or the wait is over") {
    for
      noEnd <- endlessFakeClient(position("DU1234567", "AAPL", 100))
      waited <- noEnd.reqPositions(300.millis).compile.toList.timeout(100.millis).attempt.timed
      // releasing took the stale-end wait, and the lock is free again afterwards
      free <- noEnd.positionsLock.lock.surround(IO.unit).timeout(1.second).attempt
      withEnd <- endlessFakeClient(position("DU1234567", "AAPL", 100), PositionEnd)
      quick <- withEnd.reqPositions(5.seconds).compile.toList.timed
    yield expect(waited._2.isLeft) and expect(waited._1 >= 300.millis) and expect(free.isRight) and
      expect(quick._1 < 2.seconds) and expect.same(List("AAPL"), quick._2.map(_.contract.symbol))
  }

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
