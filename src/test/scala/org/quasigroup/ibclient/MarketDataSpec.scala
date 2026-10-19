package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.given
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests (no TWS needed) for streaming market data: request encoding, tick decoding and the tick stream. */
object MarketDataSpec extends SimpleIOSuite:

  private val spy = Contract(symbol = "SPY", secType = SecType.STK, exchange = "SMART", currency = "USD")
  private def size(n: Int): Decimal = Decimal(BigDecimal(n))

  // ---- requests -------------------------------------------------------------------------------------------------

  test("reqMktData is encoded like the TWS API (msg 1, version 11)") {
    encoded[ReqMktData](
      ReqMktData(tickerId = 42, contract = spy, genericTickList = "233", snapshot = false, regulatorySnapshot = false,
        mktDataOptions = Nil)
    ).map(f =>
      expect.same(
        List("1", "11", "42", "0", "SPY", "STK", "", "0.0", "", "", "SMART", "", "USD", "", "", "0", "233", "0", "0", ""),
        f
      )
    )
  }

  test("reqMktData for a combo (BAG) contract sends its legs") {
    val combo = Contract(symbol = "MCD", secType = SecType.BAG, exchange = "SMART", currency = "USD",
      comboLegs = List(ComboLeg(conId = 43645865, ratio = 1, action = Action.BUY, exchange = "SMART"),
        ComboLeg(conId = 9408, ratio = 1, action = Action.SELL, exchange = "SMART")))
    encoded[ReqMktData](ReqMktData(tickerId = 7, contract = combo, genericTickList = "", snapshot = true,
      regulatorySnapshot = false, mktDataOptions = List(TagValue("a", "b")))).map(f =>
      expect.same(
        List("1", "11", "7", "0", "MCD", "BAG", "", "0.0", "", "", "SMART", "", "USD", "", "",
          "2", "43645865", "1", "BUY", "SMART", "9408", "1", "SELL", "SMART",
          "0", "", "1", "0", "a=b;"),
        f
      )
    )
  }

  test("cancelMktData and reqMarketDataType are encoded like the TWS API") {
    for
      cancel <- encoded[CancelMktData](CancelMktData(tickerId = 42))
      dataType <- encoded[ReqMarketDataType](ReqMarketDataType(marketDataType = MktDataType.Delayed))
    yield expect.same(List("2", "1", "42"), cancel) and expect.same(List("59", "1", "3"), dataType)
  }

  // ---- decoding -------------------------------------------------------------------------------------------------

  test("tickPrice decodes price, attributes and the matching size") {
    decodeOne("1", "6", "42", "1", "101.25", "300", "3").map(msg =>
      expect.same(
        TickPrice(42, TickType.BID, 101.25, TickAttrib(canAutoExecute = true, pastLimit = true),
          Some(TickSize(42, TickType.BID_SIZE, size(300)))),
        msg
      )
    )
  }

  test("delayed tick types are decoded by their TWS index (66 = delayed bid)") {
    decodeOne("1", "6", "42", "66", "99.5", "10", "0").map {
      case TickPrice(42, tickType, 99.5, _, Some(TickSize(42, sizeType, _))) =>
        expect.same(TickType.DELAYED_BID, tickType) and expect.same(TickType.DELAYED_BID_SIZE, sizeType)
      case other => failure(s"unexpected $other")
    }
  }

  test("unknown tick types decode as UNKNOWN instead of failing") {
    decodeOne("45", "6", "42", "999", "1.0").map(msg => expect.same(TickGeneric(42, TickType.UNKNOWN, 1.0), msg))
  }

  test("tickSize, tickString, tickGeneric decode") {
    decodeAll(
      frame("2", "6", "42", "8", "123456"),
      frame("46", "6", "42", "45", "1697665000"),
      frame("45", "6", "42", "49", "0")
    ).map(msgs =>
      expect.same(
        List(
          TickSize(42, TickType.VOLUME, size(123456)),
          TickString(42, TickType.LAST_TIMESTAMP, "1697665000"),
          TickGeneric(42, TickType.HALTED, 0.0)
        ),
        msgs
      )
    )
  }

  test("a trailing empty field decodes as an empty value") {
    decodeOne("46", "6", "42", "84", "").map(msg => expect.same(TickString(42, TickType.LAST_EXCH, ""), msg))
  }

  test("marketDataType skips the version field") {
    decodeOne("58", "1", "42", "3").map(msg => expect.same(MarketDataType(42, MktDataType.Delayed), msg))
  }

  test("error messages skip the version field") {
    decodeOne("4", "2", "42", "354", "Requested market data is not subscribed.", "").map(msg =>
      expect.same(ErrorDetail(42, 354, "Requested market data is not subscribed.", ""), msg)
    )
  }

  // ---- the tick stream ------------------------------------------------------------------------------------------

  private val mixed: List[ResponseMsg] = List(
    MarketDataType(42, MktDataType.Delayed),
    TickPrice(43, TickType.BID, 1.0, TickAttrib(), None),
    TickPrice(42, TickType.LAST, 101.0, TickAttrib(), None),
    ErrorDetail(-1, 2104, "Market data farm connection is OK", ""),
    ErrorDetail(42, 10167, "Displaying delayed market data", ""),
    TickSize(42, TickType.LAST_SIZE, Decimal(BigDecimal(5))),
    TickString(43, TickType.LAST_TIMESTAMP, "1"),
    TickGeneric(42, TickType.HALTED, 0.0)
  )

  test("the tick stream keeps only its own ticker and ignores warnings") {
    Stream.emits(mixed).through(MarketData.ticks[IO](42)).compile.toList.map(ticks =>
      expect.same(List(mixed(0), mixed(2), mixed(5), mixed(7)), ticks)
    )
  }

  test("the tick stream ends at the snapshot end of its ticker") {
    val msgs = List(mixed(2), TickSnapshotEnd(43), mixed(5), TickSnapshotEnd(42), mixed(7))
    Stream.emits(msgs).through(MarketData.ticks[IO](42)).compile.toList.map(ticks =>
      expect.same(List(mixed(2), mixed(5)), ticks)
    )
  }

  test("the tick stream fails on a TWS error for its ticker") {
    val msgs = List(mixed(2), ErrorDetail(42, 354, "not subscribed", ""), mixed(5))
    Stream.emits(msgs).through(MarketData.ticks[IO](42)).compile.toList.attempt.map(result =>
      expect.same(Left(MarketData.RequestFailed(42, 354, "not subscribed")), result)
    )
  }

  test("reqMktData sends the request, streams ticks and cancels when the stream stops") {
    for
      client <- fakeClient(mixed*)
      ticks <- client.reqMktData(42, spy).take(2).compile.toList
      sent <- client.sent.get
    yield expect.same(List(mixed(0), mixed(2)), ticks) and
      expect.same(List("1", "11", "42"), sent.head.take(3)) and
      expect.same(Vector(List("2", "1", "42")), sent.tail)
  }

  test("a snapshot ends by itself and is not cancelled") {
    for
      client <- fakeClient(mixed(2), TickSnapshotEnd(42), mixed(5))
      ticks <- client.reqMktData(42, spy, snapshot = true).compile.toList
      sent <- client.sent.get
    yield expect.same(List(mixed(2)), ticks) and expect.same(1, sent.size)
  }
