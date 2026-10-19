package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.Decimal

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests for HISTORICAL_DATA decoding. Frame fields are taken from the paper AAPL
  * capture in C:\Star-Lord\logs\aapl-mkt-check-20261019-121200.log (msg 17, reqId 8801).
  */
object HistoricalDataSpec extends SimpleIOSuite:

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  // Captured: unreadable message [17,8801,20261018 12:13:08 US/Eastern,20261019 12:13:08 US/Eastern,148,<bars>...]
  // Truncated to 2 bars for the unit test; values are verbatim from that log.
  private val capturedTwoBars: List[String] = List(
    "17",
    "8801",
    "20261018 12:13:08 US/Eastern",
    "20261019 12:13:08 US/Eastern",
    "2",
    "20261019 09:30:00 US/Eastern",
    "340.7800",
    "342.8700",
    "340.6800",
    "341.3300",
    "786090",
    "341.363",
    "2730",
    "20261019 09:31:00 US/Eastern",
    "341.3500",
    "341.9400",
    "340.7400",
    "341.8100",
    "139798",
    "341.408",
    "1862"
  )

  test("historicalData decodes timezone-aware date strings (server 177, captured AAPL frame)") {
    decodeWith(177, capturedTwoBars*).map {
      case HistoricalData(histories, start, end) =>
        expect.same("20261018 12:13:08 US/Eastern", start) and
          expect.same("20261019 12:13:08 US/Eastern", end) and
          expect.same(2, histories.size) and
          expect.same(8801, histories.head.reqId) and
          expect.same("20261019 09:30:00 US/Eastern", histories.head.bar.time) and
          expect.same(340.78, histories.head.bar.open) and
          expect.same(342.87, histories.head.bar.high) and
          expect.same(340.68, histories.head.bar.low) and
          expect.same(341.33, histories.head.bar.close) and
          expect.same(Decimal(BigDecimal(786090)), histories.head.bar.volume) and
          expect.same(Decimal(BigDecimal("341.363")), histories.head.bar.wap) and
          expect.same(2730, histories.head.bar.count) and
          expect.same("20261019 09:31:00 US/Eastern", histories(1).bar.time) and
          expect.same(341.81, histories(1).bar.close) and
          expect.same(1862, histories(1).bar.count)
      case other =>
        failure(s"expected HistoricalData, got $other")
    }
  }

  test("historicalData still decodes the legacy versioned layout (server 123)") {
    // version=3, reqId=7, start/end, 1 bar with hasGaps="false", barCount=10
    decodeWith(
      123,
      "17",
      "3",
      "7",
      "20200101 09:30:00",
      "20200101 16:00:00",
      "1",
      "20200101 09:30:00",
      "100.0",
      "101.0",
      "99.0",
      "100.5",
      "1000",
      "100.25",
      "false",
      "10"
    ).map {
      case HistoricalData(histories, start, end) =>
        expect.same("20200101 09:30:00", start) and
          expect.same("20200101 16:00:00", end) and
          expect.same(1, histories.size) and
          expect.same(7, histories.head.reqId) and
          expect.same(100.5, histories.head.bar.close) and
          expect.same(10, histories.head.bar.count)
      case other =>
        failure(s"expected HistoricalData, got $other")
    }
  }
