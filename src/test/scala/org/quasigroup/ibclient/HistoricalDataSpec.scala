package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.*
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.Decimal

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

import scala.io.Source

/** Unit tests for HISTORICAL_DATA decoding. The primary fixture is the full paper AAPL
  * capture from C:\Star-Lord\logs\aapl-mkt-check-20261019-121200.log (msg 17, reqId 8801, 148 bars).
  */
object HistoricalDataSpec extends SimpleIOSuite:

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  private def capturedAaplFields: List[String] =
    val src = Source.fromResource("aapl-historical-msg17-captured.txt")
    try src.getLines().toList.map(_.trim).filter(_.nonEmpty)
    finally src.close()

  test("historicalData decodes the full captured AAPL frame (server 177, 148 bars)") {
    val fields = capturedAaplFields
    decodeWith(177, fields*).map {
      case HistoricalData(histories, start, end) =>
        expect.same(1189, fields.size) and
          expect.same("20261018 12:13:08 US/Eastern", start) and
          expect.same("20261019 12:13:08 US/Eastern", end) and
          expect.same(148, histories.size) and
          expect.same(8801, histories.head.reqId) and
          expect.same("20261019 09:30:00 US/Eastern", histories.head.bar.time) and
          expect.same(340.78, histories.head.bar.open) and
          expect.same(342.87, histories.head.bar.high) and
          expect.same(340.68, histories.head.bar.low) and
          expect.same(341.33, histories.head.bar.close) and
          expect.same(Decimal(BigDecimal(786090)), histories.head.bar.volume) and
          expect.same(Decimal(BigDecimal("341.363")), histories.head.bar.wap) and
          expect.same(2730, histories.head.bar.count) and
          expect.same("20261019 11:57:00 US/Eastern", histories.last.bar.time) and
          expect.same(342.51, histories.last.bar.open) and
          expect.same(342.52, histories.last.bar.high) and
          expect.same(342.30, histories.last.bar.low) and
          expect.same(342.31, histories.last.bar.close) and
          expect.same(Decimal(BigDecimal(37759)), histories.last.bar.volume) and
          expect.same(Decimal(BigDecimal("342.415")), histories.last.bar.wap) and
          expect.same(423, histories.last.bar.count)
      case other =>
        failure(s"expected HistoricalData, got $other")
    }
  }

  test("historicalData still decodes the legacy versioned layout (server 123)") {
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
