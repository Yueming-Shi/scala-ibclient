package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.given
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests (no TWS) for reqContractDetails: encoding and the details stream. */
object ContractLookupSpec extends SimpleIOSuite:

  private val es = Contract(symbol = "ES", secType = SecType.FUT, exchange = "CME", currency = "USD")

  test("reqContractDetails is encoded like the TWS API (msg 9, version 8)") {
    encoded[ReqContractDetails](ReqContractDetails(reqId = 42, contract = es)).map(f =>
      expect.same(
        List("9", "8", "42", "0", "ES", "FUT", "", "0.0", "", "", "CME", "", "USD", "", "", "0", "", "", ""),
        f
      )
    )
  }

  private val sample = ContractDetails(
    contract = es.copy(conId = 123, lastTradeDateOrContractMonth = "202612", localSymbol = "ESZ6"),
    marketName = "ES",
    minTick = 0.25,
    longName = "E-mini S&P 500",
    contractMonth = "202612"
  )

  test("the details stream keeps only its own request and ends at ContractDetailsEnd") {
    val msgs = List(
      ContractDetailsMsg(43, sample),
      ContractDetailsMsg(42, sample),
      ErrorDetail(-1, 2104, "Market data farm connection is OK", ""),
      ContractDetailsEnd(43),
      ContractDetailsEnd(42),
      ContractDetailsMsg(42, sample)
    )
    Stream.emits(msgs).through(ContractLookup.details[IO](42)).compile.toList.map(got =>
      expect.same(List(sample), got)
    )
  }

  test("the details stream fails on a TWS error for its request") {
    val msgs = List(ContractDetailsMsg(42, sample), ErrorDetail(42, 200, "No security definition", ""))
    Stream.emits(msgs).through(ContractLookup.details[IO](42)).compile.toList.attempt.map(result =>
      expect.same(Left(ContractLookup.RequestFailed(42, 200, "No security definition")), result)
    )
  }

  test("reqContractDetails sends the request and streams details until the end") {
    for
      client <- fakeClient(ContractDetailsMsg(42, sample), ContractDetailsEnd(42))
      details <- client.reqContractDetails(42, es).compile.toList
      sent <- client.sent.get
    yield expect.same(List(sample), details) and
      expect.same(List("9", "8", "42"), sent.head.take(3))
  }