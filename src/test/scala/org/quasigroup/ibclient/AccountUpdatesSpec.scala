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

/** Unit tests (no TWS needed) for account updates: reqAccountUpdates encoding, updateAccountValue / updatePortfolio /
  * updateAccountTime / accountDownloadEnd decoding, the updates stream and the snapshot.
  */
object AccountUpdatesSpec extends SimpleIOSuite:

  private val Account = "DU1234567"

  /** A TWS portfolio value message (version 8): 100 AAPL at 151.00, bought at 150.25. */
  private val portfolioFields = List("7", "8", "265598", "AAPL", "STK", "", "0.0", "", "", "NASDAQ", "USD", "AAPL",
    "NMS", "100", "151.0", "15100.0", "150.25", "75.0", "0.0", Account)

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  private def value(key: String, v: String, account: String = Account) = UpdateAccountValue(key, v, "USD", account)
  private def line(conId: Int, position: Int, account: String = Account) =
    UpdatePortfolio(Contract(conId = conId), Decimal(BigDecimal(position)), 1.0, position.toDouble, 1.0, 0.0, 0.0,
      account)

  test("reqAccountUpdates (subscribe and unsubscribe) is encoded like the TWS API") {
    for
      on <- encoded[ReqAccountUpdates](ReqAccountUpdates(subscribe = true, acctCode = Account))
      off <- encoded[ReqAccountUpdates](ReqAccountUpdates(subscribe = false, acctCode = Account))
    yield expect.same(List("6", "2", "1", Account), on) and expect.same(List("6", "2", "0", Account), off)
  }

  test("updateAccountValue decodes (its message version is read, not skipped twice)") {
    decodeWith(177, "6", "2", "NetLiquidation", "1000000.00", "USD", Account).map(msg =>
      expect.same(UpdateAccountValue("NetLiquidation", "1000000.00", "USD", Account), msg)
    )
  }

  test("updatePortfolio decodes (message version 8, server version 177)") {
    decodeWith(177, portfolioFields*).map {
      case p: UpdatePortfolio =>
        expect.same(
          (265598, "AAPL", SecType.STK, "NASDAQ", "USD", "AAPL", "NMS"),
          (p.contract.conId, p.contract.symbol, p.contract.secType, p.contract.primaryExch, p.contract.currency,
            p.contract.localSymbol, p.contract.tradingClass)
        ) and
          expect.same((BigDecimal(100), 151.0, 15100.0, 150.25, 75.0, 0.0, Account),
            (p.position.value, p.marketPrice, p.marketValue, p.averageCost, p.unrealizedPNL, p.realizedPNL,
              p.accountName))
      case other => failure(s"unexpected $other")
    }
  }

  test("updateAccountTime and accountDownloadEnd decode") {
    for
      time <- decodeWith(177, "8", "1", "09:41")
      end <- decodeWith(177, "54", "1", Account)
    yield expect.same(UpdateAccountTime("09:41"), time) and expect.same(AccountDownloadEnd(Account), end)
  }

  test("reqAccountUpdates subscribes, streams this account's updates and unsubscribes when stopped") {
    val msgs = List(
      value("NetLiquidation", "100"),
      value("NetLiquidation", "5", account = "DU7654321"), // another account
      OpenOrderEnd, // other messages are ignored
      line(1, 10),
      UpdateAccountTime("09:41"),
      AccountDownloadEnd(Account),
      value("NetLiquidation", "101")
    )
    for
      client <- fakeClient(msgs*)
      updates <- client.reqAccountUpdates(Account).take(4).compile.toList
      sent <- client.sent.get
    yield expect.same(List(value("NetLiquidation", "100"), line(1, 10), UpdateAccountTime("09:41"),
      AccountDownloadEnd(Account)), updates) and
      expect.same(Vector(List("6", "2", "1", Account), List("6", "2", "0", Account)), sent)
  }

  test("accountSnapshot collects values and portfolio up to accountDownloadEnd, keeping the latest of each") {
    val msgs = List(
      value("NetLiquidation", "100"),
      UpdateAccountValue("CashBalance", "7", "CAD", Account),
      line(1, 10),
      line(2, 5),
      value("NetLiquidation", "102"),
      line(1, 12),
      UpdateAccountTime("09:41"),
      AccountDownloadEnd(Account),
      value("NetLiquidation", "999"), // after the end: not part of the snapshot
      line(3, 1)
    )
    for
      client <- fakeClient(msgs*)
      snap <- client.accountSnapshot(Account)
      sent <- client.sent.get
    yield expect.same(Account, snap.account) and
      expect.same(Some("102"), snap.value("NetLiquidation", "USD")) and
      expect.same(Some("7"), snap.value("CashBalance", "CAD")) and
      expect.same(Vector(line(1, 12), line(2, 5)), snap.portfolio) and
      expect.same("09:41", snap.updateTime) and
      expect.same(Vector(List("6", "2", "1", Account), List("6", "2", "0", Account)), sent)
  }

  test("an empty account code takes the account name from accountDownloadEnd") {
    for
      client <- fakeClient(value("NetLiquidation", "100"), AccountDownloadEnd(Account))
      snap <- client.accountSnapshot("")
    yield expect.same(Account, snap.account) and expect.same(Some("100"), snap.value("NetLiquidation", "USD"))
  }

  private def run(msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(Portfolio.accountUpdates(Account)).compile.toList.attempt

  test("the account updates stream fails on an unreadable account update, but not on other unreadable messages") {
    for
      badValue <- run(Unreadable(List("6", "2", "x"), "boom", 177))
      badLine <- run(Unreadable(List("7", "8", "x"), "boom", 177))
      other <- run(Unreadable(List("5", "7", "x"), "boom", 177), AccountDownloadEnd(Account))
    yield expect(badValue.isLeft) and
      expect.same(Left(Portfolio.Unreadable("account updates", List("7", "8", "x"), "boom")), badLine) and
      expect.same(Right(List(AccountDownloadEnd(Account))), other)
  }

  test("the account updates stream fails instead of stalling when the connection is lost") {
    for
      lost <- run(value("NetLiquidation", "100"), ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(ConnectionClosed)
    yield expect.same(Left(Portfolio.ConnectionLost("account updates", "[1100] Connectivity lost")), lost) and
      expect.same(Left(Portfolio.ConnectionLost("account updates", "TWS closed the connection")), closed)
  }

  test("the account updates stream fails on a TWS error without request id, but not on notices") {
    for
      failed <- run(ErrorDetail(-1, 321, "Error validating request", ""))
      notices <- run(ErrorDetail(-1, 2104, "Market data farm connection is OK", ""),
        ErrorDetail(-1, 1102, "Connectivity restored - data maintained", ""), ErrorDetail(7, 202, "Order cancelled", ""),
        AccountDownloadEnd(Account))
    yield expect.same(Left(Portfolio.RequestFailed("account updates", 321, "Error validating request")), failed) and
      expect.same(Right(List(AccountDownloadEnd(Account))), notices)
  }

  test("2100 and 2101 (account subscription ended or refused) fail the account updates stream") {
    for
      unsubscribed <- run(ErrorDetail(-1, 2100, "API client has been unsubscribed from account data.", ""))
      refused <- run(ErrorDetail(-1, 2101, "Unable to subscribe to account as the following clients are subscribed", ""))
    yield expect.same(
      Left(Portfolio.RequestFailed("account updates", 2100, "API client has been unsubscribed from account data.")),
      unsubscribed
    ) and expect(refused.isLeft)
  }

  test("a subscription stopped before accountDownloadEnd listens for that late end from before unsubscribing") {
    for
      client <- endlessFakeClient(value("NetLiquidation", "100"))
      _ <- client.reqAccountUpdates(Account, 100.millis).take(1).compile.drain
      events <- client.events.get
      // another account's late end marker does not count
      other <- endlessFakeClient(AccountDownloadEnd("DU7654321"))
      waited <- other.reqAccountUpdates(Account, 300.millis).compile.toList.timeout(100.millis).attempt.timed
      finished <- fakeClient(AccountDownloadEnd(Account))
      _ <- finished.accountSnapshot(Account)
      finishedEvents <- finished.events.get
    yield expect.same(Vector("subscribe", "send 6", "subscribe", "send 6"), events) and
      expect(waited._1 >= 300.millis) and
      // after its end marker, unsubscribing needs no extra listener
      expect.same(Vector("subscribe", "send 6", "send 6"), finishedEvents)
  }

  test("a subscription stopped before accountDownloadEnd keeps the lock until that end or the wait is over") {
    for
      noEnd <- endlessFakeClient(value("NetLiquidation", "100"))
      waited <- noEnd.reqAccountUpdates(Account, 300.millis).compile.toList.timeout(100.millis).attempt.timed
      free <- noEnd.accountUpdatesLock.lock.surround(IO.unit).timeout(1.second).attempt
      withEnd <- endlessFakeClient(value("NetLiquidation", "100"), AccountDownloadEnd(Account))
      quick <- withEnd.reqAccountUpdates(Account, 5.seconds).take(2).compile.toList.timed
      sent <- noEnd.sent.get
    yield expect(waited._2.isLeft) and expect(waited._1 >= 300.millis) and expect(free.isRight) and
      expect(quick._1 < 2.seconds) and expect.same(2, quick._2.size) and
      expect.same(Vector(List("6", "2", "1", Account), List("6", "2", "0", Account)), sent)
  }

  test("account updates subscriptions on one connection run one at a time") {
    for
      client <- fakeClient(AccountDownloadEnd(Account))
      blocked <- client.accountUpdatesLock.lock.surround(client.accountSnapshot(Account).timeout(200.millis).attempt)
      free <- client.accountSnapshot(Account)
      sent <- client.sent.get
    yield expect(blocked.isLeft) and expect.same(Account, free.account) and
      expect.same(Vector(List("6", "2", "1", Account), List("6", "2", "0", Account)), sent)
  }
