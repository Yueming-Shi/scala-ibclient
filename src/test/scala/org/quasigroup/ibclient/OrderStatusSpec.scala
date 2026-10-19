package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.IO
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests (no TWS needed) for order status: orderStatus decoding and the status streams. */
object OrderStatusSpec extends SimpleIOSuite:

  private def decodeWith(serverVersion: Int, fields: String*): IO[ResponseMsg] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.lastOrError

  private def status(orderId: Int, s: Order.Status, filled: Int, remaining: Int): OrderStatusMsg =
    OrderStatusMsg(orderId, s, Decimal(BigDecimal(filled)), Decimal(BigDecimal(remaining)), 0, 1L, 0, 0, 10, "", 0)

  test("orderStatus decodes (server 176: no message version, with market cap price)") {
    decodeWith(176, "3", "7", "Submitted", "40", "60", "150.25", "3000000001", "0", "150.5", "10", "", "0").map {
      case s: OrderStatusMsg =>
        expect.same(
          (7, Order.Status.Submitted, BigDecimal(40), BigDecimal(60), 150.25, 3000000001L, 0, 150.5, 10, "", 0.0),
          (s.orderId, s.status, s.filled.value, s.remaining.value, s.avgFillPrice, s.permId, s.parentId,
            s.lastFillPrice, s.clientId, s.whyHeld, s.mktCapPrice)
        )
      case other => failure(s"unexpected $other")
    }
  }

  test("orderStatus decodes fractional quantities") {
    decodeWith(176, "3", "7", "Filled", "0.5", "0", "30000.0", "12", "0", "30000.0", "10", "", "0").map {
      case s: OrderStatusMsg => expect.same((BigDecimal("0.5"), true), (s.filled.value, s.remaining.isZero))
      case other             => failure(s"unexpected $other")
    }
  }

  test("orderStatus decodes when TWS drops the trailing empty fields") {
    decodeWith(176, "3", "7", "PreSubmitted", "0", "100", "0", "12", "0", "0", "10", "", "").map {
      case s: OrderStatusMsg => expect.same((Order.Status.PreSubmitted, "", 0.0), (s.status, s.whyHeld, s.mktCapPrice))
      case other             => failure(s"unexpected $other")
    }
  }

  test("orderStatus from an older server (130) reads the message version and has no market cap price") {
    decodeWith(130, "3", "6", "7", "Submitted", "0", "100", "0", "12", "0", "0", "10", "locate").map {
      case s: OrderStatusMsg =>
        expect.same((7, 12L, "locate", Double.MaxValue), (s.orderId, s.permId, s.whyHeld, s.mktCapPrice))
      case other => failure(s"unexpected $other")
    }
  }

  test("an unknown status decodes as Unknown") {
    decodeWith(176, "3", "7", "SomethingNew", "0", "100", "0", "12", "0", "0", "10", "", "0").map {
      case s: OrderStatusMsg => expect.same(Order.Status.Unknown, s.status)
      case other             => failure(s"unexpected $other")
    }
  }

  pureTest("an order is done when cancelled, completely filled or inactive (but not while locating shares)") {
    expect(Orders.isDone(status(7, Order.Status.Cancelled, 0, 100))) and
      expect(Orders.isDone(status(7, Order.Status.ApiCancelled, 0, 100))) and
      expect(Orders.isDone(status(7, Order.Status.Filled, 100, 0))) and
      expect(!Orders.isDone(status(7, Order.Status.Filled, 40, 60))) and
      expect(!Orders.isDone(status(7, Order.Status.Submitted, 40, 60))) and
      expect(Orders.isDone(status(7, Order.Status.Inactive, 0, 100))) and
      expect(!Orders.isDone(status(7, Order.Status.Inactive, 0, 100).copy(whyHeld = "locate")))
  }

  private val fill = List(
    status(7, Order.Status.PreSubmitted, 0, 100),
    status(8, Order.Status.Cancelled, 0, 5),
    status(7, Order.Status.Submitted, 40, 60),
    status(7, Order.Status.Filled, 100, 0),
    status(7, Order.Status.Filled, 100, 0)
  )

  test("orderStatus(id) follows one order and completes with its final status, sending nothing") {
    for
      client <- fakeClient(fill*)
      seen <- client.orderStatus(7).use(_.compile.toList)
      sent <- client.sent.get
    yield expect.same(fill.filter(_.orderId == 7).take(3), seen) and expect(sent.isEmpty)
  }

  test("orderStatus(id) completes when the order is cancelled") {
    for
      client <- fakeClient(fill*)
      seen <- client.orderStatus(8).use(_.compile.toList)
    yield expect.same(List(Order.Status.Cancelled), seen.map(_.status))
  }

  test("orderStatuses keeps the updates of all orders and nothing else") {
    val msgs = List(fill(0), ErrorDetail(-1, 2104, "Market data farm connection is OK", ""), OpenOrderEnd, fill(1))
    for
      client <- fakeClient(msgs*)
      seen <- client.orderStatuses.use(_.compile.toList)
    yield expect.same(List(fill(0), fill(1)), seen)
  }

  test("status streams fail on an unreadable order status, but not on other unreadable messages") {
    val other = Unreadable(List("1", "6", "x"), "boom")
    val broken = Unreadable(List("3", "7", "x"), "boom")
    for
      ok <- Stream(fill(0), other, fill(1)).covary[IO].through(Orders.statuses).compile.toList
      failed <- Stream(fill(0), broken, fill(1)).covary[IO].through(Orders.statuses).compile.toList.attempt
    yield expect.same(List(fill(0), fill(1)), ok) and
      expect.same(Left(Orders.UnreadableOrder(List("3", "7", "x"), "boom")), failed)
  }

  test("status streams fail instead of stalling when the connection is lost") {
    for
      lost <- Stream(fill(0), ErrorDetail(-1, 1100, "Connectivity lost", "")).covary[IO]
        .through(Orders.statusesOf(7)).compile.toList.attempt
      closed <- Stream(fill(0), ConnectionClosed).covary[IO].through(Orders.statusesOf(7)).compile.toList.attempt
    yield expect.same(Left(Orders.StatusConnectionLost("[1100] Connectivity lost")), lost) and
      expect.same(Left(Orders.StatusConnectionLost("TWS closed the connection")), closed)
  }

  private val statusFrame = frame("3", "7", "Submitted", "0", "100", "0", "12", "0", "0", "10", "", "0")

  test("the connection's message stream ends with ConnectionClosed when the socket ends") {
    IBSocketClientCats.incoming[IO](Stream.emits(statusFrame)).compile.toList.map(msgs =>
      expect.same(List(Order.Status.Submitted), msgs.collect { case s: OrderStatusMsg => s.status }) and
        expect.same(Some(ConnectionClosed), msgs.lastOption)
    )
  }

  test("the connection's message stream ends with ConnectionClosed when reading the socket fails") {
    val reads = Stream.emits(statusFrame) ++ Stream.raiseError[IO](new java.io.IOException("connection reset"))
    IBSocketClientCats.incoming[IO](reads).compile.toList.map(msgs =>
      expect.same(2, msgs.size) and expect.same(Some(ConnectionClosed), msgs.lastOption)
    )
  }

  test("following an order fails when the socket ends, instead of waiting forever") {
    IBSocketClientCats
      .incoming[IO](Stream.emits(statusFrame))
      .through(Orders.statusesOf(7))
      .compile
      .toList
      .attempt
      .map(result => expect.same(Left(Orders.StatusConnectionLost("TWS closed the connection")), result))
  }

  test("the open orders stream fails when the connection closes before openOrderEnd") {
    Stream(ConnectionClosed)
      .covary[IO]
      .through(Orders.openOrders)
      .compile
      .toList
      .attempt
      .map(result => expect.same(Left(Orders.StatusConnectionLost("TWS closed the connection")), result))
  }
