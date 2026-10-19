package org.quasigroup.ibclient

import org.quasigroup.ibclient.Wire.{*, given}
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.impl.IBSocketClientCats.*
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.given
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.IO
import com.comcast.ip4s.*
import fs2.Stream
import weaver.SimpleIOSuite

/** Unit tests (no TWS needed) for placing and cancelling orders: paper-session guard, placeOrder / cancelOrder /
  * reqGlobalCancel / nextValidOrderId encoding, and the order-events stream.
  */
object PlaceOrdersSpec extends SimpleIOSuite:

  private val Account = "DU1234567"
  private val Spy =
    Contract(symbol = "SPY", secType = SecType.STK, currency = "USD", exchange = "SMART")

  private def limitBuy(price: Double): Order =
    Order(
      action = Action.BUY,
      totalQuantity = Decimal.ONE,
      orderType = Order.Type.LMT,
      lmtPrice = price,
      auxPrice = 0.0,
      tif = TimeInForce.DAY,
      account = Account,
      openClose = "",
      discretionaryAmt = 0.0
    )

  private def status(orderId: Int, s: Order.Status): OrderStatusMsg =
    OrderStatusMsg(orderId, s, Decimal.ZERO, Decimal.ONE, 0, 1L, 0, 0, 10, "", 0)

  private def open(orderId: Int): OpenOrder =
    OpenOrder(orderId, Spy, limitBuy(1.0).copy(orderId = orderId), Order.State(Order.Status.PreSubmitted))

  pureTest("order placement is allowed only on paper port 7497 with paper accounts and no live opt-in") {
    val paper = port"7497"
    val live = port"7496"
    val gatewayPaper = port"4002"
    expect(checkPaperOrders(paper, LiveTrading.Refused, "DUR280040").isRight) and
      expect(checkPaperOrders(live, LiveTrading.Refused, "DUR280040").isLeft) and
      expect(checkPaperOrders(gatewayPaper, LiveTrading.Refused, "DUR280040").isLeft) and
      expect(checkPaperOrders(paper, LiveTrading.IUnderstandThisIsARealMoneyAccount, "DUR280040").isLeft) and
      expect(checkPaperOrders(paper, LiveTrading.Refused, "U1234567").isLeft) and
      expect(checkPaperOrders(paper, LiveTrading.Refused, "DUR280040,U1").isLeft)
  }

  test("cancelOrder at server version 177 sends the manual cancel time field (empty by default)") {
    for
      withTime <- encoded[CancelOrder](CancelOrder(id = 7, manualCancelOrderTime = "20261019-09:30:00"))
      empty <- encoded[CancelOrder](CancelOrder(id = 7))
    yield expect.same(List("4", "1", "7", "20261019-09:30:00"), withTime) and
      expect.same(List("4", "1", "7", ""), empty)
  }

  test("cancelOrder on an older server (168) has no manual cancel time field") {
    given IBClient.ServerVersion = IBClient.ServerVersion(168)
    org.quasigroup.ibclient.request.MsgEncoders
      .encode[IO, CancelOrder](CancelOrder(id = 7))
      .map(Wire.fields)
      .map(fields => expect.same(List("4", "1", "7"), fields))
  }

  test("reqGlobalCancel and reqIds are encoded like the TWS API") {
    for
      global <- encoded[ReqGlobalCancel](ReqGlobalCancel())
      ids <- encoded[ReqIds](ReqIds(numIds = 1))
    yield expect.same(List("58", "1"), global) and expect.same(List("8", "1", "1"), ids)
  }

  test("placeOrder for a 1-share LMT DAY at server version 177 starts with the expected contract and order fields") {
    // msgId 3, no message version (server >= 145), orderId, contract..., action, qty, type, lmt, aux, tif, ...
    encoded[PlaceOrder](PlaceOrder(id = 42, contract = Spy, order = limitBuy(1.0).copy(orderId = 42))).map { fields =>
      expect.same("3", fields.head) and
        expect.same("42", fields(1)) and
        expect.same(List("0", "SPY", "STK", "", "0.0", "", "", "SMART", "", "USD", "", ""), fields.slice(2, 14)) and
        expect.same(List("", ""), fields.slice(14, 16)) and // secIdType, secId
        expect.same(List("BUY", "1", "LMT", "1.0", "0.0", "DAY"), fields.slice(16, 22)) and
        expect.same(Account, fields(23)) and // after ocaGroup
        expect(!fields.contains("MKT")) and
        expect(fields.length > 40) // the rest of the v177 body is present
    }
  }

  test("placeOrder streams openOrder and status until Cancelled, and cancelOrder / nextValidOrderId send the right messages") {
    val msgs = List(
      open(42),
      status(43, Order.Status.Submitted), // other order
      status(42, Order.Status.PreSubmitted),
      status(42, Order.Status.Submitted),
      ErrorDetail(42, 202, "Order Canceled - reason:", ""),
      status(42, Order.Status.Cancelled),
      status(42, Order.Status.Cancelled) // after done: not part of the stream
    )
    for
      client <- fakeClient(msgs*)
      events <- client.placeOrder(42, Spy, limitBuy(1.0)).compile.toList
      _ <- client.cancelOrder(42)
      sent <- client.sent.get
      client2 <- fakeClient(NextValidId(99))
      id <- client2.nextValidOrderId
      sent2 <- client2.sent.get
    yield expect.same(
      List(open(42), status(42, Order.Status.PreSubmitted), status(42, Order.Status.Submitted),
        ErrorDetail(42, 202, "Order Canceled - reason:", ""), status(42, Order.Status.Cancelled)),
      events
    ) and expect(sent.head.head == "3") and expect.same(List("4", "1", "42", ""), sent(1)) and
      expect.same(Vector(List("8", "1", "1")), sent2) and expect.same(99, id)
  }

  test("placeOrder completes on Filled and ignores other orders' updates") {
    val filled = status(42, Order.Status.Filled).copy(filled = Decimal.ONE, remaining = Decimal.ZERO)
    val msgs: List[ResponseMsg] = List(
      status(7, Order.Status.Submitted),
      status(42, Order.Status.Submitted),
      filled,
      filled
    )
    for
      client <- fakeClient(msgs*)
      events <- client.placeOrder(42, Spy, limitBuy(1.0)).compile.toList
    yield expect.same(List(status(42, Order.Status.Submitted), filled), events)
  }

  private def run(orderId: Int, msgs: ResponseMsg*) =
    Stream.emits(msgs).covary[IO].through(Orders.eventsOf(orderId)).compile.toList.attempt

  test("the order events stream fails when the connection is lost") {
    for
      lost <- run(42, status(42, Order.Status.Submitted), ErrorDetail(-1, 1100, "Connectivity lost", ""))
      closed <- run(42, ConnectionClosed)
    yield expect(lost.isLeft) and expect(closed.isLeft)
  }
