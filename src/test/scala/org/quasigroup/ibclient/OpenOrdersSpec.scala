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

/** Unit tests (no TWS needed) for open orders: request encoding, openOrder / openOrderEnd decoding, the stream. */
object OpenOrdersSpec extends SimpleIOSuite:

  private val Max = "1.7976931348623157E308"
  private val IntMax = "2147483647"

  /** A TWS openOrder message (field order of TWS API 10.19+) for a paper BUY 100 AAPL LMT 150 DAY order. */
  def openOrderFields(serverVersion: Int, conditions: List[String] = List("0"), algo: List[String] = List("")): List[String] =
    List("5", "7") ++ // msgId, orderId (no message version since server 145)
      List("265598", "AAPL", "STK", "", "0", "?", "", "SMART", "USD", "AAPL", "NMS") ++ // contract
      List("BUY", "100", "LMT", "150.0", "0.0", "DAY", "", "DU1234567", "", "0", "my-ref", "10", "1234567890") ++
      List("0", "0", "0", "", "") ++ // outsideRth, hidden, discretionaryAmt, goodAfterTime, sharesAllocation
      List("", "", "") ++ (if serverVersion < 177 then List("") else Nil) ++ // FA group/method/percentage(/profile)
      List("", "", "", "", "") ++ // modelCode, goodTillDate, rule80A, percentOffset, settlingFirm
      List("0", "", "-1", "0") ++ // shortSaleSlot, designatedLocation, exemptCode, auctionStrategy
      List("", "", "", "", "") ++ // startingPrice, stockRefPrice, delta, stockRangeLower, stockRangeUpper
      List("", "0", "0", "0", "", "3", "0", "0", "", "0", "0") ++ // displaySize ... parentId, triggerMethod
      List("", "0", "", "", "0", "0") ++ // volatility, volType, dnOrderType, dnAuxPrice, continuousUpdate, refPriceType
      List("", "", "", "") ++ // trailStopPrice, trailingPercent, basisPoints, basisPointsType
      List("", "0", "0", "0") ++ // comboLegsDescrip, #comboLegs, #orderComboLegs, #smartComboRoutingParams
      List("", "", "") ++ // scaleInitLevelSize, scaleSubsLevelSize, scalePriceIncrement
      List("", "0", "", "IB", "0", "0") ++ // hedgeType, optOutSmartRouting, clearingAccount/Intent, notHeld, dnContract
      algo ++
      List("0", "0", "PreSubmitted") ++ List.fill(9)(Max) ++ List(Max, Max, Max, "", "") ++ // order state
      List("0", "0") ++ // randomizeSize, randomizePrice
      conditions ++
      List("None", Max, Max, Max, Max, Max, Max, "0") ++ // adjusted order fields
      List("", "", "") ++ // soft dollar tier
      List("0.0", "1", "0", "0", "0") ++ // cashQty, dontUseAutoPriceForHedge, isOmsContainer, discrUpToLimit, priceMgmt
      List(IntMax, IntMax, "0") ++ // duration, postToAts, autoCancelParent
      List("5", IntMax, Max, Max, "0.02") // minTradeQty, minCompeteSize, competeAgainstBestOffset, midOffsets

  private def decodeWith(serverVersion: Int, fields: List[String]): IO[List[ResponseMsg]] =
    given IBClient.ServerVersion = IBClient.ServerVersion(serverVersion)
    Stream.emits(frame(fields*)).through(IBSocketClientCats.responses[IO]).compile.toList

  private def checkBasics(o: OpenOrder) =
    expect.same(7, o.orderId) and
      expect.same(
        (265598, "AAPL", SecType.STK, "SMART", "USD", "AAPL", "NMS"),
        (o.contract.conId, o.contract.symbol, o.contract.secType, o.contract.exchange, o.contract.currency,
          o.contract.localSymbol, o.contract.tradingClass)
      ) and
      expect.same((Action.BUY, Order.Type.LMT, 150.0, 0.0, TimeInForce.DAY), (o.order.action, o.order.orderType,
        o.order.lmtPrice, o.order.auxPrice, o.order.tif)) and
      expect(o.order.totalQuantity == Decimal(BigDecimal(100))) and
      expect.same(("DU1234567", "my-ref", 10, 1234567890), (o.order.account, o.order.orderRef, o.order.clientId,
        o.order.permId)) and
      expect.same(Order.Status.PreSubmitted, o.orderState.status) and
      expect.same(Order.Type.None, o.order.adjustedOrderType) and
      // the last fields only line up if every field before them was read correctly
      expect.same((5, Int.MaxValue, 0.02), (o.order.minTradeQty, o.order.minCompeteSize, o.order.midOffsetAtHalf))

  test("reqOpenOrders and reqAllOpenOrders are encoded like the TWS API") {
    for
      open <- encoded[ReqOpenOrders](ReqOpenOrders())
      all <- encoded[ReqAllOpenOrders](ReqAllOpenOrders())
    yield expect.same(List("5", "1"), open) and expect.same(List("16", "1"), all)
  }

  test("openOrder decodes (server version 176, with FA profile field)") {
    decodeWith(176, openOrderFields(176)).map {
      case List(o: OpenOrder) => checkBasics(o) and expect(o.order.conditionFields.isEmpty)
      case other              => failure(s"unexpected $other")
    }
  }

  test("openOrder decodes (server version 177, FA profile field removed)") {
    decodeWith(177, openOrderFields(177)).map {
      case List(o: OpenOrder) => checkBasics(o)
      case other              => failure(s"unexpected $other")
    }
  }

  test("openOrder with a price condition reads the condition and its two flags") {
    // 1 condition: type 1 (price), "a" = and, isMore, price, conId, exchange, triggerMethod; then ignoreRth, cancelOrder
    val condition = List("1", "1", "a", "1", "155.5", "265598", "SMART", "2", "1", "0")
    decodeWith(176, openOrderFields(176, conditions = condition)).map {
      case List(o: OpenOrder) =>
        checkBasics(o) and expect.same(
          Some(Order.ConditionParams(List(PriceCondition(true, true, 155.5, 265598, "SMART", TriggerMethod.Last)),
            conditionsIgnoreRth = true, conditionsCancelOrder = false)),
          o.order.conditionFields
        )
      case other => failure(s"unexpected $other")
    }
  }

  test("openOrder with an algo strategy reads its parameters") {
    val algo = List("Vwap", "2", "maxPctVol", "0.1", "noTakeLiq", "1")
    decodeWith(176, openOrderFields(176, algo = algo)).map {
      case List(o: OpenOrder) =>
        checkBasics(o) and expect.same(
          Some(Order.AlgoParams(AlgoStrategy.Vwap, List(TagValue("maxPctVol", "0.1"), TagValue("noTakeLiq", "1")), "")),
          o.order.algoFields
        )
      case other => failure(s"unexpected $other")
    }
  }

  test("openOrderEnd decodes") {
    decodeOne("53", "1").map(msg => expect.same(OpenOrderEnd, msg))
  }

  test("the open orders stream completes at openOrderEnd") {
    for
      orders <- decodeWith(176, openOrderFields(176)).map(_.collect { case o: OpenOrder => o })
      order = orders.head
      msgs = List(order, OrderStatusMsg(7, Order.Status.PreSubmitted, Decimal(BigDecimal(0)),
        Decimal(BigDecimal(100)), 0, 1234567890, 0, 0, 10, "", Double.MaxValue), order.copy(orderId = 8), OpenOrderEnd,
        order.copy(orderId = 9))
      client <- fakeClient(msgs*)
      open <- client.reqOpenOrders.compile.toList
      all <- client.reqAllOpenOrders.compile.toList
      sent <- client.sent.get
    yield expect.same(List(7, 8), open.map(_.orderId)) and expect.same(List(7, 8), all.map(_.orderId)) and
      expect.same(Vector(List("5", "1"), List("16", "1")), sent)
  }

  test("a message that cannot be read is skipped by the live connection instead of stopping it") {
    val broken = Array("5", "7", "265598", "AAPL", "NOT_A_SEC_TYPE")
    val good = Array("53", "1")
    Stream(broken, good).through(IBSocketClientCats.decodeOrSkip[IO]).compile.toList.map(msgs =>
      expect.same(List(OpenOrderEnd), msgs)
    )
  }
