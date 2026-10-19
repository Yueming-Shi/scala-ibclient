package org.quasigroup.ibclient.response.readers

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.IBClient.MIN_SERVER_VER_SIZE_RULES
import org.quasigroup.ibclient.decoder.Decoder.{DecoderState, read, readNothing}
import org.quasigroup.ibclient.response.ResponseMsg.ContractDetailsMsg
import org.quasigroup.ibclient.types.*
import org.quasigroup.ibclient.types.TypesCodec.given

object ContractDataReader {
  def create(using serverVersion: IBClient.ServerVersion): DecoderState[ContractDetailsMsg] =
    if serverVersion >= MIN_SERVER_VER_SIZE_RULES then createModern else createLegacy

  /** TWS 10.19+ / server 164+: no message version; multiplier is a string after minTick (mdSizeMultiplier gone). */
  private def createModern(using serverVersion: IBClient.ServerVersion): DecoderState[ContractDetailsMsg] =
    for
      reqId <- read[Int]
      symbol <- read[String]
      secType <- read[SecType]
      lastTradeDateOrContractMonth <- read[String]
      strike <- read[Double]
      right <- read[ContractRight]
      exchange <- read[String]
      currency <- read[String]
      localSymbol <- read[String]
      marketName <- read[String]
      tradingClass <- read[String]
      conid <- read[Int]
      minTick <- read[Double]
      multiplier <- read[String]
      orderTypes <- read[String]
      validExchanges <- read[String]
      priceMagnifier <- read[Int]
      underConId <- read[Int]
      longName <- read[String]
      primaryExch <- read[String]
      contractMonthField <- read[String]
      industry <- read[String]
      category <- read[String]
      subcategory <- read[String]
      timeZoneId <- read[String]
      tradingHours <- read[String]
      liquidHours <- read[String]
      evRule <- read[String]
      evMultiplier <- read[Double]
      secIdListCount <- read[Int]
      secIdList <- (0 until secIdListCount).foldLeft(readNothing(List.empty[TagValue])) { (state, _) =>
        for
          list <- state
          tag <- read[String]
          value <- read[String]
        yield TagValue(tag, value) :: list
      }
      aggGroup <- read[Int]
      underSymbol <- read[String]
      underSecType <- read[SecType]
      marketRuleIds <- read[String]
      realExpirationDate <- read[String]
      stockType <- read[String]
      minSize <- read[Decimal]
      sizeIncrement <- read[Decimal]
      suggestedSizeIncrement <- read[Decimal]
    yield build(
      reqId,
      symbol,
      secType,
      lastTradeDateOrContractMonth,
      strike,
      right,
      exchange,
      currency,
      localSymbol,
      tradingClass,
      conid,
      multiplier,
      primaryExch,
      marketName,
      minTick,
      orderTypes,
      validExchanges,
      priceMagnifier,
      underConId,
      longName,
      contractMonthField,
      industry,
      category,
      subcategory,
      timeZoneId,
      tradingHours,
      liquidHours,
      evRule,
      evMultiplier,
      secIdList,
      aggGroup,
      underSymbol,
      underSecType,
      marketRuleIds,
      realExpirationDate,
      stockType,
      minSize,
      sizeIncrement,
      suggestedSizeIncrement
    )

  private def createLegacy(using serverVersion: IBClient.ServerVersion): DecoderState[ContractDetailsMsg] =
    for
      version <- read[Int]
      reqId <- if version >= 3 then read[Int] else readNothing(-1)
      symbol <- read[String]
      secType <- read[SecType]
      lastTradeDateOrContractMonth <- read[String]
      strike <- read[Double]
      right <- read[ContractRight]
      exchange <- read[String]
      currency <- read[String]
      localSymbol <- read[String]
      marketName <- read[String]
      tradingClass <- read[String]
      conid <- read[Int]
      minTick <- read[Double]
      _ <- read[Int] // mdSizeMultiplier - not used anymore
      orderTypes <- read[String]
      validExchanges <- read[String]
      priceMagnifier <- if version >= 2 then read[Int] else readNothing(0)
      underConId <- if version >= 4 then read[Int] else readNothing(-1)
      longName <- if version >= 5 then read[String] else readNothing("")
      primaryExch <- if version >= 5 then read[String] else readNothing("")
      contractMonthField <- if version >= 6 then read[String] else readNothing("")
      industry <- if version >= 6 then read[String] else readNothing("")
      category <- if version >= 6 then read[String] else readNothing("")
      subcategory <- if version >= 6 then read[String] else readNothing("")
      timeZoneId <- if version >= 6 then read[String] else readNothing("")
      tradingHours <- if version >= 6 then read[String] else readNothing("")
      liquidHours <- if version >= 6 then read[String] else readNothing("")
      evRule <- if version >= 8 then read[String] else readNothing("")
      evMultiplier <- if version >= 8 then read[Double] else readNothing(Double.MaxValue)
      secIdListCount <- if version >= 7 then read[Int] else readNothing(0)
      secIdList <- (0 until secIdListCount).foldLeft(readNothing(List.empty[TagValue])) { (state, _) =>
        for
          list <- state
          tag <- read[String]
          value <- read[String]
        yield TagValue(tag, value) :: list
      }
      aggGroup <- read[Int]
      underSymbol <- read[String]
      underSecType <- read[SecType]
      marketRuleIds <- read[String]
      realExpirationDate <- read[String]
      stockType <- read[String]
      _ <- read[Decimal] // sizeMinTick - not used anymore
      minSize <- read[Decimal]
      sizeIncrement <- read[Decimal]
      suggestedSizeIncrement <- read[Decimal]
    yield build(
      reqId,
      symbol,
      secType,
      lastTradeDateOrContractMonth,
      strike,
      right,
      exchange,
      currency,
      localSymbol,
      tradingClass,
      conid,
      "",
      primaryExch,
      marketName,
      minTick,
      orderTypes,
      validExchanges,
      priceMagnifier,
      underConId,
      longName,
      contractMonthField,
      industry,
      category,
      subcategory,
      timeZoneId,
      tradingHours,
      liquidHours,
      evRule,
      evMultiplier,
      secIdList,
      aggGroup,
      underSymbol,
      underSecType,
      marketRuleIds,
      realExpirationDate,
      stockType,
      minSize,
      sizeIncrement,
      suggestedSizeIncrement
    )

  private def build(
      reqId: Int,
      symbol: String,
      secType: SecType,
      lastTradeDateOrContractMonth: String,
      strike: Double,
      right: ContractRight,
      exchange: String,
      currency: String,
      localSymbol: String,
      tradingClass: String,
      conid: Int,
      multiplier: String,
      primaryExch: String,
      marketName: String,
      minTick: Double,
      orderTypes: String,
      validExchanges: String,
      priceMagnifier: Int,
      underConId: Int,
      longName: String,
      contractMonthField: String,
      industry: String,
      category: String,
      subcategory: String,
      timeZoneId: String,
      tradingHours: String,
      liquidHours: String,
      evRule: String,
      evMultiplier: Double,
      secIdList: List[TagValue],
      aggGroup: Int,
      underSymbol: String,
      underSecType: SecType,
      marketRuleIds: String,
      realExpirationDate: String,
      stockType: String,
      minSize: Decimal,
      sizeIncrement: Decimal,
      suggestedSizeIncrement: Decimal
  ): ContractDetailsMsg =
    val (parsedMonth, lastTradeTime) =
      if lastTradeDateOrContractMonth.nonEmpty then
        val splitted =
          if lastTradeDateOrContractMonth.contains("-") then lastTradeDateOrContractMonth.split("-")
          else lastTradeDateOrContractMonth.split("\\s+")
        splitted match
          case Array(contractMonth, lastTradeTime, _*) => (contractMonth, lastTradeTime)
          case Array(contractMonth, _*)                => (contractMonth, "")
          case _                                       => ("", "")
      else ("", "")
    val month = if contractMonthField.nonEmpty then contractMonthField else parsedMonth
    val contract = Contract(
      conId = conid,
      symbol = symbol,
      secType = secType,
      strike = strike,
      right = right,
      multiplier = multiplier,
      exchange = exchange,
      primaryExch = primaryExch,
      currency = currency,
      localSymbol = localSymbol,
      tradingClass = tradingClass,
      lastTradeDateOrContractMonth = parsedMonth
    )
    ContractDetailsMsg(
      reqId,
      ContractDetails(
        contract = contract,
        priceMagnifier = priceMagnifier,
        underConId = underConId,
        underSymbol = underSymbol,
        underSecType = underSecType,
        minTick = minTick,
        marketName = marketName,
        orderTypes = orderTypes,
        validExchanges = validExchanges,
        contractMonth = month,
        subcategory = subcategory,
        category = category,
        industry = industry,
        longName = longName,
        evMultiplier = evMultiplier,
        evRule = evRule,
        secIdList = secIdList,
        aggGroup = aggGroup,
        marketRuleIds = marketRuleIds,
        lastTradeTime = lastTradeTime,
        liquidHours = liquidHours,
        tradingHours = tradingHours,
        timeZoneId = timeZoneId,
        stockType = stockType,
        realExpirationDate = realExpirationDate,
        minSize = minSize,
        sizeIncrement = sizeIncrement,
        suggestedSizeIncrement = suggestedSizeIncrement
      )
    )
}