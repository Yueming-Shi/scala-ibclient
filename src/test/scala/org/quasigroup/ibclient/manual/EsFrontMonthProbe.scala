package org.quasigroup.ibclient.manual

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.MarketData
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.{IO, IOApp}
import cats.effect.std.Console
import cats.syntax.all.*
import scala.concurrent.duration.*

/** Resolve ES front-month via reqContractDetails and probe market data. Paper 7497 only. 
  * Manual tool under org.quasigroup.ibclient.manual (Test/runMain only; not weaver).*/
object EsFrontMonthProbe extends IOApp.Simple:
  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[$t] $msg"))

  private val esPartial =
    Contract(symbol = "ES", secType = SecType.FUT, exchange = "CME", currency = "USD")

  def run: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 79))
      .use { ib =>
        for
          acct <- ib.asInstanceOf[IBSocketClientCats[IO]].managedAccounts.timeout(20.seconds)
          _ <- log(s"managedAccounts=$acct")
          details <- ib.reqContractDetails(7901, esPartial).compile.toList.timeout(30.seconds)
          _ <- log(s"contractDetails count=${details.size}")
          _ <- details.sortBy(_.contract.lastTradeDateOrContractMonth).traverse_ { d =>
            log(
              s"  conId=${d.contract.conId} local=${d.contract.localSymbol} expiry=${d.contract.lastTradeDateOrContractMonth} " +
                s"month=${d.contractMonth} exchange=${d.contract.exchange} tradingClass=${d.contract.tradingClass} minTick=${d.minTick}"
            )
          }
          front <- IO.fromOption(
            details
              .filter(_.contract.lastTradeDateOrContractMonth.nonEmpty)
              .sortBy(_.contract.lastTradeDateOrContractMonth)
              .headOption
          )(new RuntimeException("no ES futures returned"))
          _ <- log(s"front-month chosen: conId=${front.contract.conId} local=${front.contract.localSymbol} expiry=${front.contract.lastTradeDateOrContractMonth}")
          _ <- ib.reqMarketDataType(MktDataType.Delayed)
          ticks <- ib
            .reqMktData(7902, front.contract)
            .evalTap(t => log(s"  tick: $t"))
            .interruptAfter(20.seconds)
            .compile
            .toList
            .attempt
          _ <- ticks match
            case Left(e: MarketData.RequestFailed) =>
              log(s"ES MD RequestFailed code=${e.errorCode} msg=${e.errorMsg}")
            case Left(e) => log(s"ES MD error: ${e.getClass.getSimpleName}: ${e.getMessage}")
            case Right(list) =>
              val types = list.map {
                case t: TickPrice => s"TickPrice(${t.priceTickType})"
                case t: TickSize  => s"TickSize(${t.sizeTickType})"
                case t: TickReqParams => "TickReqParams"
                case MarketDataType(_, dt) => s"MarketDataType($dt)"
                case other => other.getClass.getSimpleName
              }.distinct
              log(s"ES MD messages=${list.size} types=${types.mkString(",")}")
              IO.whenA(list.collect { case t: TickPrice => t }.isEmpty)(
                log("ES MD: NO price ticks (paper likely needs delayed MD enabled and/or CME subscription)")
              )
        yield ()
      }