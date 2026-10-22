package org.quasigroup.ibclient.manual

import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.{IO, IOApp, Ref}
import cats.effect.std.Console
import cats.syntax.all.*
import fs2.Stream
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.*

/** Read-only paper probe: reqMarketDataType(3 = delayed), then stream ESZ6 and SPY quotes for up to 60 s,
  * logging every tick and error for those tickers, then cancel both and disconnect. Places no orders.
  * Client id 83, paper port 7497 only.
  * Run: sbt "Test/runMain org.quasigroup.ibclient.manual.DelayedQuotesProbe"
  */
object DelayedQuotesProbe extends IOApp.Simple:

  private val toronto = ZoneId.of("America/Toronto")
  private val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[${fmt.format(t.atZone(toronto))} Toronto] $msg"))

  private val esz6 = Contract(
    conId = 515416632,
    symbol = "ES",
    secType = SecType.FUT,
    currency = "USD",
    exchange = "CME",
    lastTradeDateOrContractMonth = "20261218",
    localSymbol = "ESZ6",
    tradingClass = "ES"
  )
  private val spy = Contract(symbol = "SPY", secType = SecType.STK, exchange = "SMART", currency = "USD")

  private val EsId = 8301
  private val SpyId = 8302
  private val names = Map(EsId -> "ESZ6", SpyId -> "SPY")

  private def req(id: Int, c: Contract): ReqMktData =
    ReqMktData(tickerId = id, contract = c, genericTickList = "", snapshot = false, regulatorySnapshot = false,
      mktDataOptions = Nil)

  private def describe(m: ResponseMsg): Option[(Int, String)] = m match
    case t: TickPrice if names.contains(t.tickerId) => Some(t.tickerId -> s"TickPrice ${t.priceTickType}(${t.priceTickType.index}) price=${t.price}")
    case t: TickSize if names.contains(t.tickerId) => Some(t.tickerId -> s"TickSize ${t.sizeTickType}(${t.sizeTickType.index}) size=${t.size}")
    case t: TickString if names.contains(t.tickerId) => Some(t.tickerId -> s"TickString ${t.tickType}(${t.tickType.index}) value=${t.value}")
    case t: TickGeneric if names.contains(t.tickerId) => Some(t.tickerId -> s"TickGeneric ${t.tickType}(${t.tickType.index}) value=${t.value}")
    case t: TickReqParams if names.contains(t.tickerId) => Some(t.tickerId -> s"TickReqParams $t")
    case MarketDataType(id, dt) if names.contains(id) => Some(id -> s"MarketDataType $dt (${dt.ordinal})")
    case ErrorDetail(id, code, text, _) if names.contains(id) || id == -1 => Some(id -> s"Error code=$code msg=$text")
    case _ => None

  /** IB_MD_TYPE=4 asks for delayed-frozen instead (last delayed values when the market is closed). */
  private val mdType: MktDataType =
    sys.env.get("IB_MD_TYPE").flatMap(_.toIntOption).filter(i => i >= 1 && i <= 4).map(MktDataType.fromOrdinal)
      .getOrElse(MktDataType.Delayed)

  def run: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 83)) // defaults: 127.0.0.1, PaperPort 7497
      .use { ib =>
        for
          acct <- ib.asInstanceOf[IBSocketClientCats[IO]].managedAccounts.timeout(20.seconds)
          _ <- log(s"connected clientId=83 managedAccounts=$acct")
          ct <- ib.reqCurrentTime.timeout(10.seconds).attempt
          _ <- log(s"reqCurrentTime -> $ct")
          counts <- Ref.of[IO, Map[Int, Int]](Map.empty)
          _ <- Stream
            .resource(ib.subscribe)
            .flatMap { msgs =>
              Stream.exec(
                log(s"send reqMarketDataType(${mdType.ordinal} = $mdType)") *> ib.reqMarketDataType(mdType) *>
                  log(s"send reqMktData $EsId ESZ6") *> ib.requestOnly[ReqMktData](req(EsId, esz6)) *>
                  log(s"send reqMktData $SpyId SPY") *> ib.requestOnly[ReqMktData](req(SpyId, spy))
              ) ++ msgs.evalMap(m =>
                describe(m).traverse_ { case (id, line) =>
                  log(s"${names.getOrElse(id, s"id=$id")}: $line") *> counts.update(c => c.updated(id, c.getOrElse(id, 0) + 1))
                }
              )
            }
            .interruptAfter(60.seconds)
            .compile
            .drain
          _ <- log("cancel both subscriptions")
          _ <- ib.cancelMktData(EsId).attempt
          _ <- ib.cancelMktData(SpyId).attempt
          _ <- IO.sleep(1.second)
          c <- counts.get
          _ <- log(s"SUMMARY messages ESZ6=${c.getOrElse(EsId, 0)} SPY=${c.getOrElse(SpyId, 0)} system=${c.getOrElse(-1, 0)}")
        yield ()
      }
      .flatMap(_ => log("disconnected"))
