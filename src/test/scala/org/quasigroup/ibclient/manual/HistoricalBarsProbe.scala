package org.quasigroup.ibclient.manual

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.{IO, IOApp, Ref}
import cats.effect.std.Console
import cats.syntax.all.*
import fs2.Stream
import scala.concurrent.duration.*

/** Paper historical bars probe: AAPL 1D 1-min TRADES, ESZ6 1D 1-min TRADES, then ES keepUpToDate ~2m.
  * Run: sbt "Test/runMain org.quasigroup.ibclient.manual.HistoricalBarsProbe"
  */
object HistoricalBarsProbe extends IOApp.Simple:

  private final case class HistResult(
      bars: Option[HistoricalData],
      updates: Vector[HistoricalDataUpdate],
      errors: Vector[String]
  )

  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[$t] $msg"))

  private val aapl = Contract(
    symbol = "AAPL",
    secType = SecType.STK,
    currency = "USD",
    exchange = "SMART",
    primaryExch = "NASDAQ"
  )

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

  private def reqHist(
      ib: IBClient[IO],
      reqId: Int,
      contract: Contract,
      keepUpToDate: Boolean,
      listenFor: FiniteDuration
  ): IO[HistResult] =
    for
      barsRef <- Ref.of[IO, Option[HistoricalData]](None)
      updatesRef <- Ref.of[IO, Vector[HistoricalDataUpdate]](Vector.empty)
      errorsRef <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- Stream
        .resource(ib.subscribe)
        .flatMap { msgs =>
          Stream.exec(
            ib.requestOnly(
              ReqHistoricalData(
                tickerId = reqId,
                contract = contract,
                endDateTime = "",
                durationStr = "1 D",
                barSizeSetting = "1 min",
                whatToShow = WhatToShow.TRADES,
                useRTH = if contract.secType == SecType.FUT then 0 else 1,
                formatDate = 1,
                keepUpToDate = keepUpToDate,
                chartOptions = Nil
              )
            )
          ) ++ msgs.evalTap {
            case h: HistoricalData =>
              log(s"  HistoricalData bars=${h.histories.size} start=${h.startDate} end=${h.endDate}") *>
                barsRef.set(Some(h))
            case h: HistoricalDataUpdate if h.reqId == reqId =>
              log(s"  HistoricalDataUpdate ${h.bar.time} o=${h.bar.open} c=${h.bar.close} v=${h.bar.volume}") *>
                updatesRef.update(_ :+ h)
            case ErrorDetail(id, code, text, _)
                if id == reqId || id == -1 || Set(162, 217, 320, 366, 420, 10167, 10168).contains(code) =>
              val line = s"ErrorDetail id=$id code=$code msg=$text"
              log(s"  $line") *> errorsRef.update(_ :+ line)
            case Unreadable(fields, reason, _)
                if fields.headOption.contains("17") || fields.headOption.contains("90") =>
              log(s"  Unreadable hist-related: $reason fields0=${fields.take(5)}") *>
                errorsRef.update(_ :+ s"Unreadable: $reason")
            case _ => IO.unit
          }
        }
        .interruptAfter(listenFor)
        .onFinalize(ib.requestOnly(CancelHistoricalData(tickerId = reqId)).attempt.void)
        .compile
        .drain
      bars <- barsRef.get
      updates <- updatesRef.get
      errs <- errorsRef.get
    yield HistResult(bars, updates, errs)

  private def reportBars(label: String, result: HistResult): IO[Unit] =
    result.bars match
      case Some(h) =>
        val first = h.histories.headOption.map(_.bar)
        val last = h.histories.lastOption.map(_.bar)
        log(s"SUMMARY $label: GOT ${h.histories.size} bars start=${h.startDate} end=${h.endDate}") *>
          log(s"SUMMARY $label first=$first") *>
          log(s"SUMMARY $label last=$last")
      case None =>
        log(s"SUMMARY $label: NO bars; errors=${result.errors.mkString(" | ")}")

  def run: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 83))
      .use { ib =>
        for
          acct <- ib.asInstanceOf[IBSocketClientCats[IO]].managedAccounts.timeout(20.seconds)
          _ <- log(s"managedAccounts=$acct")
          _ <- log("==== AAPL 1 D / 1 min TRADES keepUpToDate=false ====")
          aaplRes <- reqHist(ib, 8801, aapl, keepUpToDate = false, listenFor = 45.seconds)
          _ <- reportBars("AAPL", aaplRes)
          _ <- log("==== ESZ6 conId=515416632 1 D / 1 min TRADES keepUpToDate=false ====")
          esRes <- reqHist(ib, 8802, esz6, keepUpToDate = false, listenFor = 45.seconds)
          _ <- reportBars("ESZ6", esRes)
          _ <- esRes.bars match
            case Some(_) =>
              log("==== ESZ6 keepUpToDate=true ~2 minutes ====") *>
                reqHist(ib, 8803, esz6, keepUpToDate = true, listenFor = 120.seconds).flatMap { kup =>
                  reportBars("ESZ6-keepUpToDate-snapshot", kup) *>
                    log(s"SUMMARY ESZ6-keepUpToDate: live updates=${kup.updates.size}") *>
                    kup.updates.headOption.traverse_(u => log(s"SUMMARY ESZ6-keepUpToDate firstUpdate=${u.bar}")) *>
                    kup.updates.lastOption.traverse_(u => log(s"SUMMARY ESZ6-keepUpToDate lastUpdate=${u.bar}"))
                }
            case None =>
              log("SKIP keepUpToDate: ESZ6 returned no bars")
          _ <- log("HistoricalBarsProbe done")
        yield ()
      }
      .handleErrorWith { e =>
        log(s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}") *> IO.raiseError(e)
      }
