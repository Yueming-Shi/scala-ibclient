package org.quasigroup.ibclient.manual

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.MarketData
import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import cats.effect.{IO, IOApp, Ref}
import cats.effect.std.Console
import cats.syntax.all.*
import fs2.Stream
import scala.concurrent.duration.*

/** Paper AAPL market-data probe: live / delayed / delayed-frozen for ~20s each, then 1-day 1-min historical. 
  * Manual tool under org.quasigroup.ibclient.manual (Test/runMain only; not weaver).*/
object AaplMktCheck extends IOApp.Simple:

  private val aapl = Contract(
    symbol = "AAPL",
    secType = SecType.STK,
    currency = "USD",
    exchange = "SMART",
    primaryExch = "NASDAQ"
  )

  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[$t] $msg"))

  private def summarize(msg: ResponseMsg): String =
    msg match
      case t: TickPrice =>
        s"TickPrice tickerId=${t.tickerId} type=${t.priceTickType} price=${t.price} size=${t.tickSize.map(_.size)}"
      case t: TickSize =>
        s"TickSize tickerId=${t.tickerId} type=${t.sizeTickType} size=${t.size}"
      case t: TickString =>
        s"TickString tickerId=${t.tickerId} type=${t.tickType} value=${t.value}"
      case t: TickGeneric =>
        s"TickGeneric tickerId=${t.tickerId} type=${t.tickType} value=${t.value}"
      case t: TickReqParams =>
        s"TickReqParams tickerId=${t.tickerId} minTick=${t.minTick} bbo=${t.bboExchange} snapshotPermissions=${t.snapshotPermissions}"
      case MarketDataType(id, dt) => s"MarketDataType reqId=$id type=$dt"
      case ErrorDetail(id, code, text, adv) => s"ErrorDetail id=$id code=$code msg=$text advanced=$adv"
      case TickSnapshotEnd(id) => s"TickSnapshotEnd tickerId=$id"
      case h: HistoricalData =>
        s"HistoricalData bars=${h.histories.size} start=${h.startDate} end=${h.endDate}"
      case h: HistoricalDataUpdate =>
        s"HistoricalDataUpdate reqId=${h.reqId} bar=${h.bar}"
      case other => other.toString.take(240)

  private def keepForTicker(tickerId: Int)(m: ResponseMsg): Boolean =
    m match
      case t: TickPrice if t.tickerId == tickerId => true
      case t: TickSize if t.tickerId == tickerId => true
      case t: TickString if t.tickerId == tickerId => true
      case t: TickGeneric if t.tickerId == tickerId => true
      case t: TickReqParams if t.tickerId == tickerId => true
      case MarketDataType(id, _) if id == tickerId || id == -1 => true
      case ErrorDetail(id, code, _, _)
          if id == tickerId || id == -1 || Set(354, 10167, 10168, 10089, 10091, 10197, 320, 162).contains(code) =>
        true
      case TickSnapshotEnd(`tickerId`) => true
      case _ => false

  private def probeMode(ib: IBClient[IO], mode: MktDataType, tickerId: Int): IO[Unit] =
    for
      _ <- log(s"==== MODE $mode (ordinal=${mode.ordinal}) tickerId=$tickerId streaming snapshot=false ~20s ====")
      _ <- ib.reqMarketDataType(mode)
      seen <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- ib
        .reqMktData(tickerId, aapl, snapshot = false)
        .evalTap { t =>
          val line = summarize(t)
          log(s"  $line") *> seen.update(_ :+ line)
        }
        .interruptAfter(20.seconds)
        .compile
        .drain
        .attempt
      _ <- result match
        case Left(e: MarketData.RequestFailed) =>
          log(s"SUMMARY $mode RequestFailed code=${e.errorCode} msg=${e.errorMsg}")
        case Left(e) =>
          log(s"SUMMARY $mode stream error: ${e.getClass.getSimpleName}: ${e.getMessage}")
        case Right(_) => IO.unit
      lines <- seen.get
      prices = lines.count(_.startsWith("TickPrice"))
      sizes = lines.count(_.startsWith("TickSize"))
      metas = lines.filter(l =>
        l.startsWith("TickReqParams") || l.startsWith("MarketDataType") || l.startsWith("TickString") ||
          l.startsWith("TickGeneric") || l.startsWith("ErrorDetail")
      )
      _ <- log(s"SUMMARY $mode: messages=${lines.size} TickPrice=$prices TickSize=$sizes other=${metas.size}")
      _ <- metas.take(30).traverse_(e => log(s"SUMMARY $mode detail: $e"))
      _ <- IO.whenA(prices == 0 && sizes == 0)(log(s"SUMMARY $mode: NO price/size ticks"))
    yield ()
  private def probeHistorical(ib: IBClient[IO]): IO[Unit] =
    val reqId = 8801
    for
      _ <- log(s"==== HISTORICAL AAPL 1 day of 1-min TRADES reqId=$reqId ====")
      barsRef <- Ref.of[IO, Option[HistoricalData]](None)
      errorsRef <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- Stream
        .resource(ib.subscribe)
        .flatMap { msgs =>
          Stream.exec(
            ib.requestOnly(
              ReqHistoricalData(
                tickerId = reqId,
                contract = aapl,
                endDateTime = "",
                durationStr = "1 D",
                barSizeSetting = "1 min",
                whatToShow = WhatToShow.TRADES,
                useRTH = 1,
                formatDate = 1,
                keepUpToDate = false,
                chartOptions = Nil
              )
            )
          ) ++ msgs.evalTap { m =>
            m match
              case h: HistoricalData =>
                log(s"  ${summarize(h)}") *> barsRef.set(Some(h))
              case h: HistoricalDataUpdate if h.reqId == reqId =>
                log(s"  ${summarize(h)}")
              case ErrorDetail(id, code, text, _) if id == reqId || id == -1 || code == 320 || code == 162 =>
                val line = s"ErrorDetail id=$id code=$code msg=$text"
                log(s"  $line") *> errorsRef.update(_ :+ line)
              case _ => IO.unit
          }
        }
        .interruptAfter(30.seconds)
        .onFinalize(ib.requestOnly(CancelHistoricalData(tickerId = reqId)).attempt.void)
        .compile
        .drain
      bars <- barsRef.get
      errs <- errorsRef.get
      _ <- bars match
        case Some(h) =>
          log(s"SUMMARY historical: GOT ${h.histories.size} bars start=${h.startDate} end=${h.endDate}")
        case None =>
          log(s"SUMMARY historical: NO bars; errors=${errs.mkString(" | ")}")
    yield ()

  def run: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 78))
      .use { ib =>
        for
          acctStr <- ib.asInstanceOf[IBSocketClientCats[IO]].managedAccounts.timeout(20.seconds)
          _ <- log(s"managedAccounts=$acctStr")
          _ <- probeMode(ib, MktDataType.Realtime, 7801)
          _ <- probeMode(ib, MktDataType.Delayed, 7802)
          _ <- probeMode(ib, MktDataType.DelayedFrozen, 7803)
          _ <- probeHistorical(ib)
          _ <- log("AAPL market-data check done")
        yield ()
      }
      .handleErrorWith { e =>
        log(s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}") *> IO.raiseError(e)
      }