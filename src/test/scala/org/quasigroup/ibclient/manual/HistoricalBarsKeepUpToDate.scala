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
import scala.concurrent.duration.*

object HistoricalBarsKeepUpToDate extends IOApp.Simple:
  private def log(msg: String): IO[Unit] =
    IO.realTimeInstant.flatMap(t => Console[IO].println(s"[$t] $msg"))

  private val esz6 = Contract(
    conId = 515416632,
    symbol = "ES",
    secType = SecType.FUT,
    currency = "USD",
    exchange = "CME",
    lastTradeDateOrContractMonth = "20261218",
    localSymbol = "ESZ6",
    tradingClass = "ES",
    multiplier = "50"
  )

  private def tag(m: ResponseMsg): String = m match
    case _: HistoricalData => "HistoricalData"
    case h: HistoricalDataUpdate => s"HistoricalDataUpdate(reqId=${h.reqId})"
    case e: ErrorDetail => s"ErrorDetail(${e.id},${e.errorCode})"
    case ConnectionClosed => "ConnectionClosed"
    case u: Unreadable => s"Unreadable(head=${u.fields.headOption},reason=${u.reason.take(80)})"
    case other => other.getClass.getSimpleName

  def run: IO[Unit] =
    IBSocketClientCats
      .make[IO](IBSocketClientCats.Settings(clientId = 86))
      .use { ib =>
        for
          _ <- log("==== ESZ6 keepUpToDate=true ~90s verbose ====")
          counts <- Ref.of[IO, Map[String, Int]](Map.empty)
          updates <- Ref.of[IO, Int](0)
          _ <- Stream
            .resource(ib.subscribe)
            .flatMap { msgs =>
              Stream.exec(
                ib.requestOnly(
                  ReqHistoricalData(
                    tickerId = 8904,
                    contract = esz6,
                    endDateTime = "",
                    durationStr = "900 S",
                    barSizeSetting = "1 min",
                    whatToShow = WhatToShow.TRADES,
                    useRTH = 0,
                    formatDate = 1,
                    keepUpToDate = true,
                    chartOptions = Nil
                  )
                )
              ) ++ msgs.evalTap { m =>
                val t = tag(m)
                counts.update(c => c.updated(t, c.getOrElse(t, 0) + 1)) *>
                  (m match
                    case h: HistoricalData =>
                      log(s"snapshot bars=${h.histories.size}")
                    case h: HistoricalDataUpdate =>
                      updates.update(_ + 1) *>
                        log(s"UPDATE ${h.bar.time} c=${h.bar.close} v=${h.bar.volume} n=${h.bar.count}")
                    case e: ErrorDetail if e.id == 8904 || e.errorCode != 2104 && e.errorCode != 2106 && e.errorCode != 2158 =>
                      log(s"Error id=${e.id} code=${e.errorCode} msg=${e.errorMsg}")
                    case u: Unreadable =>
                      log(s"Unreadable head=${u.fields.take(8)} reason=${u.reason}")
                    case ConnectionClosed => log("ConnectionClosed")
                    case _ => IO.unit)
              }
            }
            .interruptAfter(90.seconds)
            .onFinalize(ib.requestOnly(CancelHistoricalData(tickerId = 8904)).attempt.void)
            .compile
            .drain
          c <- counts.get
          u <- updates.get
          _ <- log(s"SUMMARY counts=$c liveUpdates=$u")
        yield ()
      }
