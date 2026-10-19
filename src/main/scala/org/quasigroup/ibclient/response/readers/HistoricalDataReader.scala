package org.quasigroup.ibclient.response.readers

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.IBClient.MIN_SERVER_VER_SYNT_REALTIME_BARS
import org.quasigroup.ibclient.decoder.Decoder.{DecoderState, read, readNothing}
import org.quasigroup.ibclient.response.ResponseMsg.{HistoricalData, HistoricalDataUpdate}
import org.quasigroup.ibclient.types.{Bar, Decimal}

/** Decodes HISTORICAL_DATA (msg 17). At server >= 124 the message version is omitted and
  * start/end dates are timezone-aware strings such as "20261018 12:13:08 US/Eastern".
  * Bar times are kept as those strings (formatDate=1); they are not parsed as Int.
  */
object HistoricalDataReader:
  def create(using serverVersion: IBClient.ServerVersion): DecoderState[HistoricalData] =
    if serverVersion >= MIN_SERVER_VER_SYNT_REALTIME_BARS then createModern else createLegacy

  /** Server 124+: no message version; no hasGaps field between WAP and barCount. */
  private def createModern(using serverVersion: IBClient.ServerVersion): DecoderState[HistoricalData] =
    for
      reqId <- read[Int]
      startDate <- read[String]
      endDate <- read[String]
      itemCount <- read[Int]
      histories <- readBars(reqId, itemCount, includeBarCount = true, readHasGaps = false)
    yield HistoricalData(histories, startDate, endDate)

  /** Pre-124: leading message version; hasGaps string present; barCount when version >= 3. */
  private def createLegacy(using serverVersion: IBClient.ServerVersion): DecoderState[HistoricalData] =
    for
      version <- read[Int]
      reqId <- read[Int]
      startDate <- if version >= 2 then read[String] else readNothing("")
      endDate <- if version >= 2 then read[String] else readNothing("")
      itemCount <- read[Int]
      histories <- readBars(reqId, itemCount, includeBarCount = version >= 3, readHasGaps = true)
    yield HistoricalData(histories, startDate, endDate)

  private def readBars(
      reqId: Int,
      itemCount: Int,
      includeBarCount: Boolean,
      readHasGaps: Boolean
  ): DecoderState[List[HistoricalDataUpdate]] =
    (0 until itemCount)
      .foldLeft(readNothing(List.empty[HistoricalDataUpdate])) { (state, _) =>
        for
          list <- state
          date <- read[String]
          open <- read[Double]
          high <- read[Double]
          low <- read[Double]
          close <- read[Double]
          volume <- read[Decimal]
          wap <- read[Decimal]
          _ <- if readHasGaps then read[String].map(_ => ()) else readNothing(())
          barCount <- if includeBarCount then read[Int] else readNothing(-1)
        yield HistoricalDataUpdate(
          reqId,
          Bar(
            time = date,
            open = open,
            high = high,
            low = low,
            close = close,
            volume = volume,
            count = barCount,
            wap = wap
          )
        ) :: list
      }
      .map(_.reverse)
