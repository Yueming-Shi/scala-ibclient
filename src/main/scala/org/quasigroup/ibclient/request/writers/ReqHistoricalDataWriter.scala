package org.quasigroup.ibclient.request.writers

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.IBClient.MIN_SERVER_VER_SYNT_REALTIME_BARS

import org.quasigroup.ibclient.types.*
import org.quasigroup.ibclient.types.TypesCodec.given
import org.quasigroup.ibclient.encoder.Encoder.{*, given}
import org.quasigroup.ibclient.request.RequestMsg.ReqHistoricalData

object ReqHistoricalDataWriter {
  def apply(a: ReqHistoricalData)(using serverVersion: IBClient.ServerVersion): EncoderState =
    for
      _ <- write(a.msgId)
      _ <-
        if serverVersion >= MIN_SERVER_VER_SYNT_REALTIME_BARS then writeNothing
        else write(a.version)
      _ <- write(a.tickerId)
      _ <- ContractWriter(a.contract)
      _ <- write(a.contract.includeExpired)
      _ <- write(a.endDateTime)
      _ <- write(a.barSizeSetting)
      _ <- write(a.durationStr)
      _ <- write(a.useRTH)
      _ <- write(a.whatToShow)
      _ <- write(a.formatDate)
      _ <-
        if a.contract.secType == SecType.BAG then
          for
            _ <- write(a.contract.comboLegs.size)
            _ <- a.contract.comboLegs.foldLeft(writeNothing) { (lastWritten, comboLeg) =>
              for
                _ <- lastWritten
                _ <- write(comboLeg.conId)
                _ <- write(comboLeg.ratio)
                _ <- write(comboLeg.action)
                _ <- write(comboLeg.exchange)
              yield ()
            }
          yield ()
        else writeNothing
      _ <-
        if serverVersion >= MIN_SERVER_VER_SYNT_REALTIME_BARS then write(a.keepUpToDate)
        else writeNothing
      _ <- write(a.chartOptions)
    yield ()
}