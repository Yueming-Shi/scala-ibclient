package org.quasigroup.ibclient.request.writers

import org.quasigroup.ibclient.IBClient

import org.quasigroup.ibclient.encoder.Encoder.{EncoderState, write}
import org.quasigroup.ibclient.types.TypesCodec.given
import org.quasigroup.ibclient.request.RequestMsg.ReqContractDetails

object ReqContractDetailsWriter {
  def apply(a: ReqContractDetails)(using serverVersion: IBClient.ServerVersion): EncoderState =
    for {
      _ <- write(a.msgId)
      _ <- write(a.version)
      _ <- write(a.reqId)
      _ <- ContractWriter(a.contract)
      _ <- write(a.contract.includeExpired)
      _ <- write(a.contract.secIdType)
      _ <- write(a.contract.secId)
      _ <- write(a.contract.issuerId)
    } yield ()
}