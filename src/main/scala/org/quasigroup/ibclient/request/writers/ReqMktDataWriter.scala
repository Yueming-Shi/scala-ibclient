package org.quasigroup.ibclient.request.writers

import org.quasigroup.ibclient.IBClient

import org.quasigroup.ibclient.encoder.Encoder.{EncoderState, write, writeNothing, given}
import org.quasigroup.ibclient.request.RequestMsg.ReqMktData
import org.quasigroup.ibclient.types.{ComboLeg, SecType}
import org.quasigroup.ibclient.types.TypesCodec.given

import cats.syntax.all.*

import scala.collection.mutable

object ReqMktDataWriter {
  def apply(a: ReqMktData)(using serverVersion: IBClient.ServerVersion): EncoderState =
    for {
      _ <- write(a.msgId)
      _ <- write(a.version)
      _ <- write(a.tickerId)
      _ <- ContractWriter(a.contract)
      _ <- if a.contract.secType == SecType.BAG then comboLegs(a.contract.comboLegs) else writeNothing
      _ <- a.contract.deltaNeutralContract match
        case Some(deltaNeutralContract) =>
          write(true).flatMap(_ => write(deltaNeutralContract))
        case None =>
          write(false)
      _ <- write(a.genericTickList)
      _ <- write(a.snapshot)
      _ <- write(a.regulatorySnapshot)
      _ <- write(a.mktDataOptions)
    } yield ()

  // combo legs of a BAG contract: count, then conId, ratio, action, exchange per leg
  private def comboLegs(legs: List[ComboLeg]): EncoderState =
    write(legs.size) *> legs.traverse_(leg =>
      write(leg.conId) *> write(leg.ratio) *> write(leg.action) *> write(leg.exchange)
    )
}
