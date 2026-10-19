package org.quasigroup.ibclient.request.writers

import org.quasigroup.ibclient.encoder.Encoder.{EncoderState, write, given}
import org.quasigroup.ibclient.request.RequestMsg.ReqExecutions
import org.quasigroup.ibclient.types.TypesCodec.given

/** reqExecutions: msg id, version 3, request id, then the filter (client id, account, time, symbol, security type,
  * exchange, side). Written field by field so the security type is sent by name, not by enum position.
  */
object ReqExecutionsWriter:
  def apply(a: ReqExecutions): EncoderState =
    for
      _ <- write(a.msgId)
      _ <- write(a.version)
      _ <- write(a.reqId)
      _ <- write(a.filter.clientId)
      _ <- write(a.filter.acctCode)
      _ <- write(a.filter.time)
      _ <- write(a.filter.symbol)
      _ <- write(a.filter.secType)
      _ <- write(a.filter.exchange)
      _ <- write(a.filter.side)
    yield ()
