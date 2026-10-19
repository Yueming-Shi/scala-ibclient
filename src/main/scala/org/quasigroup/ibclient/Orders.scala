package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

object Orders:

  private val OpenOrderMsgId = "5" // incoming TWS message id of openOrder

  final case class UnreadableOrder(fields: List[String], reason: String)
      extends RuntimeException(s"could not read an order message from TWS ($reason): ${fields.mkString("[", ",", "]")}")

  /** Keeps the open orders until TWS says the list is complete (openOrderEnd). Fails if an open order message could
    * not be read, so a list is never silently incomplete.
    */
  def openOrders[F[_]: RaiseThrowable]: Pipe[F, ResponseMsg, OpenOrder] =
    _.takeWhile(_ != OpenOrderEnd).flatMap {
      case order: OpenOrder                                                    => Stream.emit(order)
      case Unreadable(fields, reason) if fields.headOption.contains(OpenOrderMsgId) =>
        Stream.raiseError[F](UnreadableOrder(fields, reason))
      case _ => Stream.empty
    }
