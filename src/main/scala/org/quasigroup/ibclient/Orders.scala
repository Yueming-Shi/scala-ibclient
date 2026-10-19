package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.*

import fs2.{Pipe, RaiseThrowable, Stream}

object Orders:

  private val OpenOrderMsgId = "5" // incoming TWS message id of openOrder
  private val OrderStatusMsgId = "3" // incoming TWS message id of orderStatus

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
      case ConnectionClosed => Stream.raiseError[F](StatusConnectionLost("TWS closed the connection"))
      case _                => Stream.empty
    }

  final case class StatusConnectionLost(reason: String) extends RuntimeException(s"order updates stopped: $reason")

  /** An order is done when it is cancelled, completely filled or inactive (rejected or cancelled by the system);
    * TWS sends no further status for it. An order held inactive while IB locates shares to short is not done.
    */
  def isDone(status: OrderStatusMsg): Boolean =
    status.status match
      case Order.Status.Cancelled | Order.Status.ApiCancelled => true
      case Order.Status.Filled                                => status.remaining.isZero
      case Order.Status.Inactive                              => !status.whyHeld.toLowerCase.contains("locate")
      case _                                                  => false

  /** Keeps the order status updates. Fails if one could not be read and when the connection to TWS or IB is lost, so
    * an order's progress is never silently missed.
    */
  def statuses[F[_]: RaiseThrowable]: Pipe[F, ResponseMsg, OrderStatusMsg] =
    _.flatMap {
      case status: OrderStatusMsg => Stream.emit(status)
      case Unreadable(fields, reason) if fields.headOption.contains(OrderStatusMsgId) =>
        Stream.raiseError[F](UnreadableOrder(fields, reason))
      case ErrorDetail(-1, code, msg, _) if MarketData.isConnectionLoss(code) =>
        Stream.raiseError[F](StatusConnectionLost(s"[$code] $msg"))
      case ConnectionClosed => Stream.raiseError[F](StatusConnectionLost("TWS closed the connection"))
      case _                => Stream.empty
    }

  /** The status updates of one order, completing with its final one (see [[isDone]]). */
  def statusesOf[F[_]: RaiseThrowable](orderId: Int): Pipe[F, ResponseMsg, OrderStatusMsg] =
    _.through(statuses).filter(_.orderId == orderId).takeThrough(!isDone(_))
