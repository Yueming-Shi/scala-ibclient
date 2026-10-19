package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

object MarketData:

  /** One market data update for a request: a price, a size, a text value, a number, the kind of data
    * (real-time / frozen / delayed) TWS is sending, option greeks, EFP data, request parameters or a news headline.
    */
  type Tick = TickPrice | TickSize | TickString | TickGeneric | MarketDataType | TickOptionComputation | TickEFP |
    TickReqParams | TickNews

  final case class RequestFailed(tickerId: Int, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"market data request $tickerId failed: [$errorCode] $errorMsg")

  final case class ConnectionLost(tickerId: Int, reason: String)
      extends RuntimeException(s"market data request $tickerId stopped: $reason")

  /** TWS notices (id -1) meaning the connection to IB was lost: 1100 lost, 1101 restored but data lost, 1300 socket
    * port reset. Subscriptions do not survive them.
    */
  def isConnectionLoss(errorCode: Int): Boolean = errorCode == 1100 || errorCode == 1101 || errorCode == 1300

  /** TWS error codes that are only warnings for a market data request (e.g. 10167: "displaying delayed data"). */
  def isWarning(errorCode: Int): Boolean =
    errorCode == 10167 || errorCode == 10090 || (errorCode >= 2100 && errorCode < 2200)

  private def tickFor(tickerId: Int): PartialFunction[ResponseMsg, Tick] =
    case t: TickPrice if t.tickerId == tickerId      => t
    case t: TickSize if t.tickerId == tickerId       => t
    case t: TickString if t.tickerId == tickerId     => t
    case t: TickGeneric if t.tickerId == tickerId    => t
    case t: MarketDataType if t.reqId == tickerId    => t
    case t: TickOptionComputation if t.tickerId == tickerId => t
    case t: TickEFP if t.tickerId == tickerId        => t
    case t: TickReqParams if t.tickerId == tickerId  => t
    case t: TickNews if t.tickerId == tickerId       => t

  /** Keeps the ticks of `tickerId`, ends at its snapshot end, fails on a (non-warning) TWS error for it and when the
    * connection to TWS or IB is lost (so it never silently stalls).
    */
  def ticks[F[_]: RaiseThrowable](tickerId: Int): Pipe[F, ResponseMsg, Tick] =
    _.takeWhile {
      case TickSnapshotEnd(`tickerId`) => false
      case _                           => true
    }.flatMap {
      case ErrorDetail(`tickerId`, code, msg, _) if !isWarning(code) =>
        Stream.raiseError[F](RequestFailed(tickerId, code, msg))
      case ErrorDetail(-1, code, msg, _) if isConnectionLoss(code) =>
        Stream.raiseError[F](ConnectionLost(tickerId, s"[$code] $msg"))
      case ConnectionClosed =>
        Stream.raiseError[F](ConnectionLost(tickerId, "TWS closed the connection"))
      case msg => Stream.emits(tickFor(tickerId).lift(msg).toList)
    }
