package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

object MarketData:

  /** One market data update for a request: a price, a size, a text value, a number, or the kind of data
    * (real-time / frozen / delayed) TWS is sending.
    */
  type Tick = TickPrice | TickSize | TickString | TickGeneric | MarketDataType

  final case class RequestFailed(tickerId: Int, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"market data request $tickerId failed: [$errorCode] $errorMsg")

  /** TWS error codes that are only warnings for a market data request (e.g. 10167: "displaying delayed data"). */
  def isWarning(errorCode: Int): Boolean =
    errorCode == 10167 || errorCode == 10090 || (errorCode >= 2100 && errorCode < 2200)

  private def tickFor(tickerId: Int): PartialFunction[ResponseMsg, Tick] =
    case t: TickPrice if t.tickerId == tickerId      => t
    case t: TickSize if t.tickerId == tickerId       => t
    case t: TickString if t.tickerId == tickerId     => t
    case t: TickGeneric if t.tickerId == tickerId    => t
    case t: MarketDataType if t.reqId == tickerId    => t

  /** Keeps the ticks of `tickerId`, ends at its snapshot end, fails on a (non-warning) TWS error for it. */
  def ticks[F[_]: RaiseThrowable](tickerId: Int): Pipe[F, ResponseMsg, Tick] =
    _.takeWhile {
      case TickSnapshotEnd(`tickerId`) => false
      case _                           => true
    }.flatMap {
      case ErrorDetail(`tickerId`, code, msg, _) if !isWarning(code) =>
        Stream.raiseError[F](RequestFailed(tickerId, code, msg))
      case msg => Stream.emits(tickFor(tickerId).lift(msg).toList)
    }
