package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

/** Live daily profit and loss (PnL) of an account, or of one position in it. Read only. */
object ProfitAndLoss:

  private val PnLMsgId = "94" // incoming TWS message id of pnl
  private val PnLSingleMsgId = "95" // incoming TWS message id of pnlSingle

  /** Keeps the updates of request `reqId` selected by `pick`. Never ends by itself; fails on a TWS error for the
    * request, on an unreadable update of the request (message `msgId`) and when the connection is lost.
    */
  private def updates[F[_]: RaiseThrowable, A](reqId: Int, what: String, msgId: String)(
      pick: PartialFunction[ResponseMsg, A]
  ): Pipe[F, ResponseMsg, A] =
    _.flatMap {
      case ErrorDetail(`reqId`, code, text, _) if !MarketData.isWarning(code) =>
        Stream.raiseError[F](Portfolio.RequestFailed(s"$what $reqId", code, text))
      case ResponseMsg.Unreadable(fields, reason, _)
          if fields.headOption.contains(msgId) && fields.lift(1).contains(reqId.toString) =>
        Stream.raiseError[F](Portfolio.Unreadable(s"$what $reqId", fields, reason))
      case msg if pick.isDefinedAt(msg) => Stream.emit(pick(msg))
      case msg                          => Portfolio.failOnConnectionLoss[F](s"$what $reqId")(msg)
    }

  /** The account PnL updates of request `reqId`. */
  def pnl[F[_]: RaiseThrowable](reqId: Int): Pipe[F, ResponseMsg, PnL] =
    updates(reqId, "PnL", PnLMsgId) { case p: PnL if p.reqId == reqId => p }

  /** The single-position PnL updates of request `reqId`. */
  def pnlSingle[F[_]: RaiseThrowable](reqId: Int): Pipe[F, ResponseMsg, PnLSingle] =
    updates(reqId, "PnL single", PnLSingleMsgId) { case p: PnLSingle if p.reqId == reqId => p }

  /** TWS sends Double.MaxValue for a value it does not have (e.g. realized PnL before any trade). */
  def known(value: Double): Option[Double] = Option.when(value != Double.MaxValue)(value)
