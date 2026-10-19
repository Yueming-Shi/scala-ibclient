package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

object Executions:

  /** One item of an executions reply: a fill (execution details) or the commission charged for one of its fills. */
  type Report = ExecDetails | CommissionReportMsg

  private val ExecDetailsMsgId = "11" // incoming TWS message id of execDetails
  private val CommissionReportMsgId = "59" // incoming TWS message id of commissionReport

  final case class RequestFailed(reqId: Int, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"executions request $reqId failed: [$errorCode] $errorMsg")

  final case class Unreadable(reqId: Int, fields: List[String], reason: String)
      extends RuntimeException(s"executions request $reqId: unreadable message ($reason): ${fields.mkString("[", ",", "]")}")

  final case class ConnectionLost(reqId: Int, reason: String)
      extends RuntimeException(s"executions request $reqId stopped: $reason")

  /** Keeps the fills of request `reqId` and the commission reports of those fills (matched by execution id, as
    * commission reports carry no request id), until TWS signals the end of the reply. Fails on a TWS error for the
    * request, on an unreadable fill or commission report and when the connection is lost.
    */
  def reports[F[_]: RaiseThrowable](reqId: Int): Pipe[F, ResponseMsg, Report] =
    _.takeWhile(_ != ExecDetailsEnd(reqId))
      .flatMap {
        case e: ExecDetails if e.reqId == reqId => Stream.emit(e)
        case c: CommissionReportMsg             => Stream.emit(c)
        case ErrorDetail(`reqId`, code, msg, _) if !MarketData.isWarning(code) =>
          Stream.raiseError[F](RequestFailed(reqId, code, msg))
        case ResponseMsg.Unreadable(fields, reason)
            if fields.headOption.exists(id => id == ExecDetailsMsgId || id == CommissionReportMsgId) =>
          Stream.raiseError[F](Unreadable(reqId, fields, reason))
        case ErrorDetail(-1, code, msg, _) if MarketData.isConnectionLoss(code) =>
          Stream.raiseError[F](ConnectionLost(reqId, s"[$code] $msg"))
        case ConnectionClosed => Stream.raiseError[F](ConnectionLost(reqId, "TWS closed the connection"))
        case _                => Stream.empty
      }
      .mapAccumulate(Set.empty[String]) {
        case (seen, e: ExecDetails)          => (seen + e.execution.execId, Some(e))
        case (seen, c: CommissionReportMsg)  => (seen, Option.when(seen(c.commissionReport.execId))(c))
      }
      .collect { case (_, Some(report)) => report }
