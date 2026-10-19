package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import cats.effect.{Deferred, Temporal}
import cats.syntax.all.*
import fs2.{Pipe, Stream}

import scala.concurrent.duration.*

object Executions:

  /** One item of an executions reply: a fill (execution details) or the commission charged for one of its fills. */
  type Report = ExecDetails | CommissionReportMsg

  /** How long to wait after the end-of-executions marker for commission reports that have not arrived yet. */
  val DefaultCommissionWait: FiniteDuration = 2.seconds

  private val ExecDetailsMsgId = "11" // incoming TWS message id of execDetails
  private val CommissionReportMsgId = "59" // incoming TWS message id of commissionReport

  final case class RequestFailed(reqId: Int, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"executions request $reqId failed: [$errorCode] $errorMsg")

  final case class Unreadable(reqId: Int, fields: List[String], reason: String)
      extends RuntimeException(s"executions request $reqId: unreadable message ($reason): ${fields.mkString("[", ",", "]")}")

  final case class ConnectionLost(reqId: Int, reason: String)
      extends RuntimeException(s"executions request $reqId stopped: $reason")

  /** What a request has seen so far: the execution ids it returned, those with a commission report, the end marker. */
  private final case class Seen(executions: Set[String], charged: Set[String], ended: Boolean):
    def complete: Boolean = ended && executions.subsetOf(charged)

  /** The execution ids and request id fields of an unreadable message, by server version (no version field at 136+). */
  private def unreadableReqId(u: ResponseMsg.Unreadable): Option[String] =
    u.fields.lift(if u.serverVersion >= IBClient.MIN_SERVER_VER_LAST_LIQUIDITY.toInt then 1 else 2)

  /** Keeps the fills of request `reqId` and the commission reports of those fills (matched by execution id, as
    * commission reports carry no request id). Completes once TWS has signalled the end of the fills and every fill
    * has its commission report, or `commissionWait` after the end signal. Fails on a TWS error for the request, on an
    * unreadable fill of this request or commission report of one of its fills, and when the connection is lost.
    */
  def reports[F[_]: Temporal](reqId: Int, commissionWait: FiniteDuration = DefaultCommissionWait): Pipe[F, ResponseMsg, Report] =
    msgs =>
      Stream.eval(Deferred[F, Unit]).flatMap { ended =>
        msgs
          .evalMapAccumulate(Seen(Set.empty, Set.empty, ended = false)) { (seen, msg) =>
            msg match {
              case e: ExecDetails if e.reqId == reqId && !seen.ended =>
                (seen.copy(executions = seen.executions + e.execution.execId), Some(e)).pure[F]
              case c: CommissionReportMsg if seen.executions(c.commissionReport.execId) =>
                (seen.copy(charged = seen.charged + c.commissionReport.execId), Some(c)).pure[F]
              case ExecDetailsEnd(`reqId`) => ended.complete(()).as((seen.copy(ended = true), None))
              case ErrorDetail(`reqId`, code, text, _) if !MarketData.isWarning(code) =>
                RequestFailed(reqId, code, text).raiseError
              case u @ ResponseMsg.Unreadable(fields, reason, _)
                  if (fields.headOption.contains(ExecDetailsMsgId) && unreadableReqId(u).contains(reqId.toString)) ||
                    (fields.headOption.contains(CommissionReportMsgId) && fields.lift(2).exists(seen.executions)) =>
                Unreadable(reqId, fields, reason).raiseError
              case ErrorDetail(-1, code, text, _) if MarketData.isConnectionLoss(code) =>
                ConnectionLost(reqId, s"[$code] $text").raiseError
              case ConnectionClosed => ConnectionLost(reqId, "TWS closed the connection").raiseError
              case _                => (seen, None).pure[F]
            }: F[(Seen, Option[Report])]
          }
          .takeThrough { case (seen, _) => !seen.complete }
          .collect { case (_, Some(report)) => report }
          .interruptWhen(ended.get *> Temporal[F].sleep(commissionWait).as(().asRight[Throwable]))
      }
