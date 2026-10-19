package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import cats.Applicative
import cats.syntax.all.*
import fs2.{Pipe, RaiseThrowable, Stream}

import scala.concurrent.duration.*

/** Read-only views of the account: positions, portfolio values and account summary. */
object Portfolio:

  private val PositionMsgId = "61" // incoming TWS message id of position

  final case class Unreadable(what: String, fields: List[String], reason: String)
      extends RuntimeException(s"$what: unreadable message from TWS ($reason): ${fields.mkString("[", ",", "]")}")

  final case class ConnectionLost(what: String, reason: String) extends RuntimeException(s"$what stopped: $reason")

  final case class RequestFailed(what: String, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"$what failed: [$errorCode] $errorMsg")

  /** How long a positions request waits for the positionEnd of an earlier request that was stopped before it. */
  val DefaultStaleEndWait: FiniteDuration = 2.seconds

  /** TWS messages without a request id (id -1) that are not errors: warnings / farm status and "connectivity
    * restored, data maintained" (1102).
    */
  private def isNotice(code: Int): Boolean = MarketData.isWarning(code) || code == 1102

  /** For the stream `what`: fails when the connection to TWS or to IB is lost, so it never waits forever, and
    * ignores any other message.
    */
  private[ibclient] def failOnConnectionLoss[F[_]: RaiseThrowable](what: String)(msg: ResponseMsg): Stream[F, Nothing] =
    msg match
      case ErrorDetail(-1, code, text, _) if MarketData.isConnectionLoss(code) =>
        Stream.raiseError[F](ConnectionLost(what, s"[$code] $text"))
      case ConnectionClosed => Stream.raiseError[F](ConnectionLost(what, "TWS closed the connection"))
      case _                => Stream.empty

  /** Collects the positions until TWS says the list is complete (positionEnd), then emits them, one per account and
    * instrument (the latest, in the order first seen), and runs `onEnd`. Fails if a position could not be read, on a
    * TWS error without request id (positions requests have none) and when the connection is lost, so the list is
    * never silently incomplete.
    */
  def positions[F[_]: RaiseThrowable: Applicative](onEnd: F[Unit]): Pipe[F, ResponseMsg, PositionMsg] =
    _.flatMap {
      case p: PositionMsg => Stream.emit(Some(p))
      case PositionEnd    => Stream.emit(None)
      case ResponseMsg.Unreadable(fields, reason, _) if fields.headOption.contains(PositionMsgId) =>
        Stream.raiseError[F](Unreadable("positions", fields, reason))
      case ErrorDetail(-1, code, text, _) if !isNotice(code) && !MarketData.isConnectionLoss(code) =>
        Stream.raiseError[F](RequestFailed("positions", code, text))
      case msg => failOnConnectionLoss[F]("positions")(msg)
    }.evalTap(p => if p.isEmpty then onEnd else Applicative[F].unit)
      .unNoneTerminate
      .fold(Vector.empty[PositionMsg]) { (list, p) =>
        list.indexWhere(q => q.account == p.account && q.contract.conId == p.contract.conId) match
          case -1 => list :+ p
          case i  => list.updated(i, p)
      }
      .flatMap(Stream.emits)
