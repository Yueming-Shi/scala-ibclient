package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.{Pipe, RaiseThrowable, Stream}

/** Read-only views of the account: positions, portfolio values and account summary. */
object Portfolio:

  private val PositionMsgId = "61" // incoming TWS message id of position

  final case class Unreadable(what: String, fields: List[String], reason: String)
      extends RuntimeException(s"$what: unreadable message from TWS ($reason): ${fields.mkString("[", ",", "]")}")

  final case class ConnectionLost(what: String, reason: String) extends RuntimeException(s"$what stopped: $reason")

  /** For the stream `what`: fails when the connection to TWS or to IB is lost, so it never waits forever, and
    * ignores any other message.
    */
  private[ibclient] def failOnConnectionLoss[F[_]: RaiseThrowable](what: String)(msg: ResponseMsg): Stream[F, Nothing] =
    msg match
      case ErrorDetail(-1, code, text, _) if MarketData.isConnectionLoss(code) =>
        Stream.raiseError[F](ConnectionLost(what, s"[$code] $text"))
      case ConnectionClosed => Stream.raiseError[F](ConnectionLost(what, "TWS closed the connection"))
      case _                => Stream.empty

  /** Keeps the positions until TWS says the list is complete (positionEnd). Fails if a position could not be read or
    * the connection is lost, so the list is never silently incomplete.
    */
  def positions[F[_]: RaiseThrowable]: Pipe[F, ResponseMsg, PositionMsg] =
    _.takeWhile(_ != PositionEnd).flatMap {
      case p: PositionMsg => Stream.emit(p)
      case ResponseMsg.Unreadable(fields, reason, _) if fields.headOption.contains(PositionMsgId) =>
        Stream.raiseError[F](Unreadable("positions", fields, reason))
      case msg => failOnConnectionLoss[F]("positions")(msg)
    }
