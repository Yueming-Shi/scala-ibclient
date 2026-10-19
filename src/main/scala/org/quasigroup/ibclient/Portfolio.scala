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
  /** Incoming TWS message ids of account updates: account value, portfolio value, update time, download end. */
  private val AccountUpdateMsgIds = Set("6", "7", "8", "54")

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

  /** TWS notices that end an account updates subscription: 2100 (this client was unsubscribed from account data)
    * and 2101 (the subscription was refused because another client is subscribed to a different account).
    */
  def isAccountSubscriptionLost(code: Int): Boolean = code == 2100 || code == 2101

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

  /** One account update: an account value (e.g. NetLiquidation), a portfolio line, the time of the last update, or
    * the marker that TWS has sent the whole account once (accountDownloadEnd).
    */
  type AccountUpdate = UpdateAccountValue | UpdatePortfolio | UpdateAccountTime | AccountDownloadEnd

  /** Keeps the updates of `account` (all accounts when it is empty). Never ends by itself; fails if an account update
    * could not be read, on a TWS error without request id (account updates have none) and when the connection is lost.
    */
  def accountUpdates[F[_]: RaiseThrowable](account: String): Pipe[F, ResponseMsg, AccountUpdate] =
    def mine(name: String) = account.isEmpty || name == account
    _.flatMap {
      case v: UpdateAccountValue if mine(v.accountName) => Stream.emit(v)
      case p: UpdatePortfolio if mine(p.accountName)    => Stream.emit(p)
      case t: UpdateAccountTime                         => Stream.emit(t)
      case e: AccountDownloadEnd if mine(e.accountName) => Stream.emit(e)
      case ResponseMsg.Unreadable(fields, reason, _) if fields.headOption.exists(AccountUpdateMsgIds) =>
        Stream.raiseError[F](Unreadable("account updates", fields, reason))
      case ErrorDetail(-1, code, text, _)
          if isAccountSubscriptionLost(code) || (!isNotice(code) && !MarketData.isConnectionLoss(code)) =>
        Stream.raiseError[F](RequestFailed("account updates", code, text))
      case msg => failOnConnectionLoss[F]("account updates")(msg)
    }

  /** The whole account at one moment: its values by (key, currency), e.g. ("NetLiquidation", "USD"), its portfolio
    * (the latest line per instrument) and the time of TWS's last update.
    */
  final case class AccountSnapshot(
      account: String,
      values: Map[(String, String), String],
      portfolio: Vector[UpdatePortfolio],
      updateTime: String
  ):
    def value(key: String, currency: String): Option[String] = values.get((key, currency))

    def add(update: AccountUpdate): AccountSnapshot =
      update match
        case v: UpdateAccountValue => copy(values = values.updated((v.key, v.currency), v.value))
        case p: UpdatePortfolio =>
          val same = (q: UpdatePortfolio) => q.contract.conId == p.contract.conId && q.accountName == p.accountName
          copy(portfolio = portfolio.indexWhere(same) match
            case -1 => portfolio :+ p
            case i  => portfolio.updated(i, p))
        case t: UpdateAccountTime  => copy(updateTime = t.timeStamp)
        case e: AccountDownloadEnd => copy(account = e.accountName)

  object AccountSnapshot:
    def empty(account: String): AccountSnapshot = AccountSnapshot(account, Map.empty, Vector.empty, "")

  /** Collects account updates into one snapshot, up to and including accountDownloadEnd. */
  def snapshot[F[_]](account: String): Pipe[F, AccountUpdate, AccountSnapshot] =
    _.takeThrough {
      case _: AccountDownloadEnd => false
      case _                     => true
    }.fold(AccountSnapshot.empty(account))(_.add(_))
