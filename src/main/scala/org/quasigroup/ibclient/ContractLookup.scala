package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.types.ContractDetails

import fs2.{Pipe, RaiseThrowable, Stream}

/** Resolve contract details from TWS (reqContractDetails until contractDetailsEnd). */
object ContractLookup:

  private val ContractDataMsgId = "10"
  private val BondContractDataMsgId = "18"

  final case class RequestFailed(reqId: Int, errorCode: Int, errorMsg: String)
      extends RuntimeException(s"contract details request $reqId failed: [$errorCode] $errorMsg")

  final case class Unreadable(reqId: Int, fields: List[String], reason: String)
      extends RuntimeException(
        s"contract details request $reqId: unreadable message ($reason): ${fields.mkString("[", ",", "]")}"
      )

  final case class ConnectionLost(reqId: Int, reason: String)
      extends RuntimeException(s"contract details request $reqId stopped: $reason")

  /** Emits each ContractDetails for `reqId` and completes at ContractDetailsEnd for that id.
    * Fails on a TWS error for the request, an unreadable contract-data message for this request, or connection loss.
    */
  def details[F[_]: RaiseThrowable](reqId: Int): Pipe[F, ResponseMsg, ContractDetails] =
    _.flatMap {
      case ContractDetailsMsg(`reqId`, cd)  => Stream.emit(Some(cd))
      case BondContractDetails(`reqId`, cd) => Stream.emit(Some(cd))
      case ContractDetailsEnd(`reqId`)      => Stream.emit(None)
      case ErrorDetail(`reqId`, code, text, _) if !MarketData.isWarning(code) =>
        Stream.raiseError[F](RequestFailed(reqId, code, text))
      case ResponseMsg.Unreadable(fields, reason, _)
          if fields.headOption.exists(id => id == ContractDataMsgId || id == BondContractDataMsgId) &&
            fields.lift(1).contains(reqId.toString) =>
        Stream.raiseError[F](Unreadable(reqId, fields, reason))
      case ErrorDetail(-1, code, text, _) if MarketData.isConnectionLoss(code) =>
        Stream.raiseError[F](ConnectionLost(reqId, s"[$code] $text"))
      case ConnectionClosed => Stream.raiseError[F](ConnectionLost(reqId, "TWS closed the connection"))
      case _                => Stream.empty
    }.unNoneTerminate