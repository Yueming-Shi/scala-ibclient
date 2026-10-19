package org.quasigroup.ibclient

import org.quasigroup.ibclient.types.*
import org.quasigroup.ibclient.types.TypesCodec.given

import org.quasigroup.ibclient.encoder.Encoder
import org.quasigroup.ibclient.encoder.Encoder.{*, given}
import org.quasigroup.ibclient.decoder.Decoder
import org.quasigroup.ibclient.decoder.Decoder.{*, given}
import org.quasigroup.ibclient.request.RequestMsg
import org.quasigroup.ibclient.request.RequestMsg.*
import org.quasigroup.ibclient.request.MsgEncoders.{*, given}
import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*
import org.quasigroup.ibclient.response.MsgDecoders.{*, given}

import cats.effect.{Async, Resource}
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Pipe, Stream}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

trait IBClient[F[_]: Async]:

  def requestOnly[Req <: RequestMsg: MsgEncoder](request: Req): F[Unit]

  def fetchSingleResponse[Resp <: ResponseMsg: ClassTag]: F[Resp]

  def fetchResponseStream[Resp <: ResponseMsg: ClassTag]: Stream[F, Resp]

  /** All incoming messages. The subscription is live once the resource is acquired, so nothing sent after that is
    * missed.
    */
  def subscribe: Resource[F, Stream[F, ResponseMsg]]

  /** Open-order replies carry no request id, so only one open-orders request may run at a time on a connection. */
  def openOrdersLock: Mutex[F]

  /** Position replies carry no request id, so only one positions request may run at a time on a connection. */
  def positionsLock: Mutex[F]

  /** Fails unless this session is paper-only: the paper TWS port 7497, no live-trading opt-in, and every managed
    * account id starts with "D". Order placement and cancellation call this first.
    */
  def requirePaperSession: F[Unit]

  /** TWS keeps one account-updates subscription per connection, so only one runs at a time. */
  def accountUpdatesLock: Mutex[F]

  /** Subscribes, then sends `request`, then streams the incoming messages through `responses`.
    *
    * Always give `Req` explicitly: enum constructors such as `ReqMktData(...)` are typed as `RequestMsg`, which would
    * pick the wrong encoder.
    */
  def requestThrough[Req <: RequestMsg: MsgEncoder, A](request: Req)(responses: Pipe[F, ResponseMsg, A]): Stream[F, A] =
    Stream.resource(subscribe).flatMap(msgs => Stream.exec(requestOnly[Req](request)) ++ msgs.through(responses))

  /** Streams market data ticks for `tickerId` until the stream is stopped (then the subscription is cancelled) or, for
    * snapshots, until TWS signals the end of the snapshot. Fails with [[MarketData.RequestFailed]] on a TWS error for
    * this ticker and with [[MarketData.ConnectionLost]] when the connection is lost.
    *
    * `regulatorySnapshot = true` costs USD 0.01 per request, on paper accounts too.
    *
    * Each stream buffers its ticks until consumed (an unbounded queue, so one slow consumer never blocks the shared
    * connection); consume promptly or `take` / sample what you need.
    */
  def reqMktData(
      tickerId: Int,
      contract: Contract,
      genericTickList: String = "",
      snapshot: Boolean = false,
      regulatorySnapshot: Boolean = false,
      mktDataOptions: List[TagValue] = Nil
  ): Stream[F, MarketData.Tick] =
    val ticks = requestThrough[ReqMktData, MarketData.Tick](
      ReqMktData(
        tickerId = tickerId,
        contract = contract,
        genericTickList = genericTickList,
        snapshot = snapshot,
        regulatorySnapshot = regulatorySnapshot,
        mktDataOptions = mktDataOptions
      )
    )(MarketData.ticks(tickerId))
    if snapshot || regulatorySnapshot then ticks else ticks.onFinalize(cancelMktData(tickerId))

  def cancelMktData(tickerId: Int): F[Unit] =
    requestOnly[CancelMktData](CancelMktData(tickerId = tickerId))

  /** The open orders placed by this client, as a stream that completes when TWS has sent them all. Concurrent calls
    * wait for each other.
    *
    * Note: when the client id is 0, TWS also binds manually entered TWS orders to this client (they become modifiable
    * through the API). The default client id is 10.
    */
  def reqOpenOrders: Stream[F, OpenOrder] =
    Stream.resource(openOrdersLock.lock) >>
      requestThrough[ReqOpenOrders, OpenOrder](ReqOpenOrders())(Orders.openOrders)

  /** The open orders of every API client and of TWS itself, as a stream that completes when TWS has sent them all.
    * Concurrent calls wait for each other.
    */
  def reqAllOpenOrders: Stream[F, OpenOrder] =
    Stream.resource(openOrdersLock.lock) >>
      requestThrough[ReqAllOpenOrders, OpenOrder](ReqAllOpenOrders())(Orders.openOrders)

  /** Chooses real-time, frozen, delayed or delayed-frozen market data for later requests. */
  /** Order status updates (submitted, filled, cancelled, ...) of all orders this client sees. TWS sends them by itself
    * after an order is placed, modified or cancelled and in reply to open-orders requests. Updates are listened for as
    * soon as the resource is open, so open it before placing an order to see all of them. The stream never ends by
    * itself; it fails if an update cannot be read or the connection is lost. TWS may repeat the same status.
    *
    * TWS does not promise an update for every change: an order that fills at once may get no status update at all.
    * To be sure an order filled, also follow its executions.
    */
  def orderStatuses: Resource[F, Stream[F, OrderStatusMsg]] =
    subscribe.map(_.through(Orders.statuses))

  /** The status updates of one order, as a stream that completes with its final one: cancelled, completely filled or
    * inactive (rejected), but not inactive while IB locates shares to short. Like [[orderStatuses]], it listens from
    * the moment the resource is opened. Because TWS may skip updates (see [[orderStatuses]]), put a timeout on it or
    * also follow the order's executions.
    */
  def orderStatus(orderId: Int): Resource[F, Stream[F, OrderStatusMsg]] =
    subscribe.map(_.through(Orders.statusesOf(orderId)))

  /** The executions (fills) of today that match `filter`, each followed by its commission report when TWS has it, as a
    * stream that completes when TWS has sent them all. Fails on a TWS error for `reqId`, on an unreadable fill or
    * commission report and when the connection is lost.
    *
    * Commission reports may arrive after TWS's end-of-executions marker: the stream then waits for the missing ones, at
    * most `commissionWait` after the marker, and completes as soon as every execution has its commission report.
    * An execution id ending in a higher number (e.g. ".02" instead of ".01") corrects an earlier one with the same
    * start.
    */
  def reqExecutions(
      reqId: Int,
      filter: ExecutionFilter = ExecutionFilter(),
      commissionWait: FiniteDuration = Executions.DefaultCommissionWait
  ): Stream[F, Executions.Report] =
    requestThrough[ReqExecutions, Executions.Report](ReqExecutions(reqId = reqId, filter = filter))(
      Executions.reports(reqId, commissionWait)
    )

  def reqMarketDataType(marketDataType: MktDataType): F[Unit] =
    requestOnly[ReqMarketDataType](ReqMarketDataType(marketDataType = marketDataType))

  def reqFamilyCodes: F[FamilyCodes] =
    for {
      _ <- requestOnly[ReqFamilyCodes](ReqFamilyCodes())
      result <- fetchSingleResponse[FamilyCodes]
    } yield result

  def reqCurrentTime: F[CurrentTime] =
    for {
      _ <- requestOnly[ReqCurrentTime](ReqCurrentTime())
      result <- fetchSingleResponse[CurrentTime]
    } yield result

  def reqScannerParameters: F[ScannerParameters] =
    for {
      _ <- requestOnly[ReqScannerParameters](ReqScannerParameters())
      result <- fetchSingleResponse[ScannerParameters]
    } yield result

  def setServerLogLevel(level: Int): F[Unit] =
    requestOnly[SetServerLogLevel](
      SetServerLogLevel(loglevel = level)
    )

  /** The positions of every account this TWS session manages, as a stream that completes when TWS has sent them all
    * (positionEnd). The positions are emitted together once the list is complete, one per account and instrument
    * (the latest, if TWS sent an update while listing). When the stream ends or is stopped, the subscription is
    * cancelled (cancelPositions). Fails if a position cannot be read, on a TWS error and when the connection is lost.
    * Concurrent calls wait for each other; if one is stopped before positionEnd arrived, the next one also waits (at
    * most `staleEndWait`) for that late positionEnd, so it cannot end the next list early.
    */
  def reqPositions: Stream[F, PositionMsg] = reqPositions(Portfolio.DefaultStaleEndWait)

  def reqPositions(staleEndWait: FiniteDuration): Stream[F, PositionMsg] =
    Stream.resource(positionsLock.lock) >> Stream.eval(cats.effect.Ref.of[F, Boolean](false)).flatMap { ended =>
      requestThrough[ReqPositions, PositionMsg](ReqPositions())(Portfolio.positions(ended.set(true)))
        .onFinalize(cancelAwaitingEnd(cancelPositions, ended.get, staleEndWait)(_ == PositionEnd))
    }

  /** Sends `cancel`. If the request had not seen its end marker yet (`ended`), also waits, at most `staleEndWait`, for
    * that late marker, listening from before `cancel` is sent, so that the next request of this kind (which waits for
    * the same lock) cannot take it for its own. A marker later than `staleEndWait` can still reach the next request.
    */
  private def cancelAwaitingEnd(cancel: F[Unit], ended: F[Boolean], staleEndWait: FiniteDuration)(
      isEnd: ResponseMsg => Boolean
  ): F[Unit] =
    ended.flatMap {
      case true  => cancel
      case false => subscribe.use(msgs => cancel >> msgs.exists(isEnd).compile.drain.timeout(staleEndWait).attempt.void)
    }

  def cancelPositions: F[Unit] =
    requestOnly[CancelPositions](CancelPositions())

  /** Subscribes to the values and portfolio of `account` (as given by the managed accounts, e.g. "DU1234567"; empty
    * means the only account of a non-advisor login). TWS first sends the whole account, then accountDownloadEnd, then
    * changes about every 3 minutes. The stream never ends by itself; when it is stopped, the subscription is
    * cancelled. Fails if an update cannot be read, on a TWS error without request id and when the connection is lost.
    * Concurrent calls wait for each other; if one is stopped before its accountDownloadEnd arrived, the next one also
    * waits (at most `staleEndWait`) for that late accountDownloadEnd, so it cannot end the next snapshot early.
    */
  def reqAccountUpdates(
      account: String,
      staleEndWait: FiniteDuration = Portfolio.DefaultStaleEndWait
  ): Stream[F, Portfolio.AccountUpdate] =
    val unsubscribe = requestOnly[ReqAccountUpdates](ReqAccountUpdates(subscribe = false, acctCode = account))
    Stream.resource(accountUpdatesLock.lock) >> Stream.eval(cats.effect.Ref.of[F, Boolean](false)).flatMap { ended =>
      requestThrough[ReqAccountUpdates, Portfolio.AccountUpdate](ReqAccountUpdates(subscribe = true, acctCode = account))(
        Portfolio.accountUpdates(account)
      ).evalTap {
        case _: AccountDownloadEnd => ended.set(true)
        case _                     => Async[F].unit
      }.onFinalize(cancelAwaitingEnd(unsubscribe, ended.get, staleEndWait) {
        case AccountDownloadEnd(name) => account.isEmpty || name == account
        case _                        => false
      })
    }

  /** The values and portfolio of `account` as one snapshot, up to accountDownloadEnd; then the subscription is
    * cancelled. TWS sends nothing for an unknown account, so put a timeout on it.
    */
  def accountSnapshot(account: String): F[Portfolio.AccountSnapshot] =
    reqAccountUpdates(account).through(Portfolio.snapshot(account)).compile.lastOrError

  /** A summary of the accounts in `group` ("All" for every account of this login): one line per account and tag
    * (e.g. NetLiquidation), with its value and currency, as a stream that completes when TWS has sent them all
    * (accountSummaryEnd). The request is cancelled when the stream ends or is stopped, as TWS would otherwise resend
    * the summary every 3 minutes. Fails on a TWS error for `reqId` (e.g. more than 2 summaries open at a time), on an
    * unreadable summary line and when the connection is lost.
    */
  def reqAccountSummary(
      reqId: Int,
      group: String = "All",
      tags: String = Portfolio.AccountSummaryTags.All
  ): Stream[F, AccountSummary] =
    requestThrough[ReqAccountSummary, AccountSummary](ReqAccountSummary(reqId = reqId, group = group, tags = tags))(
      Portfolio.accountSummary(reqId)
    ).onFinalize(cancelAccountSummary(reqId))

  def cancelAccountSummary(reqId: Int): F[Unit] =
    requestOnly[CancelAccountSummary](CancelAccountSummary(reqId = reqId))

  /** The daily, unrealized and realized profit and loss of `account` (optionally of one model), updated by TWS about
    * once a second while it changes. The stream never ends by itself; when it is stopped, the request is cancelled.
    * Fails on a TWS error for `reqId`, on an unreadable update and when the connection is lost.
    */
  def reqPnL(reqId: Int, account: String, modelCode: String = ""): Stream[F, PnL] =
    requestThrough[ReqPnL, PnL](ReqPnL(reqId = reqId, account = account, modelCode = modelCode))(
      ProfitAndLoss.pnl(reqId)
    ).onFinalize(cancelPnL(reqId))

  def cancelPnL(reqId: Int): F[Unit] = requestOnly[CancelPnL](CancelPnL(reqId = reqId))

  /** Like [[reqPnL]], for the position in instrument `conId` of `account`: also its size and market value. */
  def reqPnLSingle(reqId: Int, account: String, conId: Int, modelCode: String = ""): Stream[F, PnLSingle] =
    requestThrough[ReqPnLSingle, PnLSingle](
      ReqPnLSingle(reqId = reqId, account = account, modelCode = modelCode, conId = conId)
    )(ProfitAndLoss.pnlSingle(reqId)).onFinalize(cancelPnLSingle(reqId))

  def cancelPnLSingle(reqId: Int): F[Unit] = requestOnly[CancelPnLSingle](CancelPnLSingle(reqId = reqId))

  /** Places an order and streams its open-order, status and error events until the order is done (cancelled, filled
    * or inactive) or a TWS error ends it. Refuses unless [[requirePaperSession]] succeeds. Allocate `orderId` with
    * [[nextValidOrderId]].
    */
  def placeOrder(orderId: Int, contract: Contract, order: Order): Stream[F, Orders.OrderEvent] =
    Stream.eval(requirePaperSession) >>
      requestThrough[PlaceOrder, Orders.OrderEvent](
        PlaceOrder(id = orderId, contract = contract, order = order.copy(orderId = orderId))
      )(Orders.eventsOf(orderId))

  /** Cancels an order this program placed. Refuses unless [[requirePaperSession]] succeeds. */
  def cancelOrder(orderId: Int, manualCancelOrderTime: String = ""): F[Unit] =
    requirePaperSession >>
      requestOnly[CancelOrder](CancelOrder(id = orderId, manualCancelOrderTime = manualCancelOrderTime))

  /** Cancels every open order in TWS (API and manually entered). Refuses unless [[requirePaperSession]] succeeds. */
  def reqGlobalCancel: F[Unit] =
    requirePaperSession >> requestOnly[ReqGlobalCancel](ReqGlobalCancel())

  /** Asks TWS for the next unused order id (and reserves it). */
  def nextValidOrderId: F[Int] =
    subscribe.use(msgs =>
      requestOnly[ReqIds](ReqIds(numIds = 1)) *>
        msgs.collectFirst { case NextValidId(id) => id }.compile.lastOrError
    )

  def reqManagedAccts: F[ManagedAccounts] =
    requestSingleResponse[ReqManagedAccts, ManagedAccounts](ReqManagedAccts())

  def requestFA(faDataType: Int): F[ReceiveFA] =
    for {
      _ <- requestOnly[RequestFA](RequestFA(faDataType = faDataType))
      result <- fetchSingleResponse[ReceiveFA]
    } yield result

  def requestSingleResponse[Req <: RequestMsg: MsgEncoder, Resp <: ResponseMsg: ClassTag](request: Req): F[Resp] =
    for
      _ <- requestOnly[Req](request)
      result <- fetchSingleResponse[Resp]
    yield result

  def requestStreamResponse[Req <: RequestMsg: MsgEncoder, Resp <: ResponseMsg: ClassTag](
      request: Req
  ): Stream[F, Resp] =
    for
      _ <- Stream.eval(requestOnly[Req](request))
      result <- fetchResponseStream[Resp]
    yield result

end IBClient

object IBClient:

  opaque type ServerVersion = Int

  extension (version: ServerVersion)
    def >=(otherVersion: ServerVersion): Boolean = version >= otherVersion
    def ==(otherVersion: ServerVersion): Boolean = version == otherVersion
    def <(otherVersion: ServerVersion): Boolean = version < otherVersion
    def toInt: Int = version

  object ServerVersion:
    def apply(value: Int): ServerVersion = value

  val MIN_SERVER_VER_REAL_TIME_BARS: ServerVersion = 34
  val MIN_SERVER_VER_SCALE_ORDERS: ServerVersion = 35
  val MIN_SERVER_VER_SNAPSHOT_MKT_DATA: ServerVersion = 35
  val MIN_SERVER_VER_SSHORT_COMBO_LEGS: ServerVersion = 35
  val MIN_SERVER_VER_WHAT_IF_ORDERS: ServerVersion = 36
  val MIN_SERVER_VER_CONTRACT_CONID: ServerVersion = 37
  val MIN_SERVER_VER_PTA_ORDERS: ServerVersion = 39
  val MIN_SERVER_VER_FUNDAMENTAL_DATA: ServerVersion = 40
  val MIN_SERVER_VER_DELTA_NEUTRAL: ServerVersion = 40
  val MIN_SERVER_VER_CONTRACT_DATA_CHAIN: ServerVersion = 40
  val MIN_SERVER_VER_SCALE_ORDERS2: ServerVersion = 40
  val MIN_SERVER_VER_ALGO_ORDERS: ServerVersion = 41
  val MIN_SERVER_VER_EXECUTION_DATA_CHAIN: ServerVersion = 42
  val MIN_SERVER_VER_NOT_HELD: ServerVersion = 44
  val MIN_SERVER_VER_SEC_ID_TYPE: ServerVersion = 45
  val MIN_SERVER_VER_PLACE_ORDER_CONID: ServerVersion = 46
  val MIN_SERVER_VER_REQ_MKT_DATA_CONID: ServerVersion = 47
  val MIN_SERVER_VER_REQ_CALC_IMPLIED_VOLAT: ServerVersion = 49
  val MIN_SERVER_VER_REQ_CALC_OPTION_PRICE: ServerVersion = 50
  val MIN_SERVER_VER_CANCEL_CALC_IMPLIED_VOLAT: ServerVersion = 50
  val MIN_SERVER_VER_CANCEL_CALC_OPTION_PRICE: ServerVersion = 50
  val MIN_SERVER_VER_SSHORTX_OLD: ServerVersion = 51
  val MIN_SERVER_VER_SSHORTX: ServerVersion = 52
  val MIN_SERVER_VER_REQ_GLOBAL_CANCEL: ServerVersion = 53
  val MIN_SERVER_VER_HEDGE_ORDERS: ServerVersion = 54
  val MIN_SERVER_VER_REQ_MARKET_DATA_TYPE: ServerVersion = 55
  val MIN_SERVER_VER_OPT_OUT_SMART_ROUTING: ServerVersion = 56
  val MIN_SERVER_VER_SMART_COMBO_ROUTING_PARAMS: ServerVersion = 57
  val MIN_SERVER_VER_DELTA_NEUTRAL_CONID: ServerVersion = 58
  val MIN_SERVER_VER_SCALE_ORDERS3: ServerVersion = 60
  val MIN_SERVER_VER_ORDER_COMBO_LEGS_PRICE: ServerVersion = 61
  val MIN_SERVER_VER_TRAILING_PERCENT: ServerVersion = 62
  val MIN_SERVER_VER_DELTA_NEUTRAL_OPEN_CLOSE: ServerVersion = 66
  val MIN_SERVER_VER_ACCT_SUMMARY: ServerVersion = 67
  val MIN_SERVER_VER_TRADING_CLASS: ServerVersion = 68
  val MIN_SERVER_VER_SCALE_TABLE: ServerVersion = 69
  val MIN_SERVER_VER_LINKING: ServerVersion = 70
  val MIN_SERVER_VER_ALGO_ID: ServerVersion = 71
  val MIN_SERVER_VER_OPTIONAL_CAPABILITIES: ServerVersion = 72
  val MIN_SERVER_VER_ORDER_SOLICITED: ServerVersion = 73
  val MIN_SERVER_VER_LINKING_AUTH: ServerVersion = 74
  val MIN_SERVER_VER_PRIMARYEXCH: ServerVersion = 75
  val MIN_SERVER_VER_RANDOMIZE_SIZE_AND_PRICE: ServerVersion = 76
  val MIN_SERVER_VER_FRACTIONAL_POSITIONS: ServerVersion = 101
  val MIN_SERVER_VER_PEGGED_TO_BENCHMARK: ServerVersion = 102
  val MIN_SERVER_VER_MODELS_SUPPORT: ServerVersion = 103
  val MIN_SERVER_VER_SEC_DEF_OPT_PARAMS_REQ: ServerVersion = 104
  val MIN_SERVER_VER_EXT_OPERATOR: ServerVersion = 105
  val MIN_SERVER_VER_SOFT_DOLLAR_TIER: ServerVersion = 106
  val MIN_SERVER_VER_REQ_FAMILY_CODES: ServerVersion = 107
  val MIN_SERVER_VER_REQ_MATCHING_SYMBOLS: ServerVersion = 108
  val MIN_SERVER_VER_PAST_LIMIT: ServerVersion = 109
  val MIN_SERVER_VER_MD_SIZE_MULTIPLIER: ServerVersion = 110
  val MIN_SERVER_VER_CASH_QTY: ServerVersion = 111
  val MIN_SERVER_VER_REQ_MKT_DEPTH_EXCHANGES: ServerVersion = 112
  val MIN_SERVER_VER_TICK_NEWS: ServerVersion = 113
  val MIN_SERVER_VER_REQ_SMART_COMPONENTS: ServerVersion = 114
  val MIN_SERVER_VER_REQ_NEWS_PROVIDERS: ServerVersion = 115
  val MIN_SERVER_VER_REQ_NEWS_ARTICLE: ServerVersion = 116
  val MIN_SERVER_VER_REQ_HISTORICAL_NEWS: ServerVersion = 117
  val MIN_SERVER_VER_REQ_HEAD_TIMESTAMP: ServerVersion = 118
  val MIN_SERVER_VER_REQ_HISTOGRAM: ServerVersion = 119
  val MIN_SERVER_VER_SERVICE_DATA_TYPE: ServerVersion = 120
  val MIN_SERVER_VER_AGG_GROUP: ServerVersion = 121
  val MIN_SERVER_VER_UNDERLYING_INFO: ServerVersion = 122
  val MIN_SERVER_VER_CANCEL_HEADTIMESTAMP: ServerVersion = 123
  val MIN_SERVER_VER_SYNT_REALTIME_BARS: ServerVersion = 124
  val MIN_SERVER_VER_CFD_REROUTE: ServerVersion = 125
  val MIN_SERVER_VER_MARKET_RULES: ServerVersion = 126
  val MIN_SERVER_VER_PNL: ServerVersion = 127
  val MIN_SERVER_VER_NEWS_QUERY_ORIGINS: ServerVersion = 128
  val MIN_SERVER_VER_UNREALIZED_PNL: ServerVersion = 129
  val MIN_SERVER_VER_HISTORICAL_TICKS: ServerVersion = 130
  val MIN_SERVER_VER_MARKET_CAP_PRICE: ServerVersion = 131
  val MIN_SERVER_VER_PRE_OPEN_BID_ASK: ServerVersion = 132
  val MIN_SERVER_VER_REAL_EXPIRATION_DATE: ServerVersion = 134
  val MIN_SERVER_VER_REALIZED_PNL: ServerVersion = 135
  val MIN_SERVER_VER_LAST_LIQUIDITY: ServerVersion = 136
  val MIN_SERVER_VER_TICK_BY_TICK: ServerVersion = 137
  val MIN_SERVER_VER_DECISION_MAKER: ServerVersion = 138
  val MIN_SERVER_VER_MIFID_EXECUTION: ServerVersion = 139
  val MIN_SERVER_VER_TICK_BY_TICK_IGNORE_SIZE: ServerVersion = 140
  val MIN_SERVER_VER_AUTO_PRICE_FOR_HEDGE: ServerVersion = 141
  val MIN_SERVER_VER_WHAT_IF_EXT_FIELDS: ServerVersion = 142
  val MIN_SERVER_VER_SCANNER_GENERIC_OPTS: ServerVersion = 143
  val MIN_SERVER_VER_API_BIND_ORDER: ServerVersion = 144
  val MIN_SERVER_VER_ORDER_CONTAINER: ServerVersion = 145
  val MIN_SERVER_VER_SMART_DEPTH: ServerVersion = 146
  val MIN_SERVER_VER_REMOVE_NULL_ALL_CASTING: ServerVersion = 147
  val MIN_SERVER_VER_D_PEG_ORDERS: ServerVersion = 148
  val MIN_SERVER_VER_MKT_DEPTH_PRIM_EXCHANGE: ServerVersion = 149
  val MIN_SERVER_VER_REQ_COMPLETED_ORDERS: ServerVersion = 150
  val MIN_SERVER_VER_PRICE_MGMT_ALGO: ServerVersion = 151
  val MIN_SERVER_VER_STOCK_TYPE: ServerVersion = 152
  val MIN_SERVER_VER_ENCODE_MSG_ASCII7: ServerVersion = 153
  val MIN_SERVER_VER_SEND_ALL_FAMILY_CODES: ServerVersion = 154
  val MIN_SERVER_VER_NO_DEFAULT_OPEN_CLOSE: ServerVersion = 155
  val MIN_SERVER_VER_PRICE_BASED_VOLATILITY: ServerVersion = 156
  val MIN_SERVER_VER_REPLACE_FA_END: ServerVersion = 157
  val MIN_SERVER_VER_DURATION: ServerVersion = 158
  val MIN_SERVER_VER_MARKET_DATA_IN_SHARES: ServerVersion = 159
  val MIN_SERVER_VER_POST_TO_ATS: ServerVersion = 160
  val MIN_SERVER_VER_WSHE_CALENDAR: ServerVersion = 161
  val MIN_SERVER_VER_AUTO_CANCEL_PARENT: ServerVersion = 162
  val MIN_SERVER_VER_FRACTIONAL_SIZE_SUPPORT: ServerVersion = 163
  val MIN_SERVER_VER_SIZE_RULES: ServerVersion = 164
  val MIN_SERVER_VER_HISTORICAL_SCHEDULE: ServerVersion = 165
  val MIN_SERVER_VER_ADVANCED_ORDER_REJECT: ServerVersion = 166
  val MIN_SERVER_VER_USER_INFO: ServerVersion = 167
  val MIN_SERVER_VER_CRYPTO_AGGREGATED_TRADES: ServerVersion = 168
  val MIN_SERVER_VER_MANUAL_ORDER_TIME: ServerVersion = 169
  val MIN_SERVER_VER_PEGBEST_PEGMID_OFFSETS: ServerVersion = 170
  val MIN_SERVER_VER_WSH_EVENT_DATA_FILTERS: ServerVersion = 171
  val MIN_SERVER_VER_IPO_PRICES: ServerVersion = 172
  val MIN_SERVER_VER_WSH_EVENT_DATA_FILTERS_DATE: ServerVersion = 173
  val MIN_SERVER_VER_INSTRUMENT_TIMEZONE: ServerVersion = 174
  val MIN_SERVER_VER_HMDS_MARKET_DATA_IN_SHARES: ServerVersion = 175
  val MIN_SERVER_VER_BOND_ISSUERID: ServerVersion = 176
  val MIN_SERVER_VER_FA_PROFILE_DESUPPORT: ServerVersion = 177

  val MIN_VERSION: ServerVersion = 100
  // envelope encoding, applicable to useV100Plus mode only
  val MAX_VERSION: ServerVersion = MIN_SERVER_VER_FA_PROFILE_DESUPPORT // ditto

  def buildVersionString(
      minVersion: ServerVersion,
      maxVersion: ServerVersion
  ): String =
    "v" + (if minVersion < maxVersion then minVersion + ".." + maxVersion
           else minVersion)
