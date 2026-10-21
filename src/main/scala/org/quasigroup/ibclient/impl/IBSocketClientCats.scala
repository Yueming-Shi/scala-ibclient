package org.quasigroup.ibclient.impl

import org.quasigroup.ibclient.IBClient
import org.quasigroup.ibclient.IBClient.*

import org.quasigroup.ibclient.exceptions.InvalidMessageLengthException

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

import cats.Applicative
import cats.effect.*
import cats.effect.std.{Console, Mutex, Queue}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.*
import fs2.concurrent.*
import fs2.interop.scodec.StreamDecoder
import fs2.io.net.*
import scodec.*
import scodec.Err.General
import scodec.bits.*
import scodec.codecs.*

import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import java.time.ZoneId

class IBSocketClientCats[F[_]: Async: Console](
    socket: Socket[F],
    optionalCapabilities: Option[String],
    override val openOrdersLock: Mutex[F],
    override val positionsLock: Mutex[F],
    override val accountUpdatesLock: Mutex[F],
    override val orderIdsLock: Mutex[F],
    private val connectedPort: Port,
    private val liveTrading: IBSocketClientCats.LiveTrading,
    private val sessionAccounts: Deferred[F, String]
) extends IBClient[F]:
  import IBSocketClientCats.{*, given}

  private val requestRateLimiterRef: Ref[F, Int] = Ref.unsafe(50)
  private val msgPushingFiberDeferred =
    Deferred.unsafe[F, Fiber[F, Throwable, Unit]]
  private val msgTopicDeferred = Deferred.unsafe[F, Topic[F, ResponseMsg]]
  private val msgPullingFiberDeferred =
    Deferred.unsafe[F, Fiber[F, Throwable, Unit]]
  private val _clientId = Deferred.unsafe[F, Int]
  private val _serverVersion = Deferred.unsafe[F, IBClient.ServerVersion]
  private val _serverTimeZone: Ref[F, ZoneId] = Ref.unsafe(ZoneId.systemDefault())

  private def startMsgConsumption: F[Unit] =
    for
      given ServerVersion <- _serverVersion.get
      msgQueue <- Queue.unbounded[F, ResponseMsg]
      msgTopic <- Topic[F, ResponseMsg]
      pushFiber <-
        Stream
          .awakeEvery(1.second)
          .evalTap(_ => requestRateLimiterRef.set(50))
          .compile
          .drain
          .start
      pullFiber <- Stream
        .fromQueueUnterminated(msgQueue)
        .concurrently(incoming(socket.reads).evalTap(Console[F].println).evalTap(msgQueue.offer))
        .through(msgTopic.publish)
        .compile
        .drain
        .start
      _ <- msgPullingFiberDeferred.complete(pullFiber)
      _ <- msgPushingFiberDeferred.complete(pushFiber)
      _ <- msgTopicDeferred.complete(msgTopic)
    yield ()

  override def subscribe: Resource[F, Stream[F, ResponseMsg]] =
    Resource.eval(msgTopicDeferred.get).flatMap(_.subscribeAwait(Int.MaxValue))

  /** The comma-separated account ids this TWS session manages. */
  def managedAccounts: F[String] =
    subscribe.use(msgs =>
      requestOnly[ReqManagedAccts](ReqManagedAccts()) *>
        msgs.collectFirst { case ManagedAccounts(accounts) => accounts }.compile.lastOrError
    )

  override def requirePaperSession: F[Unit] =
    sessionAccounts.get.flatMap(accounts =>
      checkPaperOrders(connectedPort, liveTrading, accounts).liftTo[F]
    )

  override def fetchSingleResponse[Resp <: ResponseMsg: ClassTag]: F[Resp] =
    for
      msgStream <- msgTopicDeferred.get
      resp <- msgStream.subscribeUnbounded
        .collectFirst { case item: Resp => item }
        .evalTap(item => Console[F].println(s"fetched msg:$item"))
        .compile
        .lastOrError
    yield resp

  override def fetchResponseStream[Resp <: ResponseMsg: ClassTag]: Stream[F, Resp] =
    for
      topic <- Stream.eval(msgTopicDeferred.get)
      position <- topic.subscribeUnbounded
        .dropWhile {
          case item: Resp => false
          case _          => true
        }
        .collectWhile { case item: Resp => item }
        .evalTap(item => Console[F].println(s"fetched msg:$item"))
    yield position

  override def requestOnly[Req <: RequestMsg: MsgEncoder](
      request: Req
  ): F[Unit] =
    for
      given ServerVersion <- _serverVersion.get
      bytes <- encode[F, Req](request)
      _ <- requestRateLimiterRef.get.iterateUntil(_ > 0)
      _ <- requestRateLimiterRef.update(_ - 1)
      _ <- socket.write(Chunk.array(bytes))
    yield ()

  private def eConnect(clientId: Int): F[ConnectionAck] =
    for
      encoded <- ("API\u0000".getBytes ++ unsafeEncode[String](
        buildVersionString(MIN_VERSION, MAX_VERSION)
      )).pure
      _ <- socket.write(Chunk.array(encoded))
      sizeRaw <- socket.read(4)
      resultRaw <- sizeRaw
        .map(_.toByteVector.toInt())
        .flatTraverse[F, Chunk[Byte]](msgSize =>
          if msgSize <= MAX_MSG_LENGTH then socket.read(msgSize)
          else
            Async[F].raiseError(
              new InvalidMessageLengthException(
                "message size is too large: " + msgSize
              )
            )
        )
      result <- resultRaw
        .map(_.toByteVector.decodeUtf8Lenient.split(0.toChar))
        .liftTo[F](
          new InvalidMessageLengthException("message empty")
        )
      resp <- summon[Decoder[ConnectionAck]](result).liftTo[F]
      _ <- _clientId.complete(clientId)
      _ <- _serverVersion.complete(resp.serverVersion)
      _ <- Console[F].println("Current server version:" + resp.serverVersion)
      _ <- Console[F].println("Current server timezone:" + resp.time)
      _ <- startAPI
    yield resp

  private def eDisconnect: F[Unit] =
    for
      _ <- msgPushingFiberDeferred.get.flatMap(_.cancel)
      _ <- msgPullingFiberDeferred.get.flatMap(_.cancel)
      _ <- Console[F].println("Msg pull stopped")
    yield ()

  private def startAPI: F[Unit] =
    for
      clientId <- _clientId.get
      _ <- requestOnly[StartAPI](
        StartAPI(
          clientId = clientId,
          optionalCapabilities = optionalCapabilities.getOrElse("")
        )
      )
      _ <- Console[F].println("Start msg pulling")
      _ <- startMsgConsumption
    yield ()

object IBSocketClientCats:
  final val MAX_MSG_LENGTH: Int = 0xffffff

  /** Splits the TWS byte stream into messages: a 4-byte big-endian length, then NUL-separated fields. */
  val frames: StreamDecoder[Array[String]] =
    for
      msgSize <- StreamDecoder.many(int32)
      rawMessage <-
        if msgSize <= MAX_MSG_LENGTH then
          StreamDecoder.once(
            bytes(msgSize).map(_.decodeUtf8Lenient.split(0.toChar))
          )
        else
          StreamDecoder.raiseError(
            InvalidMessageLengthException(
              "message size is too large: " + msgSize
            )
          )
    yield rawMessage

  /** The messages read from TWS, always ending with [[ResponseMsg.ConnectionClosed]]: when TWS sends it, when the
    * socket reaches its end, or when reading fails, so that subscribers never wait on a dead connection.
    */
  def incoming[F[_]: RaiseThrowable: Applicative: Console](reads: Stream[F, Byte])(using
      IBClient.ServerVersion
  ): Stream[F, ResponseMsg] =
    (reads.through(frames.toPipeByte).through(decodeOrReport) ++ Stream.emit(ConnectionClosed))
      .handleErrorWith(e =>
        Stream.exec(Console[F].errorln(s"TWS connection failed: $e")) ++ Stream.emit(ConnectionClosed)
      )
      .takeThrough(_ != ConnectionClosed)

  /** Decodes messages. One that cannot be read does not stop the connection: it is reported on stderr and passed on
    * as [[ResponseMsg.Unreadable]], so streams waiting for that kind of message can fail rather than miss it.
    */
  def decodeOrReport[F[_]: Applicative: Console](using IBClient.ServerVersion): Pipe[F, Array[String], ResponseMsg] =
    _.evalMap(fields =>
      Either
        .catchNonFatal(decode(fields))
        .flatten
        .fold(
          e =>
            Console[F]
              .errorln(s"unreadable message ${fields.mkString("[", ",", "]")}: $e")
              .as(Unreadable(fields.toList, e.toString, summon[IBClient.ServerVersion].toInt)),
          _.pure[F]
        )
    )

  /** Decodes raw TWS bytes into response messages. */
  def responses[F[_]: RaiseThrowable](using IBClient.ServerVersion): Pipe[F, Byte, ResponseMsg] =
    _.through(frames.toPipeByte).flatMap(fields => Stream.fromEither[F](decode(fields)))

  final case class ConnectionAck(
      serverVersion: IBClient.ServerVersion,
      time: String
  )

  inline given Decoder[IBClient.ServerVersion] =
    summon[Decoder[Int]].map(IBClient.ServerVersion.apply)

  /** TWS paper-trading API port. This is the default. */
  val PaperPort: Port = port"7497"

  /** Ports that reach a LIVE account by default (TWS live / IB Gateway live). */
  val LivePorts: Set[Port] = Set(port"7496", port"4001")

  /** Explicit opt-in needed before [[make]] will use a live port or a live account. */
  enum LiveTrading:
    case Refused, IUnderstandThisIsARealMoneyAccount

  final case class LivePortRefused(port: Port)
      extends IllegalArgumentException(
        s"port $port is a LIVE trading port; pass LiveTrading.IUnderstandThisIsARealMoneyAccount to use it"
      )

  final case class LiveAccountRefused(accounts: String)
      extends IllegalStateException(
        s"TWS manages non-paper account(s) [$accounts]; pass LiveTrading.IUnderstandThisIsARealMoneyAccount to use them"
      )

  /** Pure check run before any socket is opened. */
  def checkPort(port: Port, liveTrading: LiveTrading = LiveTrading.Refused): Either[Throwable, Port] =
    Either.cond(
      !LivePorts(port) || liveTrading == LiveTrading.IUnderstandThisIsARealMoneyAccount,
      port,
      LivePortRefused(port)
    )

  /** IB paper account ids start with "D" (DU..., DF...); live ones do not. */
  def isPaperAccount(account: String): Boolean = account.startsWith("D")

  final case class PaperOrdersRefused(reason: String)
      extends IllegalStateException(s"paper-only order operation refused: $reason")

  /** Order placement and cancellation: paper TWS port 7497, no live opt-in, every managed account a paper account. */
  def checkPaperOrders(
      port: Port,
      liveTrading: LiveTrading,
      accounts: String
  ): Either[Throwable, Unit] =
    for
      _ <- Either.cond(
        port == PaperPort,
        (),
        PaperOrdersRefused(s"connected port is $port, not the paper TWS port $PaperPort")
      )
      _ <- Either.cond(
        liveTrading == LiveTrading.Refused,
        (),
        PaperOrdersRefused("this connection opted in to live trading")
      )
      _ <- checkAccounts(accounts, LiveTrading.Refused).leftMap(e => PaperOrdersRefused(e.getMessage))
    yield ()

  /** Pure check run right after the handshake: every managed account must be a paper account (TWS ports are
    * configurable, so the port alone does not prove we are on the paper account).
    */
  def checkAccounts(accounts: String, liveTrading: LiveTrading = LiveTrading.Refused): Either[Throwable, String] =
    val ids = accounts.split(',').map(_.trim).filter(_.nonEmpty)
    Either.cond(
      liveTrading == LiveTrading.IUnderstandThisIsARealMoneyAccount || (ids.nonEmpty && ids.forall(isPaperAccount)),
      accounts,
      LiveAccountRefused(accounts)
    )

  /** Connection settings. The defaults connect to the paper account on this machine. */
  final case class Settings(
      host: Host = host"127.0.0.1",
      port: Port = PaperPort,
      clientId: Int = 10,
      optionalCapabilities: Option[String] = None,
      liveTrading: LiveTrading = LiveTrading.Refused
  )

  /** Connects to TWS. Refuses live ports before connecting, and live accounts right after the handshake, unless
    * `settings.liveTrading` opts in.
    */
  def make[F[_]: Async: Console](settings: Settings = Settings()): Resource[F, IBClient[F]] =
    for
      port <- Resource.eval(checkPort(settings.port, settings.liveTrading).liftTo[F])
      socket <- Network.forAsync[F].client(SocketAddress(settings.host, port))
      openOrdersLock <- Resource.eval(Mutex[F])
      positionsLock <- Resource.eval(Mutex[F])
      accountUpdatesLock <- Resource.eval(Mutex[F])
      orderIdsLock <- Resource.eval(Mutex[F])
      sessionAccounts <- Resource.eval(Deferred[F, String])
      client = new IBSocketClientCats[F](
        socket,
        settings.optionalCapabilities,
        openOrdersLock,
        positionsLock,
        accountUpdatesLock,
        orderIdsLock,
        port,
        settings.liveTrading,
        sessionAccounts
      )
      _ <- Resource.make(client.eConnect(settings.clientId))(_ => client.eDisconnect)
      _ <- Resource.eval(
        client.managedAccounts
          .timeout(10.seconds)
          .flatTap(sessionAccounts.complete)
          .flatMap(checkAccounts(_, settings.liveTrading).liftTo[F])
      )
    yield client
