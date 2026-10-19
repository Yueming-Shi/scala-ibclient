package org.quasigroup.ibclient

import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.request.RequestMsg
import org.quasigroup.ibclient.request.MsgEncoders.{MsgEncoder, encode}
import org.quasigroup.ibclient.response.ResponseMsg

import cats.effect.{IO, Ref, Resource}
import fs2.{Chunk, Stream}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.reflect.ClassTag

/** Test helpers for the TWS wire format: a 4-byte big-endian length, then fields each ended by a NUL. */
object Wire:

  /** A recent TWS server version (TWS 10.19+). */
  given serverVersion: IBClient.ServerVersion = IBClient.ServerVersion(176)

  def frame(fields: String*): Array[Byte] =
    val body = fields.map(_ + "\u0000").mkString.getBytes(UTF_8)
    ByteBuffer.allocate(4).putInt(body.length).array ++ body

  /** Splits one framed message back into its fields (keeping empty ones); fails if the length prefix is wrong. */
  def fields(bytes: Array[Byte]): List[String] =
    val length = ByteBuffer.wrap(bytes.take(4)).getInt
    require(length == bytes.length - 4, s"length prefix $length != body ${bytes.length - 4}")
    new String(bytes.drop(4), UTF_8).split("\u0000", -1).toList.dropRight(1)

  def encoded[Req <: RequestMsg: MsgEncoder](request: Req): IO[List[String]] =
    encode[IO, Req](request).map(fields)

  /** Decodes canned bytes, delivered in small chunks to exercise message reassembly. */
  def decodeAll(frames: Array[Byte]*): IO[List[ResponseMsg]] =
    Stream
      .emits(frames.flatten)
      .chunkN(3)
      .flatMap(Stream.chunk)
      .through(IBSocketClientCats.responses[IO])
      .compile
      .toList

  def decodeOne(fields: String*): IO[ResponseMsg] =
    decodeAll(frame(fields*)).map(_.head)

  /** A client without a socket: incoming messages are canned, sent requests are recorded as wire fields. */
  final class FakeClient(incoming: List[ResponseMsg], val sent: Ref[IO, Vector[List[String]]]) extends IBClient[IO]:
    def requestOnly[Req <: RequestMsg: MsgEncoder](request: Req): IO[Unit] =
      encoded(request).flatMap(f => sent.update(_ :+ f))
    def subscribe: Resource[IO, Stream[IO, ResponseMsg]] = Resource.pure(Stream.emits(incoming))
    def fetchSingleResponse[Resp <: ResponseMsg: ClassTag]: IO[Resp] =
      Stream.emits(incoming).covary[IO].collectFirst { case r: Resp => r }.compile.lastOrError
    def fetchResponseStream[Resp <: ResponseMsg: ClassTag]: Stream[IO, Resp] =
      Stream.emits(incoming).covary[IO].collect { case r: Resp => r }

  def fakeClient(incoming: ResponseMsg*): IO[FakeClient] =
    Ref.of[IO, Vector[List[String]]](Vector.empty).map(FakeClient(incoming.toList, _))
