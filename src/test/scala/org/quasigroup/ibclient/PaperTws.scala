package org.quasigroup.ibclient

import org.quasigroup.ibclient.impl.IBSocketClientCats

import cats.effect.IO
import com.comcast.ip4s.*
import fs2.io.net.Network

import scala.concurrent.duration.*

/** Helpers for integration tests: is the TWS paper API reachable on this machine? */
object PaperTws:

  val address: SocketAddress[Host] = SocketAddress(host"127.0.0.1", IBSocketClientCats.PaperPort)

  val skipReason: String = s"TWS paper API is not listening on $address"

  /** Opens and immediately closes a plain TCP connection to the paper port only. */
  val isListening: IO[Boolean] =
    Network[IO].client(address).use_.timeout(2.seconds).as(true).handleError(_ => false)
