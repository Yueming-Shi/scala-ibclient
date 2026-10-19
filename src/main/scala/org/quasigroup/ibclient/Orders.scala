package org.quasigroup.ibclient

import org.quasigroup.ibclient.response.ResponseMsg
import org.quasigroup.ibclient.response.ResponseMsg.*

import fs2.Pipe

object Orders:

  /** Keeps the open orders until TWS says the list is complete (openOrderEnd). */
  def openOrders[F[_]]: Pipe[F, ResponseMsg, OpenOrder] =
    _.takeWhile(_ != OpenOrderEnd).collect { case order: OpenOrder => order }
