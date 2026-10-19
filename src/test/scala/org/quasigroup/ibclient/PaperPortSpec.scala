package org.quasigroup.ibclient

import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.impl.IBSocketClientCats.*

import com.comcast.ip4s.*
import weaver.FunSuite

/** Unit tests (no TWS needed): the client defaults to the paper account and refuses live ports. */
object PaperPortSpec extends FunSuite:

  private val defaults = Settings()
  private val paper = port"7497"
  private val liveTws = port"7496"
  private val liveGateway = port"4001"
  private val paperGateway = port"4002"

  test("default port is the paper port 7497") {
    expect(defaults.port == paper) and expect(PaperPort == paper)
  }

  test("default settings pass the live-port guard") {
    expect(checkPort(defaults.port) == Right(paper))
  }

  test("live ports 7496 and 4001 are refused without an explicit opt-in") {
    expect(checkPort(liveTws) == Left(LivePortRefused(liveTws))) and
      expect(checkPort(liveGateway) == Left(LivePortRefused(liveGateway))) and
      expect(checkPort(liveTws, LiveTrading.Refused).isLeft)
  }

  test("live ports are only accepted with the explicit opt-in") {
    expect(checkPort(liveTws, LiveTrading.IUnderstandThisIsARealMoneyAccount) == Right(liveTws))
  }

  test("other ports (e.g. IB Gateway paper 4002) are accepted") {
    expect(checkPort(paperGateway) == Right(paperGateway))
  }

  test("default settings never opt in to live trading") {
    expect(defaults.liveTrading == LiveTrading.Refused)
  }
