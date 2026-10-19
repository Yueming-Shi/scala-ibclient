package org.quasigroup.ibclient

import org.quasigroup.ibclient.impl.IBSocketClientCats
import org.quasigroup.ibclient.impl.IBSocketClientCats.*

import com.comcast.ip4s.*
import cats.effect.IO
import weaver.SimpleIOSuite

import scala.concurrent.duration.*

/** Unit tests (no TWS needed): the client defaults to the paper account and refuses live ports. */
object PaperPortSpec extends SimpleIOSuite:

  private val defaults = Settings()
  private val paper = port"7497"
  private val liveTws = port"7496"
  private val liveGateway = port"4001"
  private val paperGateway = port"4002"

  pureTest("default port is the paper port 7497") {
    expect(defaults.port == paper) and expect(PaperPort == paper)
  }

  pureTest("default settings pass the live-port guard") {
    expect(checkPort(defaults.port) == Right(paper))
  }

  pureTest("live ports 7496 and 4001 are refused without an explicit opt-in") {
    expect(checkPort(liveTws) == Left(LivePortRefused(liveTws))) and
      expect(checkPort(liveGateway) == Left(LivePortRefused(liveGateway))) and
      expect(checkPort(liveTws, LiveTrading.Refused).isLeft)
  }

  pureTest("live ports are only accepted with the explicit opt-in") {
    expect(checkPort(liveTws, LiveTrading.IUnderstandThisIsARealMoneyAccount) == Right(liveTws)) and
      expect(checkPort(liveGateway, LiveTrading.IUnderstandThisIsARealMoneyAccount) == Right(liveGateway))
  }

  pureTest("other ports (e.g. IB Gateway paper 4002) are accepted") {
    expect(checkPort(paperGateway) == Right(paperGateway))
  }

  pureTest("default settings never opt in to live trading") {
    expect(defaults.liveTrading == LiveTrading.Refused)
  }

  pureTest("paper accounts (DU..., DF...) pass the account guard") {
    expect(checkAccounts("DU1234567") == Right("DU1234567")) and
      expect(checkAccounts("DU1234567,DF7654321").isRight)
  }

  pureTest("live accounts, mixed lists and empty lists are refused without the opt-in") {
    expect(checkAccounts("U1234567") == Left(LiveAccountRefused("U1234567"))) and
      expect(checkAccounts("DU1234567,U7654321").isLeft) and
      expect(checkAccounts("").isLeft)
  }

  pureTest("live accounts are only accepted with the explicit opt-in") {
    expect(checkAccounts("U1234567", LiveTrading.IUnderstandThisIsARealMoneyAccount).isRight)
  }

  // 192.0.2.1 is a reserved, unroutable test address: even if the guard were broken this could never reach TWS.
  test("make refuses a live port before opening any connection") {
    IBSocketClientCats
      .make[IO](Settings(host = host"192.0.2.1", port = liveTws))
      .use_
      .timeout(5.seconds)
      .attempt
      .map(result => expect(result == Left(LivePortRefused(liveTws))))
  }
