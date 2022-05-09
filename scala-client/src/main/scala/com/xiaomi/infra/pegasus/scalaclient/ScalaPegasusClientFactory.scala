package com.xiaomi.infra.pegasus.scalaclient

import com.xiaomi.infra.pegasus.client.{ClientOptions, PegasusClient}

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 12:05 AM
  */
object ScalaPegasusClientFactory {

  def createClient(configPath: String): ScalaPegasusClient = {
    new ScalaPegasusClientImpl(new PegasusClient(configPath))
  }

  def createClient(options: ClientOptions): ScalaPegasusClient = {
    new ScalaPegasusClientImpl(new PegasusClient(options))
  }

}
