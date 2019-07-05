package com.xiaomi.infra.pegasus.scalaclient

import java.util.Properties

import com.xiaomi.infra.pegasus.client.PegasusClient

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 12:05 AM
  */
object ScalaPegasusClientFactory {

  def createClient(configPath: String): ScalaPegasusClient = {
    new ScalaPegasusClientImpl(new PegasusClient(configPath))
  }

  def createClient(props: Properties): ScalaPegasusClient = {
    new ScalaPegasusClientImpl(new PegasusClient(props))
  }

}
