package com.xiaomi.infra.pegasus.scalaclient

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import Serializers._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/27/18 8:09 PM
  */

class TableSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    val table = "scala_test"

    "client basic get/set/del" should "work" in {
        withClient { c =>
            val hashKey = 12345L
            delHashKey(c, table, hashKey)

            c.set(table, hashKey, "sort_1", "value_1")

            c.exists(table, hashKey, "sort_1") should equal(true)
            c.exists(table, hashKey, "sort_2") should equal(false)
            c.sortKeyCount(table, hashKey) should equal(1)

            c.get(table, hashKey, "sort_1").as[String] should equal("value_1")
            c.get(table, hashKey, "sort_2").asOpt[String] should equal(None)

            c.del(table, hashKey, "sort_1")
            c.exists(table, hashKey, "sort_1") should equal(false)
            c.sortKeyCount(table, hashKey) should equal(0)
            c.get(table, hashKey, "sort_1").asOpt[String] should equal(None)
        }
    }

    "client multi set/get/del" should "work" in {
        withClient { c =>
            val hashKey = 12345L

            val values = Seq("sort_1" -> "value_1", "sort_2" -> "value_2", "sort_3" -> "value_3")
            val sortKeys = values.unzip._1

            delHashKey(c, table, hashKey)

            c.multiSet(table, hashKey, values)

            sortKeys.foreach { k => c.exists(table, hashKey, k) should equal(true) }

            c.sortKeyCount(table, hashKey) should equal(sortKeys.size)

            val multigetValues = c.multiGet(table, hashKey, sortKeys).as[String].values
            multigetValues.size should equal(sortKeys.size)
            (0 until sortKeys.size).foreach { i => multigetValues(i) should equal(values(i)) }

            val multiGetWithLimit = c.multiGet(table, hashKey, sortKeys, 1, 0).as[String].values
            multiGetWithLimit.size should equal(1)

            val multiGetWithNil = c.multiGet(table, hashKey, Seq[String]())
            multiGetWithNil.values.size should equal(sortKeys.size)

            val multiGetSortKeys = c.multiGetSortKeys(table, hashKey)
            multiGetSortKeys.as[String].values.toSet should equal(sortKeys.toSet)

            c.multiDel(table, hashKey, Seq("sort_1", "sort_2"))
            c.sortKeyCount(table, hashKey) should equal(1)

            c.multiDel(table, hashKey, Seq("sort_3"))
            c.sortKeyCount(table, hashKey) should equal(0)

            val multigetValues2 = c.multiGet(table, hashKey, sortKeys).as[String].values
            multigetValues2.size should equal(0)
        }
    }

    private def delHashKey[A](c: ScalaPegasusClient, table: String, hashKey: A)(implicit ser: Serializer[A]) = {
        val keys = c.multiGetSortKeys(table, hashKey)
        if (keys.values.nonEmpty) {
            c.multiDel(table, hashKey, keys.values)
        }
    }

    private def withClient(f: ScalaPegasusClient => Unit) = {
        var client: ScalaPegasusClient = null
        try {
            client = ScalaPegasusClientFactory.createClient("resource:///pegasus.properties")
            f(client)
        } finally {
            println("closing client")
            client.close
        }
    }
}