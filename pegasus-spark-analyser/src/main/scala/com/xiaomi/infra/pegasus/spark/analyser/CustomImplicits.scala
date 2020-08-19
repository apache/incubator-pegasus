package com.xiaomi.infra.pegasus.spark.analyser

import org.apache.spark.SparkContext

/**
  * custom implicits object, you can:
  *
  * import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
  *
  * to use it.
  */
object CustomImplicits {

  /**
    * The implicit method of converting SparkContext to PegasusContext
    * @param context
    * @return PegasusContext
    */
  implicit def convert2PegasusContext(
      context: SparkContext
  ): PegasusContext =
    new PegasusContext(context)
}
