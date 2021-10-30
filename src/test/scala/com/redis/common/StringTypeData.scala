package com.redis.common

import com.redis.api.{BaseApi, StringApi}
import org.scalatest.{GivenWhenThen, Informing, Suite}
import org.scalatest.matchers.should.Matchers

trait StringTypeData extends IntSpec with Matchers with GivenWhenThen {
  that: Suite with Informing =>

  // StringApi, so we could put some data to test
  override protected def r: BaseApi with StringApi with AutoCloseable

  protected val testData: Map[String, String] = Map(
    "key1" -> "value1",
    "key2" -> "value2",
    "key3" -> "value3",
    "key4" -> "value4",
    "key5" -> "value5"
  )

  protected val testKeys: List[String] = testData.keys.toList

  protected def withSampleData(testFun: => Any): Unit = {
    Given("Sample data on the cluster")
    testData.foreach { case (k, v) =>
      r.set(k, v)
    }
    testFun
  }

}

