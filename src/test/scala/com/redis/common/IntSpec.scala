package com.redis.common

import com.redis.api.BaseApi
import org.scalatest.{BeforeAndAfterEach, Suite}

trait IntSpec extends BeforeAndAfterEach with RedisDocker {
  that: Suite =>

  protected def r: BaseApi with AutoCloseable

  override def afterAll(): Unit = {
    r.close()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    r.flushall
    super.afterEach()
  }

}
