package com.redis.common

import com.redis.api.BaseApi
import com.redis.cluster.ClusterNode
import com.redis.serialization.Format
import org.scalatest.{BeforeAndAfterEach, Suite}

trait IntClusterSpec extends BeforeAndAfterEach with RedisDockerCluster {
  that: Suite =>

  protected def r: BaseApi with AutoCloseable
  protected val nodeNamePrefix = "node"

  protected lazy val nodes: List[ClusterNode] =
    runningContainers.zipWithIndex.map { case (c, i) =>
      ClusterNode(s"$nodeNamePrefix$i", redisContainerHost, redisContainerPort(c))
    }

  def formattedKey(key: Any)(implicit format: Format): Array[Byte] = {
    format(key)
  }

  override def afterAll(): Unit = {
    r.close()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    r.flushall
    super.afterEach()
  }
}
