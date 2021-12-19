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
    managedContainers.containers.zipWithIndex.map { case (c, i) =>
      ClusterNode(s"$nodeNamePrefix$i", redisContainerHost, redisContainerPort(c))
    }.toList

  def formattedKey(key: Any)(implicit format: Format): Array[Byte] = {
    format(key)
  }

  override def beforeStop(): Unit = r.close()

  override def afterEach(): Unit = {
    r.flushall
    super.afterEach()
  }
}
