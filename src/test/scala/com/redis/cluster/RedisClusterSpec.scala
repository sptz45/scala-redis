package com.redis.cluster

import com.redis.cluster.KeyTag.RegexKeyTag
import com.redis.common.IntClusterSpec
import org.scalatest.FunSpec


class RedisClusterSpec extends FunSpec
  with IntClusterSpec
  // with ClusterUnimplementedMethods
  // with ClusterIncompatibleTests
  with CommonRedisClusterSpec {

  override def rProvider() =
    new RedisCluster(nodes, Some(RegexKeyTag))

}
