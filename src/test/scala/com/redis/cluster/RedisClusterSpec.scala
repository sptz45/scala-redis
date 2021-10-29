package com.redis.cluster

import com.redis.cluster.KeyTag.RegexKeyTag
import com.redis.common.IntClusterSpec
import org.scalatest.funspec.AnyFunSpec


class RedisClusterSpec extends AnyFunSpec
  with IntClusterSpec
  // with ClusterUnimplementedMethods
  // with ClusterIncompatibleTests
  with CommonRedisClusterSpec {

  override def rProvider() =
    new RedisCluster(nodes, Some(RegexKeyTag))

}
