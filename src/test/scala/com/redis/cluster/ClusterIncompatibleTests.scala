package com.redis.cluster

import com.redis.api._

/**
 * Tests that are incompatible at the moment and require rework or custom behaviour for cluster client
 */
trait ClusterIncompatibleTests
  extends BaseApiSpec
    with EvalApiSpec
    //    with GeoApiSpec
    //    with HyperLogLogApiSpec
    with HashApiSpec
    with ListApiSpec
    with NodeApiSpec
    with SetApiSpec
    // with SortedSetApiSpec
    with StringApiSpec {

  override protected def r: AutoCloseable
    with BaseApi
    with EvalApi
    //    with GeoApi
    //    with HyperLogLogApi
    with HashApi
    with ListApi
    with NodeApi
    with SetApi
    with SortedSetApi
    with StringApi

  override protected def blpop(): Unit = ()

  override protected def brpoplpush(): Unit = ()

  override protected def rpoplpush(): Unit = ()

  override protected def sinterEmpty(): Unit = ()

  override protected def sinterstore(): Unit = ()

  override protected def smoveError(): Unit = ()

  override protected def sunionstore(): Unit = ()

  // override protected def zunionT(): Unit = ()

  // override protected def zinterT(): Unit = ()

}
