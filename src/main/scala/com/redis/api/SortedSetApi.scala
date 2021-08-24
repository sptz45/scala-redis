package com.redis.api

import com.redis.RedisClient.{ASC, Aggregate, SUM, SortOrder}
import com.redis.serialization.{Format, Parse}

trait SortedSetApi {

  /**
   * Add the specified members having the specified score to the sorted set stored at key. (Variadic: >= 2.4)
   */
  def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format): Option[Long]

  /**
   * Remove the specified members from the sorted set value stored at key. (Variadic: >= 2.4)
   */
  def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Long]

  def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): Option[Double]

  def zcard(key: Any)(implicit format: Format): Option[Long]

  def zscore(key: Any, element: Any)(implicit format: Format): Option[Double]

  def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[A]]

  def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]]

  def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)])(implicit format: Format, parse: Parse[A]): Option[List[A]]

  def zrangebyscore[A](key: Any,
                       min: Double = Double.NegativeInfinity,
                       minInclusive: Boolean = true,
                       max: Double = Double.PositiveInfinity,
                       maxInclusive: Boolean = true,
                       limit: Option[(Int, Int)],
                       sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[A]]

  def zrangebyscoreWithScore[A](key: Any,
                                min: Double = Double.NegativeInfinity,
                                minInclusive: Boolean = true,
                                max: Double = Double.PositiveInfinity,
                                maxInclusive: Boolean = true,
                                limit: Option[(Int, Int)],
                                sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]]

  def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): Option[Long]

  def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): Option[Long]

  def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)(implicit format: Format): Option[Long]

  def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format): Option[Long]

  def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(implicit format: Format): Option[Long]

  def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format): Option[Long]

  def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(implicit format: Format): Option[Long]

  def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format): Option[Long]

  def zpopmax[A](key: Any, count: Int = 1)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]]

  def zpopmin[A](key: Any, count: Int = 1)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]]

  def bzpopmax[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V, Double)]

  def bzpopmin[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V, Double)]

  /**
   * Incrementally iterate sorted sets elements and associated scores (since 2.8)
   */
  def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])]
}
