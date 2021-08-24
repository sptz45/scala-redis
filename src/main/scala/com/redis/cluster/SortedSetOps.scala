package com.redis.cluster

import com.redis.RedisClient.{ASC, Aggregate, SUM, SortOrder}
import com.redis.api.SortedSetApi
import com.redis.serialization.{Format, Parse}

trait SortedSetOps extends SortedSetApi {
  rc: RedisClusterOps =>

  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zadd(key, score, member, scoreVals: _*))

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zrem(key, member, members: _*))

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): Option[Double] =
    processForKey(key)(_.zincrby(key, incr, member))

  override def zcard(key: Any)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zcard(key))

  override def zscore(key: Any, element: Any)(implicit format: Format): Option[Double] =
    processForKey(key)(_.zscore(key, element))

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    processForKey(key)(_.zrange[A](key, start, end, sortAs))

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    processForKey(key)(_.zrangeWithScore[A](key, start, end, sortAs))

  override def zrangebyscore[A](key: Any, min: Double = Double.NegativeInfinity,
                                minInclusive: Boolean = true, max: Double = Double.PositiveInfinity,
                                maxInclusive: Boolean = true, limit: Option[(Int, Int)],
                                sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    processForKey(key)(_.zrangebyscore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))

  override def zrangebyscoreWithScore[A](key: Any, min: Double = Double.NegativeInfinity,
                                         minInclusive: Boolean = true, max: Double = Double.PositiveInfinity,
                                         maxInclusive: Boolean = true, limit: Option[(Int, Int)],
                                         sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    processForKey(key)(_.zrangebyscoreWithScore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))

  override def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
                      minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zcount(key, min, max, minInclusive, maxInclusive))

  override def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zrank(key, member, reverse))

  override def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zremrangebyrank(key, start, end))

  override def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity,
                                end: Double = Double.PositiveInfinity)(implicit format: Format): Option[Long] =
    processForKey(key)(_.zremrangebyscore(key, start, end))

  override def zunionstore(dstKey: Any, keys: Iterable[Any],
                           aggregate: Aggregate = SUM)(implicit format: Format): Option[Long] =
    inSameNode((dstKey :: keys.toList): _*) { n =>
      n.zunionstore(dstKey, keys, aggregate)
    }

  override def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]],
                                   aggregate: Aggregate = SUM)(implicit format: Format): Option[Long] =
    inSameNode((dstKey :: kws.map(_._1).toList): _*) { n =>
      n.zunionstoreWeighted(dstKey, kws, aggregate)
    }

  override def zinterstore(dstKey: Any, keys: Iterable[Any],
                           aggregate: Aggregate = SUM)(implicit format: Format): Option[Long] =
    inSameNode((dstKey :: keys.toList): _*) { n =>
      n.zinterstore(dstKey, keys, aggregate)
    }

  override def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]],
                                   aggregate: Aggregate = SUM)(implicit format: Format): Option[Long] =
    inSameNode((dstKey :: kws.map(_._1).toList): _*) { n =>
      n.zinterstoreWeighted(dstKey, kws, aggregate)
    }

  override def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)])
                             (implicit format: Format, parse: Parse[A]): Option[List[A]] =
    processForKey(key)(_.zrangebylex(key, min, max, limit))

  override def zpopmax[A](key: Any, count: Int = 1)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    processForKey(key)(_.zpopmax[A](key, count))

  override def zpopmin[A](key: Any, count: Int = 1)(implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    processForKey(key)(_.zpopmax[A](key, count))

  override def bzpopmax[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V, Double)] =
    inSameNode((key :: keys.toList): _*) { n => n.bzpopmax[K, V](timeoutInSeconds, key, keys: _*) }

  override def bzpopmin[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V, Double)] =
    inSameNode((key :: keys.toList): _*) { n => n.bzpopmin[K, V](timeoutInSeconds, key, keys: _*) }

  override def zscan[A](key: Any, cursor: Int, pattern: Any, count: Int)
                       (implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    processForKey(key)(_.zscan(key, cursor, pattern, count))

}
