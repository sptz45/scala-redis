package com.redis

import com.redis.api.BaseApi
import com.redis.serialization._

trait BaseOperations extends BaseApi {
  self: Redis =>

  override def sort[A](key: String,
                       limit: Option[(Int, Int)] = None,
                       desc: Boolean = false,
                       alpha: Boolean = false,
                       by: Option[String] = None,
                       get: List[String] = Nil)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {

    val commands: List[Any] = makeSortArgs(key, limit, desc, alpha, by, get)
    send("SORT", commands)(asList)
  }

  private def makeSortArgs(key: String,
                           limit: Option[(Int, Int)] = None,
                           desc: Boolean = false,
                           alpha: Boolean = false,
                           by: Option[String] = None,
                           get: List[String] = Nil): List[Any] = {
    List(List(key), limit.map(l => List("LIMIT", l._1, l._2)).getOrElse(Nil)
      , (if (desc) List("DESC") else Nil)
      , (if (alpha) List("ALPHA") else Nil)
      , by.map(b => List("BY", b)).getOrElse(Nil)
      , get.flatMap(g => List("GET", g))
    ).flatten
  }

  override def sortNStore[A](key: String,
                             limit: Option[(Int, Int)] = None,
                             desc: Boolean = false,
                             alpha: Boolean = false,
                             by: Option[String] = None,
                             get: List[String] = Nil,
                             storeAt: String)(implicit format: Format, parse: Parse[A]): Option[Long] = {

    val commands = makeSortArgs(key, limit, desc, alpha, by, get) ::: List("STORE", storeAt)
    send("SORT", commands)(asLong)
  }

  override def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    send("KEYS", List(pattern))(asList)

  override def time[A](implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    send("TIME", false)(asList)

  @deprecated("use randomkey", "2.8")
  def randkey[A](implicit parse: Parse[A]): Option[A] =
    send("RANDOMKEY", false)(asBulk)

  override def randomkey[A](implicit parse: Parse[A]): Option[A] =
    send("RANDOMKEY", false)(asBulk)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    send("RENAME", List(oldkey, newkey))(asBoolean)

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    send("RENAMENX", List(oldkey, newkey))(asBoolean)

  override def dbsize: Option[Long] =
    send("DBSIZE", false)(asLong)

  override def exists(key: Any)(implicit format: Format): Boolean =
    send("EXISTS", List(key))(asBoolean)

  override def vexists(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    send("EXISTS", key :: keys.toList)(asLong)

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    send("DEL", key :: keys.toList)(asLong)

  override def getType(key: Any)(implicit format: Format): Option[String] =
    send("TYPE", List(key))(asString)

  override def expire(key: Any, ttl: Int)(implicit format: Format): Boolean =
    send("EXPIRE", List(key, ttl))(asBoolean)

  override def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): Boolean =
    send("PEXPIRE", List(key, ttlInMillis))(asBoolean)

  override def expireat(key: Any, timestamp: Long)(implicit format: Format): Boolean =
    send("EXPIREAT", List(key, timestamp))(asBoolean)

  override def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): Boolean =
    send("PEXPIREAT", List(key, timestampInMillis))(asBoolean)

  override def ttl(key: Any)(implicit format: Format): Option[Long] =
    send("TTL", List(key))(asLong)

  override def pttl(key: Any)(implicit format: Format): Option[Long] =
    send("PTTL", List(key))(asLong)

  override def select(index: Int): Boolean =
    send("SELECT", List(index))(if (asBoolean) {
      db = index
      true
    } else {
      false
    })

  override def flushdb: Boolean =
    send("FLUSHDB", false)(asBoolean)

  override def flushall: Boolean =
    send("FLUSHALL", false)(asBoolean)

  override def move(key: Any, db: Int)(implicit format: Format): Boolean =
    send("MOVE", List(key, db))(asBoolean)

  override def quit: Boolean =
    send("QUIT", false)(disconnect)

  override def auth(secret: Any)(implicit format: Format): Boolean =
    send("AUTH", List(secret))(asBoolean)

  override def persist(key: Any)(implicit format: Format): Boolean =
    send("PERSIST", List(key))(asBoolean)

  override def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    send("SCAN", cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x) (if (count == 10) Nil else List("count", count)))(asPair)

  override def ping: Option[String] =
    send("PING", false)(asString)

  override def watch(key: Any, keys: Any*)(implicit format: Format): Boolean =
    send("WATCH", key :: keys.toList)(asBoolean)

  override def unwatch(): Boolean =
    send("UNWATCH", false)(asBoolean)

  override def getConfig(key: Any = "*")(implicit format: Format): Option[Map[String, Option[String]]] =
    send("CONFIG", List("GET", key))(asList).map { ls =>
      ls.grouped(2).collect { case Some(k) :: v :: Nil => k -> v }.toMap
    }

  override def setConfig(key: Any, value: Any)(implicit format: Format): Option[String] =
    send("CONFIG", List("SET", key, value))(asString)
}
