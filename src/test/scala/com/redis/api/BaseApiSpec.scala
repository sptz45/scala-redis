package com.redis.api

import com.redis.common.{IntSpec, StringTypeData}
import com.redis.serialization.Parse
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._


trait BaseApiSpec extends AnyFunSpec
                     with Matchers
                     with StringTypeData
                     with IntSpec {

  // todo: remove SetApi, HashApi and ListApi
  override protected def r: BaseApi with StringApi with AutoCloseable with SetApi with HashApi with ListApi

  dbsize()
  del()
  exists()
  vexists()
  expire()
  getConfig()
  getType()
  keys()
  persist()
  ping()
  randomkey()
  rename()
  renamenx()
  setConfig()
  sort()
  sortNStore()
  time()

  protected def keys(): Unit = {
  describe("keys") {
    it("should fetch keys") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.keys("anshin*") match {
        case Some(s: List[Option[String]]) => s.size should equal(2)
        case None => fail("should have 2 elements")
      }
    }

    it("should fetch keys with spaces") {
      r.set("anshin 1", "debasish")
      r.set("anshin 2", "maulindu")
      r.keys("anshin*") match {
        case Some(s: List[Option[String]]) => s.size should equal(2)
        case None => fail("should have 2 elements")
      }
    }
  }
  }

  protected def time(): Unit = {
  describe("time") {
    it("should fetch a list of a timestamp and the number of microseconds elapsed in current second.") {
      r.time match {
        case Some(s: List[Option[String]]) => s.size should equal(2)
        case None => fail("should have 2 elements")
      }
    }

    it("should give a unix timestamp in seconds that is larger than the elapsed time in microseconds") {
      r.time match {
        case Some(Some(timestamp) :: Some(elaspedtime) :: Nil) =>
          (timestamp.toLong * 1000000L) should be > elaspedtime.toLong
        case _ => fail("should have a unix timestamp and an elasped time")
      }
    }
  }
  }

  protected def randomkey(): Unit = {
  describe("randomkey") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.randomkey match {
        case Some(s: String) => s should startWith("anshin")
        case None => fail("should have 2 elements")
      }
    }
  }
  }

  protected def rename(): Unit = {
  describe("rename") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.rename("anshin-2", "anshin-2-new") should equal(true)
      val thrown = the [Exception] thrownBy { r.rename("anshin-2", "anshin-2-new") }
      thrown.getMessage should equal ("ERR no such key")
    }
  }
  }

  protected def renamenx(): Unit = {
  describe("renamenx") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.renamenx("anshin-2", "anshin-2-new") should equal(true)
      r.renamenx("anshin-1", "anshin-2-new") should equal(false)
    }
  }
  }

  protected def dbsize(): Unit = {
  describe("dbsize") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.dbsize.get should equal(2)
    }
  }
  }

  protected def exists(): Unit = {
  describe("exists") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.exists("anshin-2") should equal(true)
      r.exists("anshin-1") should equal(true)
      r.exists("anshin-3") should equal(false)
    }
  }
  }

  protected def vexists(): Unit = {
  describe("vexists") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.vexists("anshin-2").get should equal(1)
      r.vexists("anshin-1", "anshin-2").get should equal(2)
      r.vexists("anshin-1", "anshin-1", "anshin-2").get should equal(3)
      r.vexists("anshin-3").get should equal(0)
    }
  }
  }

  protected def del(): Unit = {
  describe("del") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.del("anshin-2", "anshin-1").get should equal(2)
      r.del("anshin-2", "anshin-1").get should equal(0)
    }
  }
  }

  protected def getType(): Unit = {
  describe("type") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.getType("anshin-2").get should equal("string")
    }
  }
  }

  protected def expire(): Unit = {
  describe("expire") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.expire("anshin-2", 1000) should equal(true)
      r.ttl("anshin-2") should equal(Some(1000))
      r.expire("anshin-3", 1000) should equal(false)
    }
  }
  }

  protected def expireAndOthers(): Unit = {
  describe("expire, pexpire, expireat, pexpireat, ttl, pttl") {
    withSampleData {
      val timeout = 2.second
      val timeoutTimestamp = System.currentTimeMillis().millis + timeout

      When(s"We set expiry dates on some keys for $timeout")
      r.expire(testKeys(0), timeout.length.toInt)
      r.pexpire(testKeys(1), timeout.toMillis.toInt)
      r.expireat(testKeys(2), timeoutTimestamp.toSeconds)
      r.pexpireat(testKeys(3), timeoutTimestamp.toMillis)

      Then("It returns some ttl for the keys")
      r.ttl(testKeys(0)).get should be > 0L
      r.pttl(testKeys(0)).get should be > 0L
      r.ttl(testKeys(1)).get should be > 0L
      r.pttl(testKeys(1)).get should be > 0L
      r.ttl(testKeys(2)).get should be > 0L
      r.pttl(testKeys(2)).get should be > 0L
      r.ttl(testKeys(3)).get should be > 0L
      r.pttl(testKeys(3)).get should be > 0L

      Then("After ttl, keys should be long gone")
      Thread.sleep(timeout.toMillis + 1) // sorry :(
      val data = r.keys().get.flatten
      data.size should be(testData.size - 4)
      data should not contain testKeys(0)
      data should not contain testKeys(1)
      data should not contain testKeys(2)
      data should not contain testKeys(1)
    }
  }
  }

  protected def persist(): Unit = {
  describe("persist") {
    it("should give") {
      r.set("key-2", "maulindu")
      r.expire("key-2", 1000) should equal(true)
      r.ttl("key-2") should equal(Some(1000))
      r.persist("key-2") should equal(true)
      r.ttl("key-2") should equal(Some(-1))
      r.persist("key-3") should equal(false)
    }
  }
  }

  protected def sort(): Unit = {
  describe("sort") {
    it("should give") {
// sort[A](key:String, limit:Option[Pair[Int, Int]] = None, desc:Boolean = false, alpha:Boolean = false, by:Option[String] = None, get:List[String] = Nil)(implicit format:Format, parse:Parse[A]):Option[List[Option[A]]] = {
      r.hset("hash-1", "description", "one")
      r.hset("hash-1", "order", "100")
      r.hset("hash-2", "description", "two")
      r.hset("hash-2", "order", "25")
      r.hset("hash-3", "description", "three")
      r.hset("hash-3", "order", "50")
      r.sadd("alltest", 1)
      r.sadd("alltest", 2)
      r.sadd("alltest", 3)
      r.sort("alltest").getOrElse(Nil) should equal(List(Some("1"), Some("2"), Some("3")))
      r.sort("alltest", Some((0, 1))).getOrElse(Nil) should equal(List(Some("1")))
      r.sort("alltest", None, true).getOrElse(Nil) should equal(List(Some("3"), Some("2"), Some("1")))
      r.sort("alltest", None, false, false, Some("hash-*->order")).getOrElse(Nil) should equal(List(Some("2"), Some("3"), Some("1")))
      r.sort("alltest", None, false, false, None, List("hash-*->description")).getOrElse(Nil) should equal(List(Some("one"), Some("two"), Some("three")))
      r.sort("alltest", None, false, false, None, List("hash-*->description", "hash-*->order")).getOrElse(Nil) should equal(List(Some("one"), Some("100"), Some("two"), Some("25"), Some("three"), Some("50")))
    }
  }
  }

  protected def sortNStore(): Unit = {
  describe("sortNStore") {
    it("should give") {
      r.sadd("alltest", 10)
      r.sadd("alltest", 30)
      r.sadd("alltest", 3)
      r.sadd("alltest", 1)

      // default serialization : return String
      r.sortNStore("alltest", storeAt = "skey").getOrElse(-1) should equal(4)
      r.lrange("skey", 0, 10).get should equal(List(Some("1"), Some("3"), Some("10"), Some("30")))

      // Long serialization : return Long
      implicit val parseLong = Parse[Long](new String(_).toLong)
      r.sortNStore[Long]("alltest", storeAt = "skey").getOrElse(-1) should equal(4)
      r.lrange("skey", 0, 10).get should equal(List(Some(1), Some(3), Some(10), Some(30)))
    }
  }
  }

  protected def ping(): Unit = {
  describe("ping") {
    it("should return pong") {
      r.ping.get should equal("PONG")
    }
  }
  }

  protected def getConfig(): Unit = {
  describe("getConfig") {
    it("should return port") {
      r.getConfig("port").get should equal(Map("port" -> Some("6379")))
    }
  }
  }

  protected def setConfig(): Unit = {
  describe("setConfig") {
    it("should set config") {
      r.setConfig("loglevel", "debug").get should equal("OK")
    }
  }
  }
}
