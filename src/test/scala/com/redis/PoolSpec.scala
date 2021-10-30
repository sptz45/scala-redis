package com.redis

import com.redis.common.RedisDocker
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent._
import scala.concurrent.duration._

class PoolSpec extends AnyFunSpec
               with Matchers
               with BeforeAndAfterEach
               with RedisDocker {

  implicit lazy val clients: RedisClientPool = new RedisClientPool(redisContainerHost, redisContainerPort)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clients.port should be > 0 // initialize lazy val hack :(
  }

  override def afterEach(): Unit = {
    clients.withClient{
      client => client.flushdb
    }
    super.afterEach()
  }

  override def afterAll(): Unit = {
    clients.withClient{ client => client.disconnect }
    clients.close()
    super.afterAll()
  }

  def lp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.lpush("list-l", _))
        client.llen("list-l")
      }
    }
  }

  def rp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.rpush("list-r", _))
        client.llen("list-r")
      }
    }
  }

  def set(msgs: List[String]) = {
    clients.withClient {
      client => {
        var i = 0
        msgs.foreach { v =>
          client.set("key-%d".format(i), v)
          i += 1
        }
        Some(1000L)
      }
    }
  }

  describe("pool test") {
    it("should distribute work amongst the clients") {
      val l = (0 until 5000).map(_.toString).toList
      val fns = List[List[String] => Option[Long]](lp, rp, set)
      val tasks = fns map (fn => Future { fn(l) })
      val results = Await.result(Future.sequence(tasks), 60 seconds)
      results should equal(List(Some(5000), Some(5000), Some(1000)))
    }
  }

  import Bench._

  private val amountMultiplier = 1 // unit test multiplier
  // private val amountMultiplier = 1000 // benchmark multiplier

  describe("list load test 1") {
    it(s"should distribute work amongst the clients for ${400 * amountMultiplier} list operations") {
      val (s, o, r) = listLoad(2 * amountMultiplier)
      println(s"${400 * amountMultiplier} list operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }

  describe("list load test 2") {
    it(s"should distribute work amongst the clients for ${1000 * amountMultiplier} list operations") {
      val (s, o, r) = listLoad(5 * amountMultiplier)
      println(s"${1000 * amountMultiplier} list operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }

  describe("list load test 3") {
    it(s"should distribute work amongst the clients for ${2000 * amountMultiplier} list operations") {
      val (s, o, r) = listLoad(10 * amountMultiplier)
      println(s"${2000 * amountMultiplier} list operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }

  describe("incr load test 1") {
    it(s"should distribute work amongst the clients for ${400 * amountMultiplier} incr operations") {
      val (s, o, r) = incrLoad(2 * amountMultiplier)
      println(s"${400 * amountMultiplier} incr operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }

  describe("incr load test 2") {
    it(s"should distribute work amongst the clients for ${1000 * amountMultiplier} incr operations") {
      val (s, o, r) = incrLoad(5 * amountMultiplier)
      println(s"${1000 * amountMultiplier} incr operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }

  describe("incr load test 3") {
    it(s"should distribute work amongst the clients for ${2000 * amountMultiplier} incr operations") {
      val (s, o, r) = incrLoad(10 * amountMultiplier)
      println(s"${2000 * amountMultiplier} incr operations: elapsed = " + s + " per sec = " + o)
      r.size should equal(100)
    }
  }
}
