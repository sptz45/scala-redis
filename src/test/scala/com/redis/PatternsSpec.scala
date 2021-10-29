package com.redis

import com.redis.Patterns._
import com.redis.common.RedisDocker
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PatternsSpec extends AnyFunSpec
               with Matchers
               with BeforeAndAfterEach
               with RedisDocker {

  implicit lazy val clients = new RedisClientPool(redisContainerHost, redisContainerPort)

  override def afterEach() = clients.withClient{
    client => client.flushdb
  }

  override def afterAll() = {
    clients.withClient{ client =>
      client.flushall
      client.disconnect
    }
    clients.close()
  }

  def runScatterGather(opsPerRun: Int) = {
    val start = System.nanoTime
    val sum = scatterGatherWithList(opsPerRun)
    assert(sum == (1L to opsPerRun).sum * 100L)
    val elapsed: Double = (System.nanoTime - start) / 1000000000.0
    val opsPerSec: Double = (100 * opsPerRun * 2) / elapsed
    println("Operations per run: " + opsPerRun * 100 * 2 + " elapsed: " + elapsed + " ops per second: " + opsPerSec)
  }

  def runScatterGatherFirst(opsPerRun: Int) = {
    val start = System.nanoTime
    val sum = scatterGatherFirstWithList(opsPerRun)
    assert(sum == (1 to opsPerRun).sum)
    val elapsed: Double = (System.nanoTime - start) / 1000000000.0
    val opsPerSec: Double = (101 * opsPerRun) / elapsed
    println("Operations per run: " + opsPerRun * 101 + " elapsed: " + elapsed + " ops per second: " + opsPerSec)
  }

  private val amountMultiplier = 1 // unit test multiplier
  // private val amountMultiplier = 1000 // benchmark multiplier

  describe("scatter/gather with list test 1") {
    it("should distribute work amongst the clients") {
      runScatterGather(2 * amountMultiplier)
    }
  }

  describe("scatter/gather with list test 2") {
    it("should distribute work amongst the clients") {
      runScatterGather(5 * amountMultiplier)
    }
  }

  describe("scatter/gather with list test 3") {
    it("should distribute work amongst the clients") {
      runScatterGather(10 * amountMultiplier)
    }
  }

  describe("scatter/gather first with list test 1") {
    it("should distribute work amongst the clients") {
      runScatterGatherFirst(5 * amountMultiplier)
    }
  }
}
