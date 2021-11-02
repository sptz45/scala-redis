package com.redis

import com.redis.common.IntSpec
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PipelineSpec extends AnyFunSpec
                   with Matchers
                   with IntSpec
                   with Inside {

  override protected lazy val r: RedisClient =
    new RedisClient(redisContainerHost, redisContainerPort)

  describe("pipeline1") {
    it("should do pipelined commands") {
      r.pipeline { p =>
        p.set("key", "debasish")
        p.get("key")
        p.get("key1")
      }.get should equal(List(true, Some("debasish"), None))
    }
  }

  describe("pipeline1 with publish") {
    it("should do pipelined commands") {
      val res = r.pipeline { p =>
        p.set("key", "debasish")
        p.get("key")
        p.get("key1")
        p.publish("a", "debasish ghosh")
      }.get

      inside(res) {
        case List(true, Some("debasish"), None, Some(_)) => succeed
        case _ => fail()
      }
    }
  }

  describe("pipeline2") {
    it("should do pipelined commands") {
      r.pipeline { p =>
        p.lpush("country_list", "france")
        p.lpush("country_list", "italy")
        p.lpush("country_list", "germany")
        p.incrby("country_count", 3)
        p.lrange("country_list", 0, -1)
      }.get should equal (List(Some(1), Some(2), Some(3), Some(3), Some(List(Some("germany"), Some("italy"), Some("france")))))
    }
  }

  describe("pipeline3") {
    it("should handle errors properly in pipelined commands") {
      val thrown = the [Exception] thrownBy {
          r.pipeline { p =>
            p.set("a", "abc")
            p.lpop("a")
          }
        }
      thrown.getMessage should equal ("WRONGTYPE Operation against a key holding the wrong kind of value")
      r.get("a").get should equal("abc")
    }
  }

  describe("pipeline4") {
    it("should discard pipelined commands") {
      r.pipeline { p =>
        p.set("a", "abc")
        throw new RedisMultiExecException("want to discard")
      } should equal(None)
      r.get("a") should equal(None)
    }
  }

  it("should publish without breaking the other commands in the pipeline") {
    val res = r.pipeline { p =>
      p.set("key", "debasish")
      p.publish("a", "message")
      p.get("key")
      p.publish("a", "message2")
      p.get("key1")
    }.get

    inside(res) {
      case List(true, Some(_), Some("debasish"), Some(_), None) => succeed
      case _ => fail()
    }
  }

  import scala.concurrent.Await
  import scala.concurrent.duration._

  describe("pipeline no multi 1") {
    it("should execute 100 lpushes in pipeline") {

      val timeout = 2 minutes

      val vs = List.range(0, 100)
      import com.redis.serialization.Parse.Implicits.parseInt
      val x = r.pipelineNoMulti(vs.map(a => {() => r.lpush("list", a)}))

      x.foreach{a => Await.result(a.future, timeout)}
      r.lrange[Int]("list", 0, 100).get.map(_.get).reverse should equal(vs)
    }
  }

  describe("pipeline no multi 2") {
    it("should do pipelined commands with an exception") {

      val timeout = 2 minutes

      val x =
      r.pipelineNoMulti(
        List(
          {() => r.set("key", "debasish")},
          {() => r.get("key")},
          {() => r.get("key1")},
          {() => r.lpush("list", "maulindu")},
          {() => r.lpush("key", "maulindu")}     // should raise an exception
        )
      )

      val result = x.map{a => Await.result(a.future, timeout)}
      result.head should equal(true)
      result.last.isInstanceOf[Exception] should be (true)
    }
  }

  describe("pipeline hyperloglog") {
    it("should do pipelined commands") {
      val res =
      r.pipeline { p =>
        p.pfadd("kAdd", "v")
      }
      res.get should equal(List(Some(1)))
    }
  }

  describe("hincrbyfloat inside a pipeline") {
    it("should succeed with a correct response") {
      val float = 1.45.toFloat
      val res = r.pipeline { p =>
        p.hincrbyfloat("hincrbyfloat", "key", float)
      }
      res.get should equal(List(Some(float)))
    }
  }

  describe("incrbyfloat inside a pipeline") {
    it("should succeed with a correct response") {
      val float = 1.45.toFloat
      val res = r.pipeline { p =>
        p.incrbyfloat("key_incr", float)
      }
      res.get should equal(List(Some(float)))
    }
  }

  describe("pipeline with batch submission 1") {
    it("should execute all commands in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val res = client.batchedPipeline(
        List(
          () => client.lpush("key1", "va1", "va2"),
          () => client.rpush("key2", "va3", "va4"),
          () => client.set("key3", "va5"),
          () => client.incr("key4")
        )
      )
      res.get.size should equal(4)
    }
  }

  describe("pipeline with batch submission with custom serialization - 1") {
    it("should execute all commands in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      import com.redis.serialization._
      import Parse.Implicits.parseInt
      implicit val parseString = Parse[String](new String(_).toInt.toBinaryString)
      val res = client.batchedPipeline(
        List(
          () => client.hmset("hash", Map("field1" -> "1", "field2" -> 2)),
          () => client.hmget[String,Int]("hash", "field1", "field2"),
          () => client.hmget[String,String]("hash", "field1", "field2")
        )
      )
      println(res)
      val l = res.get
      l(1) match {
        case m: Option[Map[String, Any]] => m.get.get("field2").get should equal(2)
        case _ => ???
      }
      l(2) match {
        case m: Option[Map[String, Any]] => m.get.get("field2").get should equal("10")
        case _ => ???
      }
      res.get.size should equal(3)
    }
  }

  describe("pipeline with batch submission with custom serialization - 2") {
    it("should execute all commands in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      case class Upper(s: String)
      import com.redis.serialization.Format
      val formatUpper = Format{case Upper(s) => s.toUpperCase}
      val res = client.batchedPipeline(
        List(
          () => client.hmset("hash1", Map("field1" -> Upper("val1"), "field2" -> Upper("val2"))),
          () => client.hmset("hash2", Map("field1" -> Upper("val1"), "field2" -> Upper("val2")))(formatUpper),
          () => client.hmget("hash1", "field1", "field2"), 
          () => client.hmget("hash2", "field1", "field2") 
        )
      )
      println(res)
      res.get.size should equal(4)
    }
  }

  describe("pipeline with batch submission with custom serialization - 3") {
    it("should execute all commands in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      case class Upper(s: String)
      import com.redis.serialization.Format
      val formatUpper = Format{case Upper(s) => s.toUpperCase}
      val res = client.batchedPipeline(
        List(
          () => client.hmset("hash1", Map("field1" -> Upper("va'l1"), "field2" -> Upper("val2"))),
          () => client.hmset("hash2", Map("field1" -> Upper(s"""val"1"""), "field2" -> Upper("val2")))(formatUpper),
          () => client.hmget("hash1", "field1", "field2"), 
          () => client.hmget("hash2", "field1", "field2") 
        )
      )
      println(res)
      res.get.size should equal(4)
    }
  }

  describe("pipelined with batch submission list operations") {
    it("can pipeline zadd, followed by an expire, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val val1 = (0d, "value1")
      val val2 = (0d, "value2")
      val val3 = (0d, "value3")

      val res = client.batchedPipeline(
        List(
          () => client.zadd("key1", 0d, "value0", val1, val2, val3),
          () => client.expire("key1", 30)
        )
      )
      println(res)
      res.get.size should equal(2)
    }

    it("should handle values consisting of Kryo serialized string data") {
      // the below are Scala strings serialized with a Kryo, a popular serialization library
      val data1 = Array(3, 1, 45, 55, 52, 50, 51, 52, 57, 50, 57, 53, 58, 45, 49, 48, 56, 51, 48, 53, 53, 54, 52, -71)
      val data2 = Array(3, 1, 54, 49, 56, 48, 53, 99, 49, 102, 48, 99, 56, 55, 56, 100, 49, 49, 50, 49, 101, 57, 49, 48, 99, 101, 58, 80, 73, -44)

      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)

      val res = client.batchedPipeline(
        List(
          () => client.lpush("key1", data1, data2)
        )
      )
      println(res)
      res.get.size should equal(1)
    }

    it("can pipeline zadd when the keys contain spaces, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val val1 = (0d, "value 1")
      val val2 = (0d, "value 2")
      val val3 = (0d, "valu2 3")

      val res = client.batchedPipeline(
        List(
          () => client.zadd("key1", 0d, "value0", val1, val2, val3)
        )
      )
      println(res)
      res.get.size should equal(1)
    }

    it("can pipeline zadd when the keys contain spaces, followed by an expire, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val val1 = (0d, "value 1")
      val val2 = (0d, "value 2")
      val val3 = (0d, "valu2 3")

      val res = client.batchedPipeline(
        List(
          () => client.zadd("key1", 0d, "value0", val1, val2, val3),
          () => client.expire("key1", 30)
        )
      )
      println(res)
      res.get.size should equal(2)
    }

    it("can insert values in batch with zadd, and then later fetch them with zrange as they were inserted") {
      val batchClient = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val val1 = (0d, "bazzbazz")
      val val2 = (0d, "bazzbar")

      val res = batchClient.batchedPipeline(
        List(
          () => batchClient.zadd("key1", 0d, "foobar", val1, val2)
        )
      )

      res.get.size should equal(1)

      val simpleClient = new RedisClient(redisContainerHost, redisContainerPort)
      val values = simpleClient.zrange("key1", 0, -1).getOrElse(List.empty[String])

      values should contain theSameElementsAs List("bazzbar","bazzbazz","foobar")
    }
  }

  describe("pipeline with batch submission for TTL (expire) commands") {
    it("should accept setting simple key/value followed by an expiration, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val res = client.batchedPipeline(
        List(
          () => client.set("key1", "val1"),
          () => client.expire("key1", 1000),
          () => client.set("key2", "val2"),
          () => client.expire("key2", 1000)
        )
      )
      println(res)
      res.get.size should equal(4)
    }

    it("should accept pushing items on to a list followed by an expiration, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val res = client.batchedPipeline(
        List(
          () => client.lpush("key1", "val1" :: "val2" :: "val3" :: Nil),
          () => client.lpush("key1", "val4" :: "val5" :: "val6" :: Nil),
          () => client.expire("key1", 1000)
        )
      )
      println(res)
      res.get.size should equal(3)
    }

    it("should accept pushing binary data onto a list followed by an expiration, in batch") {
      val client = new RedisClient(redisContainerHost, redisContainerPort, batch = RedisClient.BATCH)
      val data: List[Array[Byte]] = List("foo","bar","bazz").map(_.getBytes("ASCII")) // purposely not UTF-8
      val res = client.batchedPipeline(
        List(
          () => client.lpush("key1", data),
          () => client.expire("key1", 1000)
        )
      )
      println(res)
      res.get.size should equal(2)
    }
  }
}
