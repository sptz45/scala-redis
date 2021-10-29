package com.redis.api

import com.redis.common.IntSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


trait HashApiSpec extends AnyFunSpec
                     with Matchers
                     with IntSpec {

  override protected def r: BaseApi with StringApi with HashApi with AutoCloseable

  hset()
  hset1()
  hgetall1()

  protected def hset(): Unit = {
  describe("hset") {
    it("should set and get fields") {
      r.hset("hash1", "field1", "val")
      r.hget("hash1", "field1") should be(Some("val"))
    }

    it("should return true if field did not exist and was inserted") {
      r.hdel("hash1", "field1")
      r.hset("hash1", "field1", "val") should be(true)
    }

    it("should return false if field existed before and was overwritten") {
      r.hset("hash1", "field1", "val")
      r.hset("hash1", "field1", "val") should be(false)
    }

    it("should set and get maps") {
      r.hmset("hash2", Map("field1" -> "val1", "field2" -> "val2"))
      r.hmget("hash2", "field1") should be(Some(Map("field1" -> "val1")))
      r.hmget("hash2", "field1", "field2") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
      r.hmget("hash2", "field1", "field2", "field3") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
    }

    it("should increment map values") {
      r.hincrby("hash3", "field1", 1)
      r.hget("hash3", "field1") should be(Some("1"))
    }

    it("should check existence") {
      r.hset("hash4", "field1", "val")
      r.hexists("hash4", "field1") should equal(true)
      r.hexists("hash4", "field2") should equal(false)
    }

    it("should delete fields") {
      r.hset("hash5", "field1", "val")
      r.hexists("hash5", "field1") should equal(true)
      r.hdel("hash5", "field1") should equal(Some(1))
      r.hexists("hash5", "field1") should equal(false)
      r.hmset("hash5", Map("field1" -> "val1", "field2" -> "val2"))
      r.hdel("hash5", "field1", "field2") should equal(Some(2))
    }

    it("should return the length of the fields") {
      r.hmset("hash6", Map("field1" -> "val1", "field2" -> "val2"))
      r.hlen("hash6") should be(Some(2))
    }

    it("should return the aggregates") {
      r.hmset("hash7", Map("field1" -> "val1", "field2" -> "val2"))
      r.hkeys("hash7") should be(Some(List("field1", "field2")))
      r.hvals("hash7") should be(Some(List("val1", "val2")))
      r.hgetall1("hash7") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
    }

    it("should increment map values by floats") {
      r.hset("hash1", "field1", 10.50f)
      r.hincrbyfloat("hash1", "field1", 0.1f) should be(Some(10.6f))
      r.hset("hash1", "field1", 5.0e3f)
      r.hincrbyfloat("hash1", "field1", 2.0e2f) should be(Some(5200f))
      r.hset("hash1", "field1", "abc")
      val thrown = the [Exception] thrownBy { r.hincrbyfloat("hash1", "field1", 2.0e2f) }
      thrown.getMessage should include("hash value is not a float")
    }

    it("should delete multiple keys if present on a hash") {
      r.hset("hash100", "key1", 10.20f)
      r.hset("hash100", "key2", 10.30f)
      r.hset("hash100", "key3", 10.40f)
      r.hset("hash100", "key4", 10.50f)
      r.hset("hash100", "key5", 10.60f)
      r.hkeys("hash100") should be(Some(List("key1", "key2", "key3", "key4", "key5")))
      r.hdel("hash100", "key1", "key2", "key3", "key4", "key5") should equal(Some(5))
      r.hkeys("hash100") should be(Some(List()))
    }
  }
  }

  protected def hset1(): Unit = {
  describe("hset1") {
    it("should set field") {
      r.hset1("hash1", "field1", "val")
      r.hget("hash1", "field1") should be(Some("val"))
    }

    it("should return Some(1L) if field did not exist and was inserted") {
      r.hdel("hash1", "field1")
      r.hset1("hash1", "field1", "val") should be(Some(1L))
    }

    it("should return Some(0L) if field existed before and was overwritten") {
      r.hset1("hash1", "field1", "val")
      r.hset1("hash1", "field1", "val") should be(Some(0L))
    }
  }
  }

  protected def hgetall1(): Unit = {
  describe("hgetall1") {
    it("should behave symmetrically with hmset") {
      r.hmset("hash1", Map("field1" -> "val1", "field2" -> "val2"))
      val thrown = the [Exception] thrownBy { r.hmset("hash2", Map()) }
      r.hget("hash1", "field1") should be(Some("val1"))
      r.hgetall1("hash1") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
      r.hget("hash1", "foo") should be(None)
      r.hgetall1("hash12") should be(None)
    }
  }
  }

}
