package com.redis.ds

import com.redis.RedisCommand
import com.redis.common.IntSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers



class DequeSpec extends AnyFunSpec
                with Matchers
                with IntSpec {

  override protected lazy val r: MyRedisDeque[String] =
    new RedisDequeClient(redisContainerHost, redisContainerPort).getDeque("td")

  describe("addFirst and check size and added element") {
    it("should add to the head of the deque") {
      r.addFirst("foo") should equal(Some(1))
      r.peekFirst should equal(Some("foo"))
      r.addFirst("bar") should equal(Some(2))
      r.isEmpty should equal(false)
      r.peekFirst should equal(Some("bar"))
      r.clear should equal(true)
      r.size should equal(0)
      r.isEmpty should equal(true)
      r.addFirst("foo") should equal(Some(1))
    }
  }

  describe("addLast and check size and added element") {
    it("should add to the head of the deque") {
      r.addLast("foo") should equal(Some(1))
      r.peekFirst should equal(Some("foo"))
      r.addLast("bar") should equal(Some(2))
      r.peekFirst should equal(Some("foo"))
      r.size should equal(2)
      r.isEmpty should equal(false)
    }
  }

  describe("poll") {
    it("should pull out first element") {
      r.addFirst("foo") should equal(Some(1))
      r.addFirst("bar") should equal(Some(2))
      r.addFirst("baz") should equal(Some(3))
      r.poll should equal(Some("baz"))
      r.poll should equal(Some("bar"))
      r.poll should equal(Some("foo"))
    }
  }

  describe("pollLast") {
    it("should pull out last element") {
      r.addFirst("foo") should equal(Some(1))
      r.addFirst("bar") should equal(Some(2))
      r.addFirst("baz") should equal(Some(3))
      r.pollLast should equal(Some("foo"))
      r.pollLast should equal(Some("bar"))
      r.pollLast should equal(Some("baz"))
    }
  }
}
