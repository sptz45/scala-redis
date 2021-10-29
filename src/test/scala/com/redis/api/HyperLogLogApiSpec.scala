package com.redis.api

import com.redis.common.IntSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


trait HyperLogLogApiSpec extends AnyFunSpec
                         with Matchers
                         with IntSpec {

  override protected def r: BaseApi with StringApi with HyperLogLogApi with AutoCloseable

  pfadd()
  pfcount()
  pfmerge()

  protected def pfadd(): Unit = {
  describe("pfadd") {
    it("should return one for changed estimated cardinality") {
      r.pfadd("hll-updated-cardinality", "value1") should equal(Some(1))
    }

    it("should return zero for unchanged estimated cardinality") {
      r.pfadd("hll-nonupdated-cardinality", "value1")
      r.pfadd("hll-nonupdated-cardinality", "value1") should equal(Some(0))
    }

    it("should return one for variadic values and estimated cardinality changes") {
      r.pfadd("hll-variadic-cardinality", "value1")
      r.pfadd("hll-variadic-cardinality", "value1", "value2") should equal(Some(1))
    }
  }
  }

  protected def pfcount(): Unit = {
  describe("pfcount") {
    it("should return zero for an empty") {
      r.pfcount("hll-empty") should equal(Some(0))
    }

    it("should return estimated cardinality") {
      r.pfadd("hll-card", "value1") should equal(Some(1))
      r.pfcount("hll-card") should equal(Some(1))
    }

    it("should return estimated cardinality of unioned keys") {
      r.pfadd("hll-union-1", "value1")
      r.pfadd("hll-union-2", "value2")
      r.pfcount("hll-union-1", "hll-union-2") should equal(Some(2))
    }
  }
  }

  protected def pfmerge(): Unit = {
  describe("pfmerge") {
    it("should merge existing entries") {
      r.pfadd("hll-merge-source-1", "value1")
      r.pfadd("hll-merge-source-2", "value2")
      r.pfmerge("hell-merge-destination", "hll-merge-source-1", "hll-merge-source-2")
      r.pfcount("hell-merge-destination") should equal(Some(2))
    }
  }
  }
}
