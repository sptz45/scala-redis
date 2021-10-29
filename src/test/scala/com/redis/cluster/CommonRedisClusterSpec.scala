package com.redis.cluster

import com.redis.RedisClient
import com.redis.api._
import com.redis.common.IntClusterSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

// todo: remove, test every API separately
@deprecated trait CommonRedisClusterSpec extends AnyFunSpec with Matchers with IntClusterSpec {

  override lazy val r = rProvider()

  def rProvider(): AutoCloseable with RedisClusterOps with WithHashRing[IdentifiableRedisClientPool]
    with BaseApi with HashApi with ListApi with NodeApi with SetApi with SortedSetApi with StringApi
    with EvalApi

  describe("cluster operations") {
    shouldSet()
    shouldGetKeysFromProperNodes()
    shouldDoAllOpsOnTheCluster()
    shouldMGet()
    shouldListOps()
    shouldKeyTags()
    shouldReplaceNode()
    shouldRemoveNode()
    shouldListNodes()
  }

  def shouldSet(): Unit = {
    it("should set") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // last 3 should map to the same node
      l.map(r.nodeForKey(_)).reverse.slice(0, 3).forall(_.toString == "node2") should equal(true)

      // set
      l foreach (s => r.processForKey(s)(_.set(s, "working in anshin")) should equal(true))

      // check get: should return all 5
      r.keys("*").get.size should equal(5)
    }
  }

  def shouldGetKeysFromProperNodes(): Unit = {
    it("should get keys from proper nodes") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l foreach (s => r.processForKey(s)(_.set(s, s + " is working in anshin")) should equal(true))

      r.get("debasish").get should equal("debasish is working in anshin")
      r.get("maulindu").get should equal("maulindu is working in anshin")
      l.map(r.get(_).get).distinct.size should equal(5)
    }
  }

  def shouldDoAllOpsOnTheCluster(): Unit = {
    it("should do all operations on the cluster") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l foreach (s => r.processForKey(s)(_.set(s, s + " is working in anshin")) should equal(true))

      r.dbsize.get should equal(5)
      r.exists("debasish") should equal(true)
      r.exists("maulindu") should equal(true)
      r.exists("debasish-1") should equal(false)

      r.del("debasish", "nilanjan").get should equal(2)
      r.dbsize.get should equal(3)
      r.del("satire").get should equal(0)
    }
  }

  def shouldMGet(): Unit = {
    it("mget on a cluster should fetch values in the same order as the keys") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l foreach (s => r.processForKey(s)(_.set(s, s + " is working in anshin")) should equal(true))

      // mget
      r.mget(l.head, l.tail: _*).get.map(_.get.split(" ")(0)) should equal(l)
    }
  }

  def shouldListOps(): Unit = {
    it("list operations should work on the cluster") {
      r.lpush("java-virtual-machine-langs", "java") should equal(Some(1))
      r.lpush("java-virtual-machine-langs", "jruby") should equal(Some(2))
      r.lpush("java-virtual-machine-langs", "groovy") should equal(Some(3))
      r.lpush("java-virtual-machine-langs", "scala") should equal(Some(4))
      r.llen("java-virtual-machine-langs") should equal(Some(4))
    }
  }

  def shouldKeyTags(): Unit = {
    it("keytags should ensure mapping to the same server") {
      r.lpush("java-virtual-machine-{langs}", "java") should equal(Some(1))
      r.lpush("java-virtual-machine-{langs}", "jruby") should equal(Some(2))
      r.lpush("java-virtual-machine-{langs}", "groovy") should equal(Some(3))
      r.lpush("java-virtual-machine-{langs}", "scala") should equal(Some(4))
      r.llen("java-virtual-machine-{langs}") should equal(Some(4))
      r.lpush("microsoft-platform-{langs}", "c++") should equal(Some(1))
      r.rpoplpush("java-virtual-machine-{langs}", "microsoft-platform-{langs}").get should equal("java")
      r.llen("java-virtual-machine-{langs}") should equal(Some(3))
      r.llen("microsoft-platform-{langs}") should equal(Some(2))
    }
  }

  def shouldReplaceNode(): Unit = {
    it("replace node should not change hash ring order") {
      val r = rProvider()
      r.set("testkey1", "testvalue2")
      r.get("testkey1") should equal(Some("testvalue2"))

      val nodename = r.hr.getNode(formattedKey("testkey1").toIndexedSeq).toString

      //simulate the same value is duplicated to slave
      //for test, don't set to master, just to make sure the expected value is loaded from slave
      val redisClient = new RedisClient(redisContainerHost, redisContainerPort(dockerContainers.head))
      redisClient.set("testkey1", "testvalue1")

      //replaced master with slave on the same node
      r.replaceServer(ClusterNode(nodename, redisContainerHost, redisContainerPort(dockerContainers.head)))
      r.nodeForKey("testkey1").port should equal(redisContainerPort(dockerContainers.head))

      r.hr.cluster.find(_.node.nodename.equals(nodename)).get.port should equal(redisContainerPort(dockerContainers.head))
      r.get("testkey1") should equal(Some("testvalue1"))

      //switch back to master. the old value is loaded
      val oldnode = nodes.filter(_.nodename.equals(nodename))(0)
      r.replaceServer(oldnode)
      r.get("testkey1") should equal(Some("testvalue2"))
      r.close()
    }
  }

  def shouldRemoveNode(): Unit = {
    it("remove failure node should change hash ring order so that key on failure node should be served by other running nodes") {
      val r = rProvider()
      r.set("testkey1", "testvalue2")
      r.get("testkey1") should equal(Some("testvalue2"))

      val nodename = r.hr.getNode(formattedKey("testkey1").toIndexedSeq).toString

      //replaced master with slave on the same node
      r.removeServer(nodename)
      r.get("testkey1") should equal(None)

      r.set("testkey1", "testvalue2")
      r.get("testkey1") should equal(Some("testvalue2"))
      r.close()
    }
  }

  def shouldListNodes(): Unit = {
    it("list nodes should return the running nodes but not configured nodes") {
      val r = rProvider()
      r.listServers.toSet should equal(nodes.toSet)
      r.removeServer("node1")
      r.listServers.toSet should equal(nodes.filterNot(_.nodename.equals("node1")).toSet)
      r.close()
    }
  }
}
