package com.redis.cluster

import com.redis.cluster.KeyTag.NoOpKeyTag
import com.redis.common.IntClusterSpec
import com.whisk.docker.testkit.{BaseContainer, Container}
import org.scalatest.{GivenWhenThen, Suite}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class ReconnectableSpec extends AnyFunSpec with GivenWhenThen
  with ControlledDockerRedisCluster {

  override protected lazy val r: RedisCluster with Reconnectable = new RedisCluster(nodes, Some(NoOpKeyTag)) with Reconnectable {
    override protected lazy val checkIntervalSeconds: Int = 1
  }

  describe("reconnectable cluster") {
    it("should properly disconnect and reconnect node") {
      Given("Initial cluster")
      When("Evertything is running")

      val nodeName0 = s"${nodeNamePrefix}0"
      val initialKeyNodeMap = Map(
        "abc4" -> nodeName0,
        "abc8" -> s"${nodeNamePrefix}1",
        "abc7" -> s"${nodeNamePrefix}2",
        "abc0" -> s"${nodeNamePrefix}3"
      )

      Then("The keys are mapped to all nodes")
      initialKeyNodeMap.foreach { case (k, v) =>
        val node = r.nodeForKey(k)
        node.node.nodename should be(v)

        r.set(k, v) should be(true)
        r.get(k).get should be(v)
        r.del(k).get should be(1)
      }

      When("One node is stopped")
      stopContainer0()

      Then("The cluster should re-balance with one node less")
      waitForCluster(!_.exists(_.nodename == nodeName0)).futureValue should be(true)
      initialKeyNodeMap.foreach { case (k, v) =>
        val node = r.nodeForKey(k)
        node.node.nodename should not be (container0Name)

        r.set(k, v) should be(true)
        r.get(k).get should be(v)
        r.del(k).get should be(1)
      }

      When("The node comes back up")
      startContainer0()

      Then("The cluster should re-balance with new node")
      waitForCluster(_.exists(_.nodename == nodeName0)).futureValue should be(true)
      initialKeyNodeMap.foreach { case (k, v) =>
        val node = r.nodeForKey(k)
        node.node.nodename should be(v)

        r.set(k, v) should be(true)
        r.get(k).get should be(v)
        r.del(k).get should be(1)
      }
    }
  }

  def waitForCluster(expected: List[ClusterNode] => Boolean, remaining: Int = 10,
                     p: Promise[Boolean] = Promise[Boolean]()): Future[Boolean] =
    if (expected(r.listServers)) {
      p.success(true).future
    } else if (remaining > 0) {
      Thread.sleep(1000)
      waitForCluster(expected, remaining - 1, p)
    } else {
      p.failure(new Throwable("Did not reach expected state")).future
    }

  override def beforeStop(): Unit = {
    Try(stopContainer0())
    r.close()
  }
}

trait ControlledDockerRedisCluster extends IntClusterSpec with Matchers {
  that: Suite =>

  implicit lazy val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val logger = LoggerFactory.getLogger(getClass)

  protected lazy val container0: BaseContainer = managedContainers.containers.head
  protected lazy val container0Name: String = container0.spec.name.orNull
  protected lazy val container0Ports: Map[Int, Int] = container0.mappedPorts()
  protected lazy val newContainer0: Container = new Container(createContainer(Some(container0Name), container0Ports))

  protected lazy val containerNames: Seq[String] = managedContainers.containers.flatMap(_.spec.name)

  protected def startContainer0(): Unit = {
    logger.info(s"Manually starting node [$container0Name], [$container0Ports]")
    val new0Id = dockerExecutor.createContainer(newContainer0.spec).futureValue
    dockerExecutor.startContainer(new0Id.id()).futureValue
  }

  protected def stopContainer0(): Unit = {
    logger.info(s"Manually removing node [$container0Name], [$container0Ports]")
    dockerExecutor.remove(container0Name, force = true, removeVolumes = true).futureValue
  }

}
