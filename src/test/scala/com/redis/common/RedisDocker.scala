package com.redis.common

import java.io.File
import javax.net.ssl.SSLContext

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

import com.redis.RedisClient
import com.spotify.docker.client.messages.HostConfig.Bind
import com.spotify.docker.client.messages.PortBinding
import com.whisk.docker.testkit.{BaseContainer, Container, ContainerCommandExecutor, ContainerGroup, ContainerSpec, DockerReadyChecker, SingleContainer}
import com.whisk.docker.testkit.scalatest.DockerTestKitForAll
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.Suite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Milliseconds, Seconds, Span}


trait RedisDockerCluster extends RedisContainer {
  that: Suite =>

  protected def redisContainerPort(container: BaseContainer): Int = container.mappedPort(redisPort)

  protected def make4Containers: List[Container] = (0 until 4)
    .map(_ => new Container(createContainer()))
    .toList

  override val managedContainers: ContainerGroup = ContainerGroup(make4Containers)
}

trait RedisDocker extends RedisContainer {
  that: Suite =>

  implicit lazy val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  protected def redisContainerPort: Int = managedContainers.container.mappedPort(redisPort)

  override val managedContainers: SingleContainer = SingleContainer(new Container(createContainer()))
}

trait RedisDockerSSL extends RedisDocker {
  that: Suite =>

  private lazy val certsPath = new File("src/test/resources/certs").getAbsolutePath

  override protected def baseContainer(name: Option[String]): ContainerSpec =
    name.foldLeft(ContainerSpec("madflojo/redis-tls:latest")
      .withVolumeBindings(Bind.from(certsPath).to("/certs").build()))(_ withName _)
}

trait RedisContainer extends DockerTestKitForAll with ScalaFutures {
  that: Suite =>

  def sslContext: Option[SSLContext] = None

  implicit val pc: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Milliseconds))

  protected val redisContainerHost: String = "localhost"
  protected val redisPort: Int = 6379

  protected def baseContainer(name: Option[String]): ContainerSpec =
    name.foldLeft(ContainerSpec("redis:latest"))(_ withName _)

  private def ping: DockerReadyChecker = new DockerReadyChecker {
    override def apply(container: BaseContainer)(implicit docker: ContainerCommandExecutor, ec: ExecutionContext): Future[Unit] = {
      Future.traverse(container.mappedPorts().values) { p =>
        Future{
          val cli = new RedisClient(redisContainerHost, p, sslContext = sslContext)
          try cli.ping finally cli.close()
        }
      }.map(_ => ())
    }
  }
  protected def createContainer(name: Option[String] = Some(RandomStringUtils.randomAlphabetic(10)),
                                ports: Map[Int, Int] = Map.empty): ContainerSpec = {
    val containerPorts: Seq[(Int, PortBinding)] = if (ports.isEmpty) {
      Seq((redisPort -> PortBinding.randomPort("0.0.0.0")))
    } else {
      ports.mapValues(i => PortBinding.of(redisContainerHost, i)).toSeq
    }

    baseContainer(name).withPortBindings(containerPorts: _*)
      .withReadyChecker(DockerReadyChecker.And(
        DockerReadyChecker.LogLineContains("Ready to accept connections"),
        ping.looped(1000, 10.milli)))
  }
}
