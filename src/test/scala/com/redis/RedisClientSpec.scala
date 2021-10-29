package com.redis

import java.net.{ServerSocket, URI}

import com.github.dockerjava.core.DefaultDockerClientConfig
import com.redis.api.ApiSpec
import com.whisk.docker.DockerContainerManager
import com.whisk.docker.impl.dockerjava.Docker
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class RedisClientSpec extends AnyFunSpec
  with Matchers with ApiSpec {

  override protected lazy val r: RedisClient =
    new RedisClient(redisContainerHost, redisContainerPort)

  private lazy val redisUrl = s"$redisContainerHost:$redisContainerPort"

  describe("constructor") {
    it("should parse the db-number from the path of connection uri") {
      val client = new RedisClient(new URI(s"redis://$redisUrl/4"))
      client.database shouldBe 4
      client.close()
    }

    it("should default to db 0 for connection uri without db-number") {
      val client = new RedisClient(new URI(s"redis://$redisUrl"))
      client.database shouldBe 0
      client.close()
    }
  }

  describe("toString") {
    it("should include the db-number") {
      val c = new RedisClient(redisContainerHost, redisContainerPort, 1)
      c.toString shouldBe s"$redisUrl/1"
      c.close()
    }
  }

  describe("test subscribe") {
    it("should subscribe") {
    val r = new RedisClient(redisContainerHost, redisContainerPort)
  
    println(r.get("vvl:qm"))
  
    r.subscribe("vvl.qm") { m =>
      println(m)
    }
  
    Thread.sleep(3000)
  
    r.unsubscribe("vvl.qm")
  
    Thread.sleep(3000)
  
    println(r.get("vvl:qm"))
  
    r.subscribe("vvl.qm") { m =>
      println(m)
    }
  
    Thread.sleep(3000)
  
    r.unsubscribe("vvl.qm")
  
    Thread.sleep(3000)
  
    r.get("vvl:qm")
    r.close()
  }}

//   describe("test reconnect") {
//     it("should re-init after server restart") {
//       val docker = new Docker(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).client
// 
//       val port = {
//         val s = new ServerSocket(0)
//         val p = s.getLocalPort
//         s.close()
//         p
//       }
// 
//       val manager = new DockerContainerManager(
//         createContainer(ports = Map(redisPort -> port)) :: Nil, dockerFactory.createExecutor()
//       )
// 
//       val key = "test-1"
//       val value = "test-value-1"
// 
//       val (cs, _) :: _ = Await.result(manager.initReadyAll(30.seconds), 21.second)
//       val id = Await.result(cs.id, 20.seconds)
// 
//       val c = new RedisClient(redisContainerHost, port, 8, timeout = 20.seconds.toMillis.toInt)
//       c.set(key, value)
//       docker.stopContainerCmd(id).exec()
//       try {c.get(key)} catch { case e: Throwable => }
//       docker.startContainerCmd(id).exec()
//       val got = c.get(key)
//       c.close()
//       docker.removeContainerCmd(id).withForce(true).withRemoveVolumes(true).exec()
//       docker.close()
// 
//       got shouldBe Some(value)
//     }
//   }
}
