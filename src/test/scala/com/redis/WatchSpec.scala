package com.redis

import com.redis.common.RedisDocker
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class WatchSpec extends AnyFunSpec
                     with Matchers
                     with RedisDocker {

  describe("watch") {
    it("should fail a transaction if modified from another client") {
      val clients = new RedisClientPool(redisContainerHost, redisContainerPort)
      val p1: Future[Option[List[Any]]] = Future {
        clients.withClient { client =>
          client.watch("key")
          client.pipeline { p =>
            p.set("key", "debasish")
            Thread.sleep(500)
            p.get("key")
            p.get("key1")
          }
      }
      }

      val p2: Future[Boolean] = Future {
        clients.withClient { client =>
          Thread.sleep(50)
          client.set("key", "anshin")
        }
      }

      p2.futureValue should equal(true)
      p1.futureValue should equal(None)
      clients.close()
    }
  }
}

