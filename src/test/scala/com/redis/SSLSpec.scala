package com.redis

import java.security.cert.X509Certificate
import org.apache.http.ssl.{SSLContexts, TrustStrategy}

import com.redis.common.RedisDockerSSL
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SSLSpec extends AnyFunSpec with Matchers with RedisDockerSSL {

  // Our certificate on the test server is self-signed, which will be
  // rejected by the default SSLContext. This SSLContext is therefore
  // specifically configured to trust all certificates.
  private val sslContext = SSLContexts
    .custom()
    .loadTrustMaterial(null, new TrustStrategy() {
      def isTrusted(arg0: Array[X509Certificate], arg1: String) = true
    })
    .build()

  describe("ssl connections") {
    it("should be established for a RedisClient with a valid SSLContext") {
      val secureClient: RedisClient = new RedisClient(
        redisContainerHost,
        redisContainerPort,
        sslContext = Some(sslContext)
      )
      secureClient.ping shouldEqual Some("PONG")
      secureClient.close()
    }

    it("should be established for a RedisClientPool with a valid SSLContext") {
      val clients = new RedisClientPool(
        redisContainerHost,
        redisContainerPort,
        sslContext = Some(sslContext)
      )
      clients.withClient(_.ping) shouldEqual Some("PONG")
      clients.withClient { client =>
        client.disconnect
      }
      clients.close()
    }

    it("should be rejected for a RedisClient with no SSLContext") {
      val insecureClient =
        new RedisClient(redisContainerHost, redisContainerPort) {
          // this disables retries
          override def disconnect = false
        }

      assertThrows[RedisConnectionException] {
        insecureClient.ping
      }

      insecureClient.close()
    }
  }
}
