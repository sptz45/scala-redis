package com.redis

import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext

import org.apache.commons.pool2._
import org.apache.commons.pool2.impl._

private [redis] class RedisClientFactory(val host: String, val port: Int, val database: Int = 0, val secret: Option[Any] = None, val timeout : Int = 0, val sslContext: Option[SSLContext] = None, batch: RedisClient.Mode = RedisClient.SINGLE)
  extends PooledObjectFactory[RedisClient] {

  // when we make an object it's already connected
  override def makeObject: PooledObject[RedisClient] = {
    new DefaultPooledObject[RedisClient](new RedisClient(host, port, database, secret, timeout, sslContext, batch))
  }

  // quit & disconnect
  override def destroyObject(p: PooledObject[RedisClient]): Unit = {
    val rc = p.getObject
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  override def passivateObject(p: PooledObject[RedisClient]): Unit = {}
  override def validateObject(p: PooledObject[RedisClient]): Boolean = p.getObject.connected

  // noop: it should be connected already
  override def activateObject(p: PooledObject[RedisClient]): Unit = {}
}

object RedisClientPool {
  val UNLIMITED_CONNECTIONS: Int = -1
}

class RedisClientPool(
                       val host: String,
                       val port: Int,
                       val maxIdle: Int = 8,
                       val database: Int = 0,
                       val secret: Option[Any] = None,
                       val timeout: Int = 0,
                       val maxConnections: Int = RedisClientPool.UNLIMITED_CONNECTIONS,
                       val poolWaitTimeout: Long = 3000,
                       val sslContext: Option[SSLContext] = None,
                       val batch: RedisClient.Mode = RedisClient.SINGLE
                     ) {

  val objectPoolConfig = new GenericObjectPoolConfig[RedisClient]
  objectPoolConfig.setMaxIdle(maxIdle)
  objectPoolConfig.setMaxTotal(maxConnections)
  objectPoolConfig.setBlockWhenExhausted(true)
  objectPoolConfig.setTestOnBorrow(false)
  objectPoolConfig.setTestOnReturn(true)

  val abandonedConfig = new AbandonedConfig
  abandonedConfig.setRemoveAbandonedTimeout(TimeUnit.MILLISECONDS.toSeconds(poolWaitTimeout).toInt)
  val pool = new GenericObjectPool(new RedisClientFactory(host, port, database, secret, timeout, sslContext, batch), objectPoolConfig,abandonedConfig)
  override def toString: String = host + ":" + String.valueOf(port)

  def withClient[T](body: RedisClient => T): T = {
    val client = pool.borrowObject
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }

  // close pool & free resources
  def close(): Unit = pool.close()
}
