package com.redis

import com.redis.api.NodeApi

trait NodeOperations extends NodeApi {
  self: Redis =>

  override def save: Boolean =
    send("SAVE", false)(asBoolean)

  override def bgsave: Boolean =
    send("BGSAVE", false)(asBoolean)

  override def lastsave: Option[Long] =
    send("LASTSAVE", false)(asLong)

  override def shutdown: Boolean =
    send("SHUTDOWN", false)(asBoolean)

  override def bgrewriteaof: Boolean =
    send("BGREWRITEAOF", false)(asBoolean)

  override def info: Option[String] =
    send("INFO", false)(asBulk)

  override def monitor: Boolean =
    send("MONITOR", false)(asBoolean)

  override def slaveof(options: Any): Boolean = options match {
    case (h: String, p: Int) =>
      send("SLAVEOF", List(h, p))(asBoolean)
    case _ => setAsMaster()
  }

  @deprecated("use slaveof", "1.2.0")
  def slaveOf(options: Any): Boolean =
    slaveof(options)

  private def setAsMaster(): Boolean =
    send("SLAVEOF", List("NO", "ONE"))(asBoolean)
}
