package com.redis

import com.redis.api.NodeApi

trait NodeOperations extends NodeApi {
  self: Redis =>

  override def save: Boolean =
    send("SAVE")(asBoolean)

  override def bgsave: Boolean =
    send("BGSAVE")(asBoolean)

  override def lastsave: Option[Long] =
    send("LASTSAVE")(asLong)

  override def shutdown: Boolean =
    send("SHUTDOWN")(asBoolean)

  override def bgrewriteaof: Boolean =
    send("BGREWRITEAOF")(asBoolean)

  override def info: Option[String] =
    send("INFO")(asBulk)

  override def monitor: Boolean =
    send("MONITOR")(asBoolean)

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
