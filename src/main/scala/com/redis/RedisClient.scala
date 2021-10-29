package com.redis

import java.net.SocketException
import javax.net.ssl.SSLContext

import com.redis.serialization.Format

object RedisClient {
  sealed trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  sealed trait Mode
  case object SINGLE extends Mode
  case object BATCH extends Mode

  private def extractDatabaseNumber(connectionUri: java.net.URI): Int = {
    Option(connectionUri.getPath).map(path =>
      if (path.isEmpty) 0
      else Integer.parseInt(path.tail)
    )
      .getOrElse(0)
  }
}

import RedisClient._
abstract class Redis(batch: Mode) extends IO with Protocol {
  var handlers: Vector[(String, () => Any)] = Vector.empty
  var commandBuffer: StringBuffer           = new StringBuffer
  val crlf = "\r\n"


  def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = try {
    if (batch == BATCH) {
      handlers :+= ((command, () => result))
      commandBuffer.append((List(command) ++ args.toList).mkString(" ") ++ crlf)
      null.asInstanceOf[A] // hack
    } else {
      write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
      result
    }
  } catch {
    case e: RedisConnectionException =>
      if (disconnect) send(command, args)(result)
      else throw e
    case e: SocketException =>
      if (disconnect) send(command, args)(result)
      else throw e
  }

  def send[A](command: String, submissionMode: Boolean = false)(result: => A): A = try {
    if (batch == BATCH) {
      if (!submissionMode) {
        handlers :+= ((command, () => result))
        commandBuffer.append(command ++ crlf)
        null.asInstanceOf[A]
      } else {
        write(command.getBytes("UTF-8"))
        result
      }
    } else {
      write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
      result
    }
  } catch {
    case e: RedisConnectionException =>
      if (disconnect) send(command, submissionMode)(result)
      else throw e
    case e: SocketException =>
      if (disconnect) send(command, submissionMode)(result)
      else throw e
  }

  def cmd(args: Seq[Array[Byte]]): Array[Byte] = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList

}

abstract class RedisCommand(batch: Mode) extends Redis(batch)
  with BaseOperations
  with GeoOperations
  with NodeOperations
  with StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations
  with HashOperations
  with EvalOperations
  with PubOperations
  with HyperLogLogOperations
  with AutoCloseable {

  val database: Int = 0
  val secret: Option[Any] = None

  override def onConnect(): Unit = {
    secret.foreach {s =>
      auth(s)
    }
    selectDatabase()
  }

  private def selectDatabase(): Unit = {
    if (database != 0)
      select(database)
  }

  private def authenticate(): Unit = {
    secret.foreach(auth _)
  }
}

class RedisClient(override val host: String, override val port: Int,
    override val database: Int = 0, override val secret: Option[Any] = None, override val timeout : Int = 0, 
    override val sslContext: Option[SSLContext] = None, val batch: Mode = RedisClient.SINGLE)
  extends RedisCommand(batch) with PubSub {

  def this() = this("localhost", 6379)
  def this(connectionUri: java.net.URI) = this(
    host = connectionUri.getHost,
    port = connectionUri.getPort,
    database = RedisClient.extractDatabaseNumber(connectionUri),
    secret = Option(connectionUri.getUserInfo)
      .flatMap(_.split(':') match {
        case Array(_, password, _*) => Some(password)
        case _ => None
      })
  )
  override def toString: String = host + ":" + String.valueOf(port) + "/" + database

  // with MULTI/EXEC
  def pipeline(f: PipelineClient => Any): Option[List[Any]] = {
    send("MULTI", false)(asString) // flush reply stream
    try {
      val pipelineClient = new PipelineClient(this)
      try {
        f(pipelineClient)
      } catch {
        case e: Exception =>
          send("DISCARD", false)(asString)
          throw e
      }
      send("EXEC", false)(asExec(pipelineClient.responseHandlers))
    } catch {
      case e: RedisMultiExecException =>
        None
    }
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.{Future, Promise}
  import scala.util.Try

  /**
   * Redis pipelining API without the transaction semantics. The implementation has a non-blocking
   * semantics and returns a <tt>List</tt> of <tt>Promise</tt>. The caller may use <tt>Future.firstCompletedOf</tt> to get the
   * first completed task before all tasks have been completed. However the commands are submitted one by one and NOT in batch.
   * If you want to send the commands in batch mode, use the `batchedPipeline` method.
   * If an exception is raised in executing any of the commands, then the corresponding <tt>Promise</tt> holds
   * the exception. Here's a sample usage:
   * <pre>
   * val x =
   *  r.pipelineNoMulti(
   *    List(
   *      {() => r.set("key", "debasish")},
   *      {() => r.get("key")},
   *      {() => r.get("key1")},
   *      {() => r.lpush("list", "maulindu")},
   *      {() => r.lpush("key", "maulindu")}     // should raise an exception
   *    )
   *  )
   * </pre>
   *
   * This queues up all commands and does pipelining. The returned r is a <tt>List</tt> of <tt>Promise</tt>. The client
   * may want to wait for all to complete using:
   *
   * <pre>
   * val result = x.map{a => Await.result(a.future, timeout)}
   * </pre>
   *
   * Or the client may wish to track and get the promises as soon as the underlying <tt>Future</tt> is completed.
   */
  def pipelineNoMulti(commands: Seq[() => Any]) = {
    val ps = List.fill(commands.size)(Promise[Any]())
    var i = -1
    val f = Future {
      commands.map {command =>
        i = i + 1
        Try {
          command()
        } recover {
          case ex: java.lang.Exception =>
            ps(i) success ex
        } foreach {r =>
          ps(i) success r
        }
      }
    }
    ps
  }

  // batched pipelines : all commands submitted in batch
  def batchedPipeline(commands: List[() => Any]): Option[List[Any]] = {
    assert(batch == BATCH)
    commands.foreach { command =>
      command()
    }
    val r = send(commandBuffer.toString, true)(Some(handlers.map(_._2).map(_()).toList))
    handlers = Vector.empty
    commandBuffer.setLength(0)
    r
  }

  class PipelineClient(parent: RedisClient) extends RedisCommand(parent.batch) with PubOperations {
    import com.redis.serialization.Parse

    var responseHandlers: Vector[() => Any] = Vector.empty

    override def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {
      write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
      responseHandlers :+= (() => result)
      receive(singleLineReply).map(Parse.parseDefault)
      null.asInstanceOf[A] // ugh... gotta find a better way
    }
    override def send[A](command: String, submissionMode: Boolean = false)(result: => A): A = {
      write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
      responseHandlers :+= (() => result)
      receive(singleLineReply).map(Parse.parseDefault)
      null.asInstanceOf[A]
    }

    val host = parent.host
    val port = parent.port
    val timeout = parent.timeout
    override val secret = parent.secret
    override val database = parent.database

    // TODO: Find a better abstraction
    override def connected = parent.connected
    override def connect = parent.connect
    override def disconnect = parent.disconnect
    override def clearFd() = parent.clearFd()
    override def write(data: Array[Byte]) = parent.write(data)
    override def readLine = parent.readLine
    override def readCounted(count: Int) = parent.readCounted(count)
    override def onConnect() = parent.onConnect()

    override def close(): Unit = parent.close()
  }

  override def close(): Unit = disconnect
}
