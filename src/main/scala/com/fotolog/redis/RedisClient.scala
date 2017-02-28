package com.fotolog.redis

import java.net.URI
import java.util.concurrent.TimeUnit

import com.fotolog.redis.commands._
import com.fotolog.redis.connections.{InMemoryRedisConnection, Netty3RedisConnection, Netty4RedisConnection, RedisConnection}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object RedisClient {

  private final val DEFAULT_TIMEOUT = Duration(1, TimeUnit.MINUTES)

  implicit class RichFutureWrapper[T](val f: Future[T]) extends AnyVal {
    def get() = Await.result[T](f, DEFAULT_TIMEOUT)
  }

  @throws(classOf[AuthenticationException])
  // redis://[:password@]host[:port][/db-number]
  def apply(uri: String = "redis://localhost:6379", timeout: Duration = DEFAULT_TIMEOUT) = {

    val redisUri = new URI(uri)

    Option(redisUri.getScheme) match {
      case Some("redis") | None =>
        val port = if (redisUri.getPort > 0) redisUri.getPort else 6379
        //val client = new RedisClient(new Netty3RedisConnection(redisUri.getHost, port), timeout)
        val client = new RedisClient(new Netty4RedisConnection(redisUri.getHost, port), timeout)

        for (userInfo <- Option(redisUri.getUserInfo)) {
          val password = userInfo.stripPrefix(":")

          if (!client.auth(password)) {
            throw AuthenticationException("Authentication failed")
          }
        }

        for (db <- Option(redisUri.getPath) if db.nonEmpty) {
          val dbIndex = Try(db.toInt).filter(_ >= 0).getOrElse {
            throw new IllegalArgumentException(s"Invalid path value: '$db' in URI: '$uri'. Has to be a valid database index")
          }

          client.select(dbIndex)
        }

        client
      case Some("redis-mem") =>
        new RedisClient(new InMemoryRedisConnection(redisUri.getHost), timeout)
      case Some(unknownSchema) =>
        throw new IllegalArgumentException(s"Unsupported schema: '$unknownSchema' in URI: '$uri'. Valid schemas are 'redis' and 'redis-mem://'")
    }
  }

}

class RedisClient(val r: RedisConnection, val timeout: Duration) extends GenericCommands with StringCommands
                                             with HashCommands with ListCommands
                                             with SetCommands with ScriptingCommands with PubSubCommands
                                             with HyperLogLogCommands {

  def isConnected: Boolean = r.isOpen
  def shutdown() { r.shutdown() }
  def await[T](f: Future[T]) = Await.result[T](f, timeout)
}
