package com.fotolog.redis.commands

import com.fotolog.redis.BinaryConverter
import com.fotolog.redis.connections._
import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions

import scala.concurrent.Future

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] trait SortedSetCommands extends ClientCommands {

  import com.fotolog.redis.commands.ClientCommands._

  def zaddAsync[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Zadd(key, kvs.map(els => els._1.toString -> conv.write(els._2)), opts)).map(integerResultAsInt)

  def zadd[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Int = await {
    zaddAsync(key, opts, kvs: _*)(conv)
  }

  def zcardAsync(key: String): Future[Int] = r.send(Zcard(key)).map(integerResultAsInt)

  def zcard[T](key: String): Int = await {
    zcardAsync(key)
  }

  def zcountAsync(key: String, min: Float, max: Float): Future[Int] = r.send(Zcount(key, min, max)).map(integerResultAsInt)

  def zcount[T](key: String, min: Float, max: Float): Int = await {
    zcountAsync(key, min, max)
  }

  def zincrbyAsync[T](key: String, increment: Float, member: T)(implicit conv: BinaryConverter[T]): Future[Float] = {
    r.send(Zincrby(key, increment, conv.write(member))).map(stringResultAsFloat)
  }

  def zincrby[T](key: String, increment: Float, member: T): Float = await {
    zincrbyAsync(key, increment, member)
  }

  def zinterstoreAsync = ???

  def zinterstore = ???

  def zlexcountAsync(key: String, min: Float, max: Float): Future[Int] = r.send(Zlexcount(key, min, max)).map(integerResultAsInt)

  def zlexcount[T](key: String, min: Float, max: Float): Int = await {
    zcountAsync(key, min, max)
  }

}
