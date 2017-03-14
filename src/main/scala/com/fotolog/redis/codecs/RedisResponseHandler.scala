package com.fotolog.redis.codecs

import java.util.concurrent.atomic.AtomicReference

import com.fotolog.redis.connections._
import com.fotolog.redis.{EmptyArrayRedisMessage, _}
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseHandler(connStateRef: AtomicReference[ConnectionState]) extends SimpleChannelInboundHandler[RedisMessage] with ChannelExceptionHandler {

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Nil)

  override def channelRead0(ctx: ChannelHandlerContext, msg: RedisMessage): Unit = {
    msg match {
      case ErrorRedisMessage(error) => handleResult(ErrorResult(error))
      case StringRedisMessage(content) => handleResult(SingleLineResult(content))
      case IntRedisMessage(number) => handleResult(BulkDataResult(Some(number.toString.getBytes)))
      case RawRedisMessage(bytes) => handleResult(BulkDataResult(Option(bytes)))
      case EmptyArrayRedisMessage => handleResult(EMPTY_MULTIBULK)
      //TODO: Multibulk from multibulk
      case a: ArrayRedisMessage => handleResult(MultiBulkDataResult(a.asBulk))
      case NullRedisMessage => handleResult(BULK_NONE)
      case _ => throw new Exception("Unexpected error: " + msg)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }

  private def handleResult(r: Result) {
    try {
      //fill result
      val nextStateOpt = connStateRef.get().handle(r)

      for (nextState <- nextStateOpt) {
        connStateRef.set(nextState)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}
