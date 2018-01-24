package com.fotolog.redis.codecs

import java.util


import com.fotolog.redis.codecs.RedisArrayAgregator.AggregateState
import com.fotolog.redis._
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder

import scala.collection.mutable.ArrayBuffer

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.03.2017.
  */
class RedisArrayAgregator extends MessageToMessageDecoder[RedisMessage] {

  val queue: util.Deque[AggregateState] = new util.ArrayDeque[AggregateState]

  override def decode(ctx: ChannelHandlerContext, msg: RedisMessage, out: util.List[AnyRef]): Unit = {
    msg match {
      case header: ArrayHeaderRedisMessage if header.length == 0 =>
        out.add(EmptyArrayRedisMessage)

      case header: ArrayHeaderRedisMessage if header.length == -1 =>
        out.add(NullRedisMessage)

      case ArrayHeaderRedisMessage(length) if queue.isEmpty =>
        queue.push(new AggregateState(length))

      case ArrayHeaderRedisMessage(length) if !queue.isEmpty =>
        queue.push(new AggregateState(length))

      case proxyMsg if queue.isEmpty =>
        out.add(proxyMsg)

      case partMsg if !queue.isEmpty =>

        var promMsg: RedisMessage = partMsg

        while (!queue.isEmpty) {
          val current = queue.peekFirst()
          current.add(promMsg)

          if (current.isFull) {
            promMsg = ArrayRedisMessage(current.msgs.toList)
            queue.pop()
          } else {
            promMsg = null
            return
          }
        }

        Option(promMsg).foreach(msg => out.add(msg))
    }
  }

}

object RedisArrayAgregator {

  case class AggregateState(length: Int, msgs: ArrayBuffer[RedisMessage]) {

    def isFull: Boolean = length == msgs.size

    def this(length: Int) = this(length, new ArrayBuffer[RedisMessage](length))

    def add(msg: RedisMessage): Unit = msgs.append(msg)
  }

}
