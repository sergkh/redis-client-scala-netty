package com.fotolog.redis.codecs

import java.util

import com.fotolog.redis.{ArrayHeaderRedisMessage, RedisMessage}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{CodecException, MessageToMessageDecoder}
import io.netty.util.ReferenceCountUtil

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.03.2017.
  */
class RedisArrayAgregator extends MessageToMessageDecoder[RedisMessage] {
}
