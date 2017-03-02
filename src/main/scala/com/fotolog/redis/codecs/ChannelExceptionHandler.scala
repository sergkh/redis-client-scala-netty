package com.fotolog.redis.codecs

import io.netty.channel.ChannelHandlerContext

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[codecs] trait ChannelExceptionHandler {
  def handleException(ctx: ChannelHandlerContext, ex: Throwable) {
    ctx.close()
  }
}
