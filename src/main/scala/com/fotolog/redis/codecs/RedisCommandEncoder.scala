package com.fotolog.redis.codecs

import com.fotolog.redis.connections.ResultFuture
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
@Sharable
private[redis] class RedisCommandEncoder extends MessageToByteEncoder[ResultFuture] {

  import com.fotolog.redis.connections.Cmd._

  override def encode(ctx: ChannelHandlerContext, msg: ResultFuture, out: ByteBuf): Unit = {
    binaryCmd(msg.cmd.asBin, out)
  }

  private def binaryCmd(cmdParts: Seq[Array[Byte]], out: ByteBuf) = {
    out.writeBytes(ARRAY_START)
    out.writeBytes(cmdParts.length.toString.getBytes)
    out.writeBytes(EOL)

    for (p <- cmdParts) {
      out.writeBytes(STRING_START)
      out.writeBytes(p.length.toString.getBytes)
      out.writeBytes(EOL)
      out.writeBytes(p)
      out.writeBytes(EOL)
    }
  }
}
