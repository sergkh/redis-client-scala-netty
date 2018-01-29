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
    out.writeBytes(("*" + cmdParts.length + "\r\n").getBytes)

    for (p <- cmdParts) {
      out.writeBytes(("$" + p.length + "\r\n").getBytes) // len of the chunk
      out.writeBytes(p)
      out.writeBytes(EOL)
    }
  }
}
