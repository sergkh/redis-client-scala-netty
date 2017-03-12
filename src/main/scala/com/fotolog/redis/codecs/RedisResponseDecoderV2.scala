package com.fotolog.redis.codecs

import java.nio.charset.Charset
import java.util

import com.fotolog.redis._
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.util.ByteProcessor

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseDecoderV2 extends ByteToMessageDecoder with ChannelExceptionHandler {

  val charset: Charset = Charset.forName("UTF-8")
  var responseType: ResponseType = Unknown

  //TODO: add RedisResponseMessage Type, split into frame without '/r/n'
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    responseType match {
      case Unknown if in.isReadable =>
        responseType = ResponseType(in.readByte)

      case Unknown if !in.isReadable =>

      case BulkData => readAsciiLine(in).foreach { line =>
        line.toInt match {
          case -1 =>
            responseType = Unknown
            out.add(NullRedisMessage)
          case n =>
            responseType = BinaryData(n)
        }
      }

      //read simple string with specified length
      //TODO: move up
      case BinaryData(len) =>
        if (in.readableBytes >= (len + 2)) {
          // +2 for eol
          responseType = Unknown
          val bytes = new Array[Byte](len)
          in.readBytes(bytes)
          in.skipBytes(2)
          out.add(RawRedisMessage(bytes))
        }
      case MultiBulkData =>
        readAsciiLine(in).map { line =>
          responseType = Unknown
          out.add(ArrayHeaderRedisMessage(line.toInt))
        }
      case Integer =>
        readAsciiLine(in).map { line =>
          responseType = Unknown
          out.add(IntRedisMessage(line.toInt))
        }
      case Error =>
        readAsciiLine(in).map { line =>
          responseType = Unknown
          out.add(ErrorRedisMessage(line))
        }
      case SingleLine =>
        readAsciiLine(in).map { line =>
          responseType = Unknown
          out.add(StringRedisMessage(line))
        }
    }
  }

  private def findEndOfLine(buffer: ByteBuf): Int = {
    val i = buffer.forEachByte(ByteProcessor.FIND_LF)
    if (i > 0 && buffer.getByte(i - 1) == '\r') i - 1 else -1
  }

  private def readAsciiLine(buf: ByteBuf): Option[String] = if (!buf.isReadable) None else {
    findEndOfLine(buf) match {
      case -1 => None
      case n =>
        val line = buf.toString(buf.readerIndex, n - buf.readerIndex, charset)
        buf.skipBytes(line.length + 2)
        Some(line)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }
}
