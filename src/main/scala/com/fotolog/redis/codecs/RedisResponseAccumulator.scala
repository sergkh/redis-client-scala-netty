package com.fotolog.redis.codecs

import java.util.concurrent.atomic.AtomicReference

import com.fotolog.redis.connections._
import com.fotolog.redis.{Error, Integer, MultiBulkData, NullData, ResponseType, SingleLine}
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseAccumulator(connStateRef: AtomicReference[ConnectionState]) extends SimpleChannelInboundHandler[AnyRef] with ChannelExceptionHandler {

  import scala.collection.mutable.ArrayBuffer

  val bulkDataBuffer = ArrayBuffer[BulkDataResult]()
  var numDataChunks = 0

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Nil)

  override def channelRead0(ctx: ChannelHandlerContext, msg: AnyRef) {
    msg match {
      case (resType: ResponseType, line: String) =>
        clear()
        resType match {
          case Error => handleResult(ErrorResult(line))
          case SingleLine => handleResult(SingleLineResult(line))
          case Integer => handleResult(BulkDataResult(Some(line.getBytes)))
          case MultiBulkData => line.toInt match {
            case x if x <= 0 => handleResult(EMPTY_MULTIBULK)
            case n => numDataChunks = line.toInt // ask for bulk data chunks
          }
          case _ => throw new Exception("Unexpected %s -> %s".format(resType, line))
        }
      case data: Array[Byte] => handleDataChunk(Option(data))
      case NullData => handleDataChunk(None)
      case _ => throw new Exception("Unexpected error: " + msg)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }

  private def handleDataChunk(bulkData: Option[Array[Byte]]) {
    val chunk = bulkData match {
      case None => BULK_NONE
      case Some(data) => BulkDataResult(Some(data))
    }

    numDataChunks match {
      case 0 =>
        handleResult(chunk)

      case 1 =>
        bulkDataBuffer += chunk
        val allChunks = new Array[BulkDataResult](bulkDataBuffer.length)
        bulkDataBuffer.copyToArray(allChunks)
        clear()
        handleResult(MultiBulkDataResult(allChunks))

      case _ =>
        bulkDataBuffer += chunk
        numDataChunks = numDataChunks - 1
    }
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

  private def clear() {
    numDataChunks = 0
    bulkDataBuffer.clear()
  }
}
