package com.fotolog.redis.connections

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.fotolog.redis._
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}
import io.netty.util.ByteProcessor

import scala.concurrent.Future
import scala.util.Try

object Netty4RedisConnection {

  private[redis] val cmdQueue = new ArrayBlockingQueue[(Netty4RedisConnection, ResultFuture)](2048)

  private[redis] val queueProcessor = new Runnable {
    override def run() = {
      while (true) {
        val (conn, f) = cmdQueue.take()
        try {
          if (conn.isOpen) {
            conn.enqueue(f)
          } else {
            conn.reconnect()
            f.promise.failure(new IllegalStateException("Channel closed, command: " + f.cmd))
          }
        } catch {
          case e: Exception =>
            f.promise.failure(e); conn.shutdown()
        }
      }
    }
  }

  new Thread(queueProcessor, "Queue Processor").start()
}

class Netty4RedisConnection(val host: String, val port: Int) extends RedisConnection {

  import com.fotolog.redis.connections.Netty4RedisConnection._

  private[Netty4RedisConnection] val isRunning = new AtomicBoolean(true)
  private[Netty4RedisConnection] val isConnecting = new AtomicBoolean(false)
  private[Netty4RedisConnection] val workerGroup: EventLoopGroup = new NioEventLoopGroup()
  private[Netty4RedisConnection] val clientBootstrap = createClientBootstrap
  private[Netty4RedisConnection] val opQueue = new ArrayBlockingQueue[ResultFuture](1028)
  private[Netty4RedisConnection] var clientState = new AtomicReference[ConnectionState](NormalConnectionState(opQueue))

  private[Netty4RedisConnection] def createClientBootstrap: Bootstrap = {

    val bootstrap = new Bootstrap()
    bootstrap.group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .option(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Int.box(1000))
      .handler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(ch: SocketChannel): Unit = {
          val pipeline = ch.pipeline()

          //pipeline.addLast("response_frame_decoder", new DelimiterBasedFrameDecoder(512 * 1024 *1024, false, Unpooled.wrappedBuffer("\r\n".getBytes)))
          pipeline.addLast("response_decoder", new RedisResponseDecoder4())
          pipeline.addLast("response_accumulator", new RedisResponseAccumulator4(clientState))

          pipeline.addLast("command_encoder", new RedisCommandEncoder4())
        }
      })
  }

  private[Netty4RedisConnection] val channel = new AtomicReference(newChannel())

  def newChannel(): Channel = {
    isConnecting.set(true)

    val channelReady = new CountDownLatch(1)
    val future = clientBootstrap.connect(new InetSocketAddress(host, port))

    var channelInternal: Channel = null

    future.addListener(new ChannelFutureListener() {
      override def operationComplete(channelFuture: ChannelFuture) = {
        if (future.isSuccess) {
          channelInternal = channelFuture.channel()
          channelReady.countDown()
        } else {
          channelReady.countDown()
          throw channelFuture.cause()
        }
      }
    })

    try {
      channelReady.await(1, TimeUnit.MINUTES)
      isConnecting.set(false)
    } catch {
      case _: InterruptedException =>
        throw new RedisException("Interrupted while waiting for connection")
    }

    channelInternal
  }

  def reconnect() {
    if (isRunning.get() && !isConnecting.compareAndSet(false, true)) {
      new Thread("reconnection-thread") {
        override def run() {
          Try {
            channel.set(newChannel())
          }
        }
      }.start()

    }
  }

  def send(cmd: Cmd): Future[Result] = {
    if (!isRunning.get()) {
      throw new RedisException("Connection closed")
    }
    val f = ResultFuture(cmd)
    cmdQueue.offer((this, f), 10, TimeUnit.SECONDS)
    f.future
  }

  def isOpen: Boolean = {
    val channelLocal = channel.get()
    isRunning.get() && channelLocal != null && channelLocal.isOpen
  }

  def shutdown() {
    if (isOpen) {
      isRunning.set(false)
      val channelClosed = new CountDownLatch(1)

      channel.get.close().addListener(new ChannelFutureListener() {
        override def operationComplete(channelFuture: ChannelFuture) = {
          channelClosed.countDown()
        }
      })

      try {
        channelClosed.await(1, TimeUnit.MINUTES)
      } catch {
        case _: InterruptedException =>
          throw new RedisException("Interrupted while waiting for connection close")
      }
    }

    workerGroup.shutdownGracefully()
  }

  private def enqueue(f: ResultFuture) {
    opQueue.offer(f, 10, TimeUnit.SECONDS)
    channel.get().writeAndFlush(f).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
  }
}


private[redis] trait ChannelExceptionHandler4 {
  def handleException(ctx: ChannelHandlerContext, ex: Throwable) {
    ctx.close()
  }
}

private[redis] class RedisResponseDecoder4 extends ByteToMessageDecoder with ChannelExceptionHandler4 {

  val charset: Charset = Charset.forName("UTF-8")
  var responseType: ResponseType = Unknown

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    responseType match {
      case Unknown if in.isReadable =>
        responseType = ResponseType(in.readByte)

      case Unknown if !in.isReadable =>

      case BulkData => readAsciiLine(in).foreach { line =>
        line.toInt match {
          case -1 =>
            responseType = Unknown
            out.add(NullData)
          case n =>
            responseType = BinaryData(n)
        }
      }

      case BinaryData(len) =>
        if (in.readableBytes >= (len + 2)) {
          // +2 for eol
          responseType = Unknown
          val bytes = new Array[Byte](len)
          in.readBytes(bytes)
          in.skipBytes(2)
          out.add(bytes)
        }

      case x => readAsciiLine(in).map { line =>
        responseType = Unknown
        out.add((x, line))
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

private[redis] class RedisResponseAccumulator4(connStateRef: AtomicReference[ConnectionState]) extends ChannelInboundHandlerAdapter with ChannelExceptionHandler4 {

  import scala.collection.mutable.ArrayBuffer

  val bulkDataBuffer = ArrayBuffer[BulkDataResult]()
  var numDataChunks = 0

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Nil)

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) {
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

@Sharable
private[redis] class RedisCommandEncoder4 extends MessageToByteEncoder[ResultFuture] {

  import com.fotolog.redis.connections.Cmd._

  override def encode(ctx: ChannelHandlerContext, msg: ResultFuture, out: ByteBuf): Unit = {
    binaryCmd(msg.cmd.asBin, out)
  }

  private def binaryCmd(cmdParts: Seq[Array[Byte]], out: ByteBuf) = {
    val params = new Array[Array[Byte]](3 * cmdParts.length + 1)
    params(0) = ("*" + cmdParts.length + "\r\n").getBytes
    // num binary chunks
    var i = 1
    for (p <- cmdParts) {
      params(i) = ("$" + p.length + "\r\n").getBytes // len of the chunk
      i = i + 1
      params(i) = p
      i = i + 1
      params(i) = EOL
      i = i + 1
    }

    params.foreach { bytes =>
      out.writeBytes(bytes)
    }
  }
}