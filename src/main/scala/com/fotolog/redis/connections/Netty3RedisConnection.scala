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

object Netty3RedisConnection {

  private[redis] val cmdQueue = new ArrayBlockingQueue[(Netty3RedisConnection, ResultFuture)](2048)

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

class Netty3RedisConnection(val host: String, val port: Int) extends RedisConnection {

  import com.fotolog.redis.connections.Netty3RedisConnection._

  private[Netty3RedisConnection] val isRunning = new AtomicBoolean(true)
  private[Netty3RedisConnection] val isConnecting = new AtomicBoolean(false)
  private[Netty3RedisConnection] val clientBootstrap = createClientBootstrap
  private[Netty3RedisConnection] val opQueue = new ArrayBlockingQueue[ResultFuture](1028)
  private[Netty3RedisConnection] var clientState = new AtomicReference[ConnectionState](NormalConnectionState(opQueue))

  private[Netty3RedisConnection] def createClientBootstrap: Bootstrap = {
    val workerGroup: EventLoopGroup = new NioEventLoopGroup()

    val bootstrap = new Bootstrap()
    bootstrap.group(workerGroup)
    bootstrap.channel(classOf[NioSocketChannel])
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true)
    bootstrap.option(ChannelOption.TCP_NODELAY, true)
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)

    bootstrap.handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline().addLast(
          new RedisResponseDecoder(), new RedisResponseAccumulator(clientState)
        )
        //new DelimiterBasedFrameDecoder()
        //, new RedisCommandEncoder()
      }
    })
  }

  private[Netty3RedisConnection] val channel = new AtomicReference(newChannel())

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

  def enqueue(f: ResultFuture) {
    opQueue.offer(f, 10, TimeUnit.SECONDS)
    channel.get().write(f).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
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
          //channelFuture.channel().close()
        }
      })

      try {
        channelClosed.await(1, TimeUnit.MINUTES)
      } catch {
        case _: InterruptedException =>
          throw new RedisException("Interrupted while waiting for connection close")
      }
    }
  }
}


private[redis] trait ChannelExceptionHandler {
  def handleException(ctx: ChannelHandlerContext, ex: Throwable) {
    ctx.close()
  }
}

private[redis] class RedisResponseDecoder extends ByteToMessageDecoder with ChannelExceptionHandler {

  val charset: Charset = Charset.forName("UTF-8")
  var responseType: ResponseType = Unknown

  //message received to channel read
  //Delimiters.lineDelimiter()

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    responseType match {
      case Unknown if in.isReadable =>
        responseType = ResponseType(in.readByte)
        decode(ctx, in, out)

      case Unknown if !in.isReadable => // need more data

      case BulkData => readAsciiLine(in) match {
        case null => // need more data
        case line => line.toInt match {
          case -1 =>
            responseType = Unknown
            out.add(NullData)
          case n =>
            responseType = BinaryData(n)
            decode(ctx, in, out)
        }
      }

      case BinaryData(len) =>
        if (in.readableBytes >= (len + 2)) {
          // +2 for eol
          responseType = Unknown
          val data = in.readSlice(len)
          in.skipBytes(2) // eol is there too
          out.add(data)
        } else {
          // need more data
        }

      case x => readAsciiLine(in) match {
        case null => null // need more data
        case line =>
          responseType = Unknown
          out.add((x, line))
      }
    }
  }

  private def findEndOfLine(buffer: ByteBuf): Int = {
    val i = buffer.forEachByte(ByteProcessor.FIND_LF)
    if (i > 0 && buffer.getByte(i - 1) == '\r') i - 1 else -1
  }

  private def readAsciiLine(buf: ByteBuf): String = if (!buf.isReadable) null else {
    findEndOfLine(buf) match {
      case -1 => null
      case n =>
        val line = buf.toString(buf.readerIndex, n - buf.readerIndex, charset)
        buf.skipBytes(line.length + 2)
        line
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }
}

/*class AA extends SimpleChannelInboundHandler {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Nothing): Unit = ???
}*/

private[redis] class RedisResponseAccumulator(connStateRef: AtomicReference[ConnectionState]) extends ChannelInboundHandlerAdapter with ChannelExceptionHandler {

  import scala.collection.mutable.ArrayBuffer

  val bulkDataBuffer = ArrayBuffer[BulkDataResult]()
  var numDataChunks = 0

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Seq())

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
      case data: ByteBuf => handleDataChunk(data)
      case NullData => handleDataChunk(null)
      case _ => throw new Exception("Unexpected error: " + msg)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }

  private def handleDataChunk(bulkData: ByteBuf) {
    val chunk = bulkData match {
      case null =>
        BULK_NONE
      case buf =>
        if (buf.isReadable) {
          val bytes = new Array[Byte](buf.readableBytes())
          buf.readBytes(bytes)
          BulkDataResult(Some(bytes))
        } else BulkDataResult(None)
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
private[redis] class RedisCommandEncoder extends MessageToByteEncoder[ResultFuture] {

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

/**
  * Connection can operate in two states: Normal which is used for all major commands and
  * Subscribed with reduced commands set and receiving responses without issuing commands.
  *
  * Base implementation adds support for complex commands which are commands that receives
  * more than one response. E.g. Subscribe/Unsibscribe commands receive separate BulkDataResult
  * for each specified channel, all other commands has one to one relationship with responses.
  */
sealed abstract class ConnectionState(queue: BlockingQueue[ResultFuture]) {

  var currentComplexResponse: Option[ResultFuture] = None

  def fillResult(r: Result): ResultFuture = {
    val nextFuture = nextResultFuture()

    nextFuture.fillWithResult(r)

    if (!nextFuture.complete) {
      currentComplexResponse = Some(nextFuture)
    } else {
      currentComplexResponse = None
    }

    nextFuture
  }

  def fillError(err: ErrorResult) {
    nextResultFuture().fillWithFailure(err)
    currentComplexResponse = None
  }

  /**
    * Handles results got from socket. Optionally can return new connection state.
    *
    * @param r result to handle
    * @return new connection state or none if current state remains.
    */
  def handle(r: Result): Option[ConnectionState]

  private[this] def nextResultFuture() = currentComplexResponse getOrElse queue.poll(60, TimeUnit.SECONDS)
}

/**
  * Processes responses for all commands and completes promises of results in listeners.
  * Can be changed to subscribed state when receives results of Subscribe command.
  *
  * @param queue queue of result promises holders.
  */
case class NormalConnectionState(queue: BlockingQueue[ResultFuture]) extends ConnectionState(queue) {
  def handle(r: Result): Option[ConnectionState] = r match {
    case err: ErrorResult =>
      fillError(err)
      None
    case r: Result =>
      val respFuture = fillResult(r)

      respFuture.cmd match {
        case subscribeCmd: Subscribe if respFuture.complete =>
          Some(SubscribedConnectionState(queue, subscribeCmd))
        case _ =>
          None
      }
  }
}

/**
  * Connection state that supports only limited set of commands (Subscribe/Unsubscribe) and can process
  * messages from subscribed channels and pass them to channel subscribers.
  * When no subscribers left (as result of unsubscribing) switches to Normal connection state.
  *
  * @param queue     commands queue to process
  * @param subscribe subscribe command that caused state change.
  */
case class SubscribedConnectionState(queue: BlockingQueue[ResultFuture], subscribe: Subscribe) extends ConnectionState(queue) {

  type Subscriber = MultiBulkDataResult => Unit

  var subscribers = extractSubscribers(subscribe)

  def handle(r: Result): Option[ConnectionState] = {
    r match {
      case err: ErrorResult =>
        fillError(err)
      case cmdResult: BulkDataResult =>
        val respFuture = fillResult(r)

        if (respFuture.complete) {
          respFuture.cmd match {
            case subscribeCmd: Subscribe =>
              subscribers ++= extractSubscribers(subscribeCmd)
            case unsubscribeCmd: Unsubscribe =>
              return processUnsubscribe(unsubscribeCmd.channels)
            case other =>
              new RuntimeException("Unsupported response from server in subscribed mode: " + other)
          }
        }

      case message: MultiBulkDataResult =>
        val channel = message.results(1).data.map(new String(_)).get

        subscribers.foreach { case (pattern, handler) =>
          if (channel == pattern) handler(message)
        }
      case other =>
        new RuntimeException("Unsupported response from server in subscribed mode: " + other)
    }

    None
  }

  private def processUnsubscribe(channels: Seq[String]) = {
    subscribers = subscribers.filterNot { case (channel, _) =>
      channels.contains(channel)
    }

    if (subscribers.isEmpty) {
      Some(NormalConnectionState(queue))
    } else {
      None
    }
  }

  private def extractSubscribers(cmd: Subscribe) =
    cmd.channels.map(p => (p, cmd.handler))

}