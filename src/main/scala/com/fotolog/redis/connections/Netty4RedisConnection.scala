package com.fotolog.redis.connections

import java.net.InetSocketAddress
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.fotolog.redis._
import com.fotolog.redis.codecs._
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.DelimiterBasedFrameDecoder

import scala.concurrent.Future
import scala.util.Try

object Netty4RedisConnection {

  private[redis] val cmdQueue = new ArrayBlockingQueue[(Netty4RedisConnection, ResultFuture)](2048)
  private[redis] val commandEncoder = new RedisCommandEncoder()

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
  private[Netty4RedisConnection] val (workerGroup, transportProtocol) = selectTransportProtocol
  private[Netty4RedisConnection] val clientBootstrap = createClientBootstrap
  private[Netty4RedisConnection] val opQueue = new ArrayBlockingQueue[ResultFuture](1028)
  private[Netty4RedisConnection] var clientState = new AtomicReference[ConnectionState](NormalConnectionState(opQueue))

  private def selectTransportProtocol = {
    val osName = Option(System.getProperties.getProperty("os.name")).map(_.toLowerCase)
    val isUnix = osName.exists(os => os.contains("nix") || os.contains("nux") || os.contains("aix"))

    if (isUnix) {
      new EpollEventLoopGroup() -> classOf[EpollSocketChannel]
    } else {
      new NioEventLoopGroup() -> classOf[NioSocketChannel]
    }
  }

  private[Netty4RedisConnection] def createClientBootstrap: Bootstrap = {

    val bootstrap = new Bootstrap()
    bootstrap.group(workerGroup)
      .channel(transportProtocol)
      .option(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .option(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Int.box(1000))
      .handler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(ch: SocketChannel): Unit = {
          val pipeline = ch.pipeline()

          pipeline.addLast("response_frame_decoder", new DelimiterBasedFrameDecoder(512 * 1024 * 1024, false, Unpooled.wrappedBuffer("\r\n".getBytes)))
          pipeline.addLast("response_decoder", new RedisResponseDecoder())
          pipeline.addLast("response_array_agregator", new RedisArrayAgregator())
          pipeline.addLast("response_handler", new RedisResponseHandler(clientState))

          pipeline.addLast("request_command_encoder", commandEncoder)
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


