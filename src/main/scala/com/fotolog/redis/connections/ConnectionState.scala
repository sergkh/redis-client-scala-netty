package com.fotolog.redis.connections

import java.util
import java.util.concurrent.{BlockingQueue, TimeUnit}

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
    case r: Result =>
      val respFuture = fillResult(r)

      respFuture.cmd match {
        case subscribeCmd: Subscribe if respFuture.complete =>
          Some(SubscribedConnectionState(queue, subscribeCmd))
        case _ =>
          None
      }

    case err: ErrorResult =>
      fillError(err)
      None
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

  private var subscribers = extractSubscribers(subscribe)

  def handle(r: Result): Option[ConnectionState] = {
    r match {
      case MultiBulkDataResult(Seq(mode, channel, bin)) if !util.Arrays.equals(mode.data.get, "message".getBytes) =>
        val respFuture = fillResult(MultiBulkDataResult(Seq(mode, channel, bin)))

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

      case e: MultiBulkDataResult =>

        val channel = e.results(1).data.map(new String(_)).get

        subscribers.foreach { case (pattern, handler) =>
          if (channel == pattern) handler(e)
        }

      case err: ErrorResult =>
        fillError(err)

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
