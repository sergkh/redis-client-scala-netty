package com.fotolog.redis.connections

import com.fotolog.redis.RedisException

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

private[redis] sealed abstract class Result

trait SuccessResult extends Result

case class ErrorResult(err: String) extends Result

case class SingleLineResult(msg: String) extends SuccessResult

case class BulkDataResult(data: Option[Array[Byte]]) extends SuccessResult {
  override def toString =
    "BulkDataResult(%s)".format(data.map(d => new String(d)).getOrElse(""))
}

case class MultiBulkDataResult(results: Seq[BulkDataResult]) extends SuccessResult

/**
  * Holds command and promise for response for that command.
  * Promise will be satisfied after response from server for that command.
  *
  * @param cmd command that waits for result.
  */
private[redis] class ResultFuture(val cmd: Cmd) {
  val promise = Promise[Result]()

  def future = promise.future

  def fillWithSuccess(r: SuccessResult): Boolean = {
    promise.success(r); true
  }

  def fillWithFailure(err: ErrorResult) = promise.failure(new RedisException(err.err))

  def complete: Boolean = true
}


/**
  * ResultFuture that contains command which receives multiple responses.
  * Used when dealing with Subscribe/Unsubscribe commands which forces server to
  * respond with multiple BulkDataResults which here are packaged into MultiBulkDataResults.
  *
  * @param complexCmd command to handle response for
  * @param parts      number of parts that command expects
  */
private[redis] case class ComplexResultFuture(complexCmd: Cmd, parts: Int) extends ResultFuture(complexCmd) {
  val responses = new ListBuffer[BulkDataResult]()

  override def fillWithSuccess(r: SuccessResult) = {
    r match {
      case MultiBulkDataResult(Seq(subCmd, channel, result)) =>
        responses += result

        if (responses.length == parts) {
          promise.success(MultiBulkDataResult(responses))
          true
        } else {
          false
        }
    }
  }

  override def complete = responses.length == parts
}

object ResultFuture {
  def apply(cmd: Cmd) = {
    cmd match {
      case scrb: Subscribe =>
        ComplexResultFuture(scrb, scrb.channels.length)
      case unscrb: Unsubscribe =>
        ComplexResultFuture(unscrb, unscrb.channels.length)
      case any: Cmd =>
        new ResultFuture(any)
    }
  }

}