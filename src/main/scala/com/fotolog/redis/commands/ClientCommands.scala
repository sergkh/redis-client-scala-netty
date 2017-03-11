package com.fotolog.redis.commands

import com.fotolog.redis._
import com.fotolog.redis.connections._

import scala.collection.{Set, mutable}
import scala.concurrent.Future

private[redis] trait ClientCommands {
  protected val r: RedisConnection
  def await[T](f: Future[T]): T
}

private[commands] object ClientCommands {

  val integerResultAsBoolean: PartialFunction[Result, Boolean] = {
    case BulkDataResult(Some(v)) => BinaryConverter.IntConverter.read(v) > 0
    case BulkDataResult(None) => throw new RuntimeException("Unknown integer type")
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  val okResultAsBoolean: PartialFunction[Result, Boolean] = {
    case SingleLineResult("OK") => true
    case BulkDataResult(None) => false
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
    // for cases where any other val should produce an error
  }

  val integerResultAsInt: PartialFunction[Result, Int] = {
    case BulkDataResult(Some(v)) => BinaryConverter.IntConverter.read(v)
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  val doubleResultAsDouble: PartialFunction[Result, Double] = {
    case BulkDataResult(Some(v)) => BinaryConverter.DoubleConverter.read(v)
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  val stringResultAsFloat: PartialFunction[Result, Float] = {
    case BulkDataResult(Some(v)) => BinaryConverter.FloatConverter.read(v)
    case SingleLineResult("QUEUED") => throw new RuntimeException("Should not be read")
  }

  def bulkDataResultToOpt[T](convert: BinaryConverter[T]): PartialFunction[Result, Option[T]] = {
    case BulkDataResult(data) => data.map(convert.read)
  }

  def multiBulkDataResultToFilteredSeq[T](conv: BinaryConverter[T]): PartialFunction[Result, Seq[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv)
  }

  def multiBulkDataResultToSet[T](conv: BinaryConverter[T]): PartialFunction[Result, Set[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv).toSet
  }

  def multiBulkDataResultToLinkedSet[T](conv: BinaryConverter[T]): PartialFunction[Result, Set[T]] = {
    case MultiBulkDataResult(results) => mutable.LinkedHashSet(filterEmptyAndMap(results, conv):_*)
  }

  def multiBulkDataResultToMap[T](keys: Seq[String], conv: BinaryConverter[T]): PartialFunction[Result, Map[String,T]] = {
    case BulkDataResult(data) => data match {
      case None => Map()
      case Some(d) => Map(keys.head -> conv.read(d))
    }

    case MultiBulkDataResult(results) =>
      keys.zip(results).filter {
        case (k, BulkDataResult(Some(_))) => true
        case (k, BulkDataResult(None)) => false
      }.map { kv => kv._1 -> conv.read(kv._2.data.get) }.toMap
  }

  def multiBulkDataResultToMap[K, V](implicit keyConv: BinaryConverter[K], valueConv: BinaryConverter[V]): PartialFunction[Result, Map[K,V]] = {
    case MultiBulkDataResult(List()) => Map()
    case MultiBulkDataResult(results) =>
      var take = false
      results.zip(results.tail).filter { (_) => take = !take; take }.collect {
        case (BulkDataResult(Some(keyData)), BulkDataResult(Some(data))) => keyData -> data
      }.map { kv => keyConv.read(kv._1) -> valueConv.read(kv._2)}.toMap
    case unknown =>
      throw UnsupportedResponseException("Unsupported response type: " + unknown)
  }

  def bulkResultToSet[T](conv: BinaryConverter[T]): PartialFunction[Result, Set[T]] = {
    case MultiBulkDataResult(results) => filterEmptyAndMap(results, conv).toSet
    case BulkDataResult(Some(v)) => Set(conv.read(v))
  }


  private[this] def filterEmptyAndMap[T](r: Seq[BulkDataResult], conv: BinaryConverter[T]) = r.collect {
    case BulkDataResult(Some(data)) => conv.read(data)
  }

}

