package com.fotolog.redis.commands

import com.fotolog.redis.BinaryConverter
import com.fotolog.redis.connections._
import com.fotolog.redis.utils.Options.Limit
import com.fotolog.redis.utils.SortedSetOptions.{Agregation, SumAgregation, ZaddOptions}

import scala.collection.Set
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] trait SortedSetCommands extends ClientCommands {

  import com.fotolog.redis.commands.ClientCommands._

  def zaddAsync[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zadd(key, kvs.map(els => els._1 -> conv.write(els._2)), opts)).map(integerResultAsInt)
  }

  def zadd[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Int = await {
    zaddAsync(key, opts, kvs: _*)(conv)
  }

  def zaddAsync[T](key: String, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Zadd(key, kvs.map(els => els._1 -> conv.write(els._2)), ZaddOptions())).map(integerResultAsInt)

  def zadd[T](key: String, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Int = await {
    zaddAsync(key, kvs: _*)(conv)
  }

  def zcardAsync(key: String): Future[Int] = r.send(Zcard(key)).map(integerResultAsInt)

  def zcard(key: String): Int = await {
    zcardAsync(key)
  }

  def zcountAsync(key: String, min: Float, max: Float): Future[Int] = r.send(Zcount(key, min, max)).map(integerResultAsInt)

  def zcount[T](key: String, min: Float, max: Float): Int = await {
    zcountAsync(key, min, max)
  }

  def zincrbyAsync[T](key: String, increment: Float, member: T)(implicit conv: BinaryConverter[T]): Future[Float] = {
    r.send(Zincrby(key, increment, conv.write(member))).map(stringResultAsFloat)
  }

  def zincrby[T](key: String, increment: Float, member: T)(implicit conv: BinaryConverter[T]): Float = await {
    zincrbyAsync(key, increment, member)(conv)
  }

  def zinterstoreAsync = ???

  def zinterstore = ???

  def zlexcountAsync[T](key: String, min: T, max: T)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zlexcount(key, conv.write(min), conv.write(max))).map(integerResultAsInt)
  }

  def zlexcount[T](key: String, min: T, max: T)(implicit conv: BinaryConverter[T]): Int = await {
    zlexcountAsync(key, min, max)(conv)
  }

  def zrangeAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(Zrange(key, start, stop, false)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrange[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeAsync(key, start, stop)(conv)
  }

  def zrangeWithScoresAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(Zrange(key, start, stop, true)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrangeWithScores[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrangeWithScoresAsync(key, start, stop)(conv)
  }

  def zrangeByLexAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrangeByLex(key, min, max, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrangeByLex[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeByLexAsync(key, min, max, limit)(conv)
  }

  def zrangeByScoreAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrangeByScore(key, min, max, false, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrangeByScore[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeByScoreAsync(key, min, max, limit)(conv)
  }

  def zrangeByScoreWithScoresAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(ZrangeByScore(key, min, max, true, limit)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrangeByScoreWithScores[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrangeByScoreWithScoresAsync(key, min, max, limit)(conv)
  }

  def zrankAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Int]] = {
    r.send(Zrank(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.IntConverter))
  }

  def zrank[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Int] = await {
    zrankAsync(key, member)(conv)
  }

  def zremAsync[T](key: String, members: T*)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zrem(key, members.map(conv.write))).map(integerResultAsInt)
  }

  def zrem[T](key: String, members: T*)(implicit conv: BinaryConverter[T]): Int = await {
    zremAsync(key, members:_*)(conv)
  }

  def zremRangeByLexAsync(key: String, min: String, max: String): Future[Int] = {
    r.send(ZremRangeByLex(key, min, max)).map(integerResultAsInt)
  }

  def zremRangeByLex(key: String, min: String, max: String): Int = await {
    zremRangeByLexAsync(key, min, max)
  }

  def zremRangeByRankAsync(key: String, startRange: Int, stopRange: Int): Future[Int] = {
    r.send(ZremRangeByRank(key, startRange, stopRange)).map(integerResultAsInt)
  }

  def zremRangeByRank(key: String, startRange: Int, stopRange: Int): Int = await {
    zremRangeByRankAsync(key, startRange, stopRange)
  }

  def zremRangeByScoreAsync(key: String, minScore: String, maxScore: String): Future[Int] = {
    r.send(ZremRangeByScore(key, minScore, maxScore)).map(integerResultAsInt)
  }

  def zremRangeByScore(key: String, minScore: String, maxScore: String): Int = await {
    zremRangeByScoreAsync(key, minScore, maxScore)
  }

  def zrevRangeAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRange(key, start, stop)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRange[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeAsync(key, start, stop)
  }

  def zrevRangeByLexAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRangeByLex(key, start, stop, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRangeByLex[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeByLexAsync(key, start, stop, limit)
  }

  def zrevRangeByScoreAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRangeByScore(key, start, stop, limit, false)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRangeByScore[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeByScoreAsync(key, start, stop, limit)
  }

  def zrevRangeByScoreWithScoreAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(ZrevRangeByScore(key, start, stop, limit, true)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrevRangeByScoreWithScore[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrevRangeByScoreWithScoreAsync(key, start, stop, limit)(conv)
  }

  def zrevRankAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Int]] = {
    r.send(Zrevrank(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.IntConverter))
  }

  def zrevRank[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Int] = await {
    zrevRankAsync(key, member)(conv)
  }

  def zscoreAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Float]] = {
    r.send(Zscore(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.FloatConverter))
  }

  def zscore[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Float] = await {
    zscoreAsync(key, member)(conv)
  }

  def zunionstoreAsync(dstZsetName: String, zsetNumber: Int, srcZets: Seq[String], weights: Seq[Double] = Nil, agregationFunc: Agregation = SumAgregation): Future[Int] = {
    r.send(Zunionstore(dstZsetName, zsetNumber, srcZets, weights, agregationFunc)).map(integerResultAsInt)
  }

  def zunionstore(dstZsetName: String, zsetNumber: Int, srcZets: Seq[String], weights: Seq[Double] = Nil, agregationFunc: Agregation = SumAgregation): Int = await {
    zunionstoreAsync(dstZsetName, zsetNumber, srcZets, weights, agregationFunc)
  }

}
