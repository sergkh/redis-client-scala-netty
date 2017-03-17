package com.fotolog.redis.commands

import com.fotolog.redis.utils.Options.Limit
import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions
import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions.{NX, XX}
import com.fotolog.redis.{RedisException, TestClient}
import org.scalatest.{FlatSpec, Matchers}


class SortedSetCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A zadd method" should "correctly add a value to a set" in {
    client.zadd("key-zadd", (1.0F, "one")) shouldEqual 1
    client.zadd("key-zadd", (1.0F, "one"), (2.5F, "two")) shouldEqual 1
    client.zadd("key-zadd", (1.0F, "one")) shouldEqual 0
    client.zadd("key-zadd", ZaddOptions(Some(XX)), (1.0F, "new-member")) shouldEqual 0
    client.zadd("key-zadd", ZaddOptions(Some(NX)), (2.0F, "one")) shouldEqual 0

    client.zadd("key-zadd", ZaddOptions(None, true), (2.0F, "one"), (4.0F, "two")) shouldEqual 2

    client.zadd("key-zadd", ZaddOptions(Some(XX), true), (5.0F, "one"), (1.0F, "new-member")) shouldEqual 1
    client.zadd("key-zadd", ZaddOptions(Some(NX), true), (5.0F, "one"), (1.0F, "new-member")) shouldEqual 1

    client.zadd("key-zadd", ZaddOptions(None, true), (5.0F, "one"), (1.0F, "new-member")) shouldEqual 0
    client.zadd("key-zadd", ZaddOptions(None, true), (1.0F, "new-member2")) shouldEqual 1

    the [RedisException] thrownBy {
      client.zadd("key-zadd", ZaddOptions(None, false, true), (2.0F, "two"), (3.0F, "three"))
    } should have message "ERR INCR option supports a single increment-element pair"
  }

  "A zcard" should "return the cardinality (number of elements) of the sorted set" in {
    client.zadd("zset-zcard", (1.0F, "one")) shouldEqual 1
    client.zadd("zset-zcard", (1.0F, "one"), (2.5F, "two")) shouldEqual 1
    client.zcard("zset-zcard") shouldEqual 2
    client.zcard("zset-zcar-noexist") shouldEqual 0
  }

  "A zcount" should "the number of elements in the sorted set at key with a score between min and max" in {
    client.zadd[String]("zset-zcount", (1.0F, "one"), (2.5F, "two"), (2.8F, "three")) shouldEqual 3
    client.zcount[String]("zset-zcount", 1, 3) shouldEqual 3
    client.zcount[String]("zset-zcount", 1, 2.5F) shouldEqual 2
    client.zcount[String]("zset-zcount", 1, 2.49F) shouldEqual 1
  }

  "A zincrby" should "increment the score of member" in {
    client.zadd[String]("zset-zincrby", (1.0F, "one"), (2.5F, "two"), (2.8F, "three")) shouldEqual 3
    client.zincrby[String]("zset-zincrby", 3, "one") shouldEqual 4.0F
  }

  "A zlexcount" should "return the number of elements in the specified score range" in {
    client.zadd[String]("zset-zlexcount", (0F, "a"), (0F, "b"), (0F, "c"), (0F, "d"), (0F, "e")) shouldEqual 5
    client.zlexcount[String]("zset-zlexcount", "[a", "[d") shouldEqual 4
  }

  "A zrange" should "returns the specified range of elements in the sorted set" in {
    client.zadd[String]("zset-zrange", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrange[String]("zset-zrange", 0, -1) shouldEqual Set("a", "b", "c")
    client.zrangeWithScores[String]("zset-zrange", 0, -1) shouldEqual Map("a" -> 1F, "b" -> 2F, "c" -> 3F)
  }

  "A zrangebylex" should "returns the specified range of elements in the sorted set by lex" in {
    client.zadd[String]("zset-zrangebylex", (1F, "a"), (1F, "b"), (1F, "c"), (1F, "d"), (1F, "e"), (1F, "f"), (1F, "g")) shouldEqual 7
    client.zrangeByLex[String]("zset-zrangebylex", "-", "[c") shouldEqual Set("a", "b", "c")
    client.zrangeByLex[String]("zset-zrangebylex", "-", "(c") shouldEqual Set("a", "b")
    client.zrangeByLex[String]("zset-zrangebylex", "[aaa", "(g") shouldEqual Set("b", "c", "d", "e", "f")
  }

  "A zrangebyscore" should "returns all the elements in the sorted set at key with a score between min and max" in {
    client.zadd[String]("zset-zrangebyscore", (1F, "a"), (1F, "b"), (1F, "c")) shouldEqual 3
    client.zrangeByScore[String]("zset-zrangebyscore", "-inf", "+inf") shouldEqual Set("a", "b", "c")
    client.zrangeByScore[String]("zset-zrangebyscore", "-inf", "+inf", Some(Limit(1, 2))) shouldEqual Set("b", "c")
    client.zrangeByScore[String]("zset-zrangebyscore", "-inf", "+inf", Some(Limit(1, 1))) shouldEqual Set("b")
    client.zrangeByScoreWithScores[String]("zset-zrangebyscore", "-inf", "+inf") shouldEqual Map("a" -> 1F, "b" -> 1F, "c" -> 1F)
  }

  "A zrank" should "returns all the elements in the sorted set at key with a score between min and max" in {
    client.zadd[String]("zset-zrank", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrank[String]("zset-zrank", "a") shouldEqual Some(0)
    client.zrank[String]("zset-zrank", "c") shouldEqual Some(2)
  }

  "A zrem" should "returns all the elements in the sorted set at key with a score between min and max" in {
    client.zadd[String]("zset-zrem", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrem[String]("zset-zrem", "a") shouldEqual 1
    client.zrange[String]("zset-zrem", 0, -1) shouldEqual Set("b", "c")
    client.zrem[String]("zset-zrem", "d") shouldEqual 0
    client.zrem[String]("zset-zrem", "c") shouldEqual 1
    client.zrange[String]("zset-zrem", 0, -1) shouldEqual Set("b")
  }

  "A zremrangebylex" should "removes all elements in the sorted set stored at key between the lexicographical range specified by min and max" in {
    client.zadd[String]("zset-zremrangebylex", (0F, "aaaa"), (0F, "b"), (0F, "c"), (0F, "d"), (0F, "e")) shouldEqual 5
    client.zadd[String]("zset-zremrangebylex", (0F, "foo"), (0F, "zap"), (0F, "zip"), (0F, "ALPHA"), (0F, "alpha")) shouldEqual 5
    client.zrange[String]("zset-zremrangebylex", 0, -1) shouldEqual Set("ALPHA", "aaaa", "alpha", "b", "c", "d", "e", "foo", "zap", "zip")
    client.zremRangeByLex("zset-zremrangebylex", "[alpha", "[omega") shouldEqual 6
    client.zrange[String]("zset-zremrangebylex", 0, -1) shouldEqual Set("ALPHA", "aaaa", "zap", "zip")
  }

  "A zremrangebyrank" should "removes all elements in the sorted set stored at key with rank between start and stop" in {
    client.zadd[String]("zset-zremrangebyrank", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zremRangeByRank("zset-zremrangebyrank", 0, 1) shouldEqual 2
    client.zrange[String]("zset-zremrangebyrank", 0, -1) shouldEqual Set("c")
  }

  "A zremrangebyscore" should "removes all elements in the sorted set stored at key with a score between min and max (inclusive)" in {
    client.zadd[String]("zset-zremrangebyscore", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zremRangeByScore("zset-zremrangebyscore", "-inf", "(2") shouldEqual 1
    client.zrange[String]("zset-zremrangebyscore", 0, -1) shouldEqual Set("b", "c")
  }

  "A zrevrange" should "returns the specified range of elements in the sorted set stored at key" in {
    client.zadd[String]("zset-zrevtange", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrevRange[String]("zset-zrevtange", 0, -1) shouldEqual Set("c", "b", "a")
    client.zrevRange[String]("zset-zrevtange", 2, 3) shouldEqual Set("a")
    client.zrevRange[String]("zset-zrevtange", -2, -1) shouldEqual Set("b", "a")
  }

  "A zrevrangebylex" should "returns the specified range of elements in the sorted set stored at key" in {
    client.zadd[String]("zset-zrevrangebylex", (1F, "a"), (1F, "b"), (1F, "c"), (1F, "d"), (1F, "e"), (1F, "f"), (1F, "g")) shouldEqual 7
    client.zrevRangeByLex[String]("zset-zrevrangebylex", "[c", "-") shouldEqual Set("c", "b", "a")
    client.zrevRangeByLex[String]("zset-zrevrangebylex", "(c", "-") shouldEqual Set("b", "a")
    client.zrevRangeByLex[String]("zset-zrevrangebylex", "(g", "[aaa") shouldEqual Set("f", "e", "d", "c", "b")
  }

  "A zrevrangebyscore" should "returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min)" in {
    client.zadd[String]("zset-zrevrangebyscore", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrevRangeByScore[String]("zset-zrevrangebyscore", "+inf", "-inf") shouldEqual Set("c", "b", "a")
    client.zrevRangeByScore[String]("zset-zrevrangebyscore", "2", "1") shouldEqual Set("b", "a")
    client.zrevRangeByScore[String]("zset-zrevrangebyscore", "2", "(1") shouldEqual Set("b")
    client.zrevRangeByScore[String]("zset-zrevrangebyscore", "(2", "(1") shouldEqual Set()
    client.zrevRangeByScoreWithScore[String]("zset-zrevrangebyscore", "+inf", "-inf") shouldEqual Map("c" -> 3F, "b" -> 2f, "a" -> 1f)
  }

  "A zrevrank" should "returns the rank of member in the sorted set stored at key, with the scores ordered from high to low" in {
    client.zadd[String]("zset-zrevrank", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrevRank[String]("zset-zrevrank", "a") shouldEqual Some(2)
    client.zrevRank[String]("zset-zrevrank", "d") shouldEqual None
  }

  "A zscore" should "returns the score of member in the sorted set at key" in {
    client.zadd[String]("zset-zscore", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zscore[String]("zset-zscore", "a") shouldEqual Some(1F)
  }

  "A zunionstore" should "computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination" in {
    client.zadd[String]("zset-zunionstore1", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zadd[String]("zset-zunionstore2", (1F, "a"), (2F, "b")) shouldEqual 2
    client.zunionstore("out", 2, Seq("zset-zunionstore1", "zset-zunionstore2"), Seq(2D, 3D)) shouldEqual 3
    client.zrangeWithScores[String]("out", 0, -1) shouldEqual Map("a" -> 5F, "c" -> 6F, "b" -> 10F)
  }
  
}
