package com.fotolog.redis.commands

import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions
import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions.{INCR, NX, XX}
import com.fotolog.redis.{RedisException, TestClient}
import org.scalatest.{FlatSpec, Matchers}


class SortedSetCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A zadd method" should "correctly add a value to a set" in {
    client.zadd("key-sadd", (1.0F, "one")) shouldEqual 1
    client.zadd("key-sadd", (1.0F, "one"), (2.5F, "two")) shouldEqual 1
    client.zadd("key-sadd", (1.0F, "one")) shouldEqual 0
    client.zadd("key-sadd", ZaddOptions(Some(XX)), (1.0F, "new-member")) shouldEqual 0
    client.zadd("key-sadd", ZaddOptions(Some(NX)), (2.0F, "one")) shouldEqual 0 //TODO: retrieve and check score

    the [RedisException] thrownBy {
      client.zadd("key-sadd", ZaddOptions(None, None, Some(INCR)), (2.0F, "two"), (3.0F, "three"))
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

  "A zrange" should "Returns the specified range of elements in the sorted set" in {
    client.zadd[String]("zset-zrange", (1F, "a"), (2F, "b"), (3F, "c")) shouldEqual 3
    client.zrange[String]("zset-zrange", 0, -1) shouldEqual Set("a", "b", "c")
    client.zrangeWithScores[String]("zset-zrange", 0, -1) shouldEqual Map("a" -> 1F, "b" -> 2F, "c" -> 3F)
  }

}
