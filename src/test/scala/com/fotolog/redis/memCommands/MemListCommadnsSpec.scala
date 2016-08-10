package com.fotolog.redis.memCommands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 08.08.16.
  */
class MemListCommadnsSpec extends FlatSpec with Matchers with TestClient {

  "A rpush" should "add elem to tail of the list" in {
    memClient.rpush("key", "hello") shouldEqual 1
    memClient.rpush("key", "hello") shouldEqual 2
  }

  "A lpush" should "add elem to the head of list" in {
    memClient.lpush("key", "Hello") shouldEqual 1
    memClient.lpush("key", "Hello") shouldEqual 2
  }

  "A lrange/llen" should "returns the specified elements of the list" in {

    memClient.rpush("key", 1) shouldEqual 1
    memClient.rpush("key", 2) shouldEqual 2
    memClient.rpush("key", 3) shouldEqual 3
    memClient.rpush("key", 4) shouldEqual 4

    memClient.lrange[Int]("key", 5, 8) shouldEqual Nil
    memClient.lrange[Int]("key", 1, -2) shouldEqual Nil
    memClient.lrange[Int]("key", 0, 0) shouldEqual List(1)
    memClient.lrange[Int]("key", -100, 100) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", -4, -2) shouldEqual List(1, 2, 3)
    memClient.lrange[Int]("key", -3, 0) shouldEqual List(2, 3, 4, 1)

    memClient.llen("key") shouldEqual 4
  }

  "A ltrim" should "trim an existing list to specified range of elem" in {
    memClient.rpush("key", 1) shouldEqual 1
    memClient.rpush("key", 2) shouldEqual 2
    memClient.rpush("key", 3) shouldEqual 3
    memClient.rpush("key", 4) shouldEqual 4
    memClient.rpush("key", 5) shouldEqual 5


    memClient.ltrim("key", -5, -1) shouldEqual true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3, 4, 5)

    memClient.ltrim("key", -1, 2) shouldEqual true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(5, 1, 2, 3)

    memClient.ltrim("key", 1, -1)
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3)
  }
}
