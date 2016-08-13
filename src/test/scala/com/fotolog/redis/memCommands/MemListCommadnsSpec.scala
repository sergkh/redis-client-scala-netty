package com.fotolog.redis.memCommands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 08.08.16.
  */
class MemListCommadnsSpec extends FlatSpec with Matchers with TestClient {

  def iter(n: Int) = {
    for ( i <- 1 to n ) {
      memClient.rpush("key", i) shouldEqual i
    }
  }

  "A rpush" should "add elem to tail of the list" in {
    memClient.rpush("key", "hello") shouldEqual 1
    memClient.rpush("key", "hello") shouldEqual 2
  }

  "A lpush" should "add elem to the head of list" in {
    memClient.lpush("key", "Hello") shouldEqual 1
    memClient.lpush("key", "Hello") shouldEqual 2
  }

  "A lrange/llen" should "returns the specified elements of the list" in {

    iter(4)

    memClient.lrange[Int]("key", -100, -4) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", -4, -2) shouldEqual List(1, 2, 3)
    memClient.lrange[Int]("key", -4, 0) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", -1, -1) shouldEqual List(4)
    memClient.lrange[Int]("key", -3, 0) shouldEqual List(2, 3, 4, 1)
    memClient.lrange[Int]("key", 0, 4) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", 1, 3) shouldEqual List(2, 3, 4)
    memClient.lrange[Int]("key", 3, 100) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", 3, 3) shouldEqual List(4)
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 2, 3, 4)
    memClient.lrange[Int]("key", 5, 4) shouldEqual Nil
    memClient.lrange[Int]("key", -3, -4) shouldEqual Nil

    memClient.llen("key") shouldEqual 4
  }

  "A ltrim" should "trim an existing list to specified range of elem" in {

    iter(5)

    memClient.ltrim("key", -5, -1) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3, 4, 5)

    memClient.ltrim("key", -1, 2) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(5, 1, 2, 3)

    memClient.ltrim("key", -1, 0) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(3, 5)

    memClient.ltrim("key", 5, 9) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil

    iter(4)

    memClient.ltrim("key", 0, 2) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3)

    memClient.ltrim("key", 2, 1) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil

    iter(4)

    memClient.ltrim("key", -2, -3) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil
  }

  "A lindex" should "return value at index in list stored at key" in {
    iter(3)
    memClient.lindex[Int]("key", 0) shouldEqual Some(1)
    memClient.lindex[Int]("key", -1) shouldEqual Some(3)
    memClient.lindex[Int]("key", 3) shouldEqual None
  }

  "A lset" should "update value on existing index" in {

    iter(3)

    memClient.lset[Int]("key", 1, 4) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 4, 3)

    memClient.lset[Int]("key", -1, 5) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 4, 5)

    memClient.lset[Int]("key", 0, 3) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(3, 4, 5)
  }
}
