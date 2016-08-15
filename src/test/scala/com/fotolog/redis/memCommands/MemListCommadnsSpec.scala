package com.fotolog.redis.memCommands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 08.08.16.
  */
class MemListCommadnsSpec extends FlatSpec with Matchers with TestClient {

  def iter(key: String, n: Int) = {

      for (i <- 1 to n) {
        memClient.rpush(key, i) shouldEqual i
      }
  }

  "A rpush" should "add elem to tail of the list" in {
    memClient.rpush("key", 1) shouldEqual 1
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1)
    memClient.rpush("key", 1) shouldEqual 2
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 1)
  }

  "A lpush" should "add elem to the head of list" in {
    memClient.lpush("key", 2) shouldEqual 1
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(2)
    memClient.lpush("key", 2) shouldEqual 2
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(2, 2)
  }

  "A lrange/llen" should "returns the specified elements of the list" in {

    iter("key", 4)

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

    iter("key1", 10)

    memClient.lrange[Int]("key1", 0, -1) shouldEqual List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    memClient.llen("key") shouldEqual 4
  }

  "A ltrim" should "trim an existing list to specified range of elem" in {

    iter("key", 5)

    memClient.ltrim("key", -5, -1) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3, 4, 5)

    memClient.ltrim("key", -1, 2) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(5, 1, 2, 3)

    memClient.ltrim("key", -1, 0) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(3, 5)

    memClient.ltrim("key", 5, 9) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil

    iter("key", 4)

    memClient.ltrim("key", 0, 2) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual List(1, 2, 3)

    memClient.ltrim("key", 2, 1) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil

    iter("key", 4)

    memClient.ltrim("key", -2, -3) shouldBe true
    memClient.lrange[Int]("key", 0, 5) shouldEqual Nil
  }

  "A lindex" should "return value at index in list stored at key" in {
    iter("key", 3)
    memClient.lindex[Int]("key", 0) shouldEqual Some(1)
    memClient.lindex[Int]("key", -1) shouldEqual Some(3)
    memClient.lindex[Int]("key", 3) shouldEqual None
  }

  "A lset" should "update value on existing index" in {

    iter("key", 3)

    memClient.lset[Int]("key", 1, 4) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 4, 3)

    memClient.lset[Int]("key", -1, 5) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 4, 5)

    memClient.lset[Int]("key", 0, 3) shouldBe true
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(3, 4, 5)
  }

  "A lpop" should "correctly remove and return removed elem from head of a list" in {

    iter("key", 4)

    memClient.lpop[Int]("key") shouldEqual Some(1)
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(2, 3, 4)

    iter("key1", 1)

    memClient.lpop[Int]("key1") shouldEqual Some(1)
    memClient.lrange[Int]("key1", 0, -1) shouldEqual Nil

    memClient.lpop[Int]("key2") shouldEqual None

  }

  "A rpop" should "correctly remove and return removed elem from tail of a list" in {

    iter("key", 4)

    memClient.rpop[Int]("key") shouldEqual Some(4)
    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 2, 3)

    iter("key1", 1)

    memClient.rpop[Int]("key1") shouldEqual Some(1)
    memClient.lrange[Int]("key1", 0, -1) shouldEqual Nil

    memClient.rpop[Int]("key2") shouldEqual None
  }

  /*"A lrem" should "correctly remove from list" in {

    iter("key", 4)
    memClient.rpush("key", 4) shouldEqual 5
    memClient.lpush("key", 2) shouldEqual 6
    memClient.lpush("key", 1) shouldEqual 7

    memClient.lrange[Int]("key", 0, -1) shouldEqual List(1, 2, 1, 2, 3, 4, 4)

  }*/

}
