package com.fotolog.redis.memCommands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 07.08.16.
  */
class MemSetCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A srem" should "correctly remove ele from set" in {
    memClient.sadd("key", "Hello", "World") shouldEqual 2
    memClient.srem[String]("key", "Hello1") shouldBe false
    memClient.srem[String]("key", "Hello") shouldBe true
    memClient.smembers[String]("key") shouldEqual Set("World")
  }

  "A scard" should "correctly show set's size" in {
    memClient.sadd("key", "Hello", "World") shouldEqual 2
    memClient.scard("key") shouldEqual 2
    memClient.scard("key1") shouldEqual 0
  }

  "A smove" should "move elem from source set to destination set" in {
    memClient.sadd("key", "Hello") shouldEqual 1
    memClient.sadd("key1","World") shouldEqual 1
    memClient.smove("key", "key1", "Hello") shouldBe true
    memClient.smembers[String]("key") shouldEqual Set()
    memClient.smembers[String]("key1") shouldEqual Set("World", "Hello")
  }

  "A sinter/sinterstore" should "return intersection of sets and store it to another set" in {
    memClient.sadd("key", 3, 4, 5, 6, 7) shouldEqual 5
    memClient.sadd("key1", 1, 2, 3, 4) shouldEqual 4
    memClient.sadd("key2", 8, 9, 10, 4, 3) shouldEqual 5
    memClient.sinter[Int]("key", "key1", "key2") shouldEqual Set(3, 4)

    memClient.sadd("key3", 3, 4, 5, 6, 7) shouldEqual 5
    memClient.sinterstore("key3", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key3") shouldEqual Set(3, 4)

    memClient.sinterstore("key4", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key4") shouldEqual Set(3, 4)
  }

  "A sdiff/sdiffstore" should "return difference of sets and store it to another set" in {
    memClient.sadd("key", 3, 4, 5, 6, 7) shouldEqual 5
    memClient.sadd("key1", 1, 2, 3, 4) shouldEqual 4
    memClient.sadd("key2", 8, 9, 10, 4, 3) shouldEqual 5
    memClient.sdiff[Int]("key", "key1", "key2") shouldEqual Set(5, 6, 7)

    memClient.sadd("key3", 3, 4, 5, 6, 7) shouldEqual 5
    memClient.sdiffstore("key3", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key3") shouldEqual Set(5, 6, 7)

    memClient.sdiffstore("key4", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key4") shouldEqual Set(5, 6, 7)
  }


}
