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
    memClient.sadd("key1", "World") shouldEqual 1
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

  "A sunion/sunionstore" should "retunr union of sets" in {
    memClient.sadd("key", 1, 2, 3) shouldEqual 3
    memClient.sadd("key1", 4, 5, 6) shouldEqual 3
    memClient.sadd("key2", 7, 8, 9) shouldEqual 3
    memClient.sunion[Int]("key", "key1", "key2") shouldEqual Set(1, 2, 3, 4, 5, 6, 7, 8, 9)

    memClient.sunionstore[Int]("key4", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key4") shouldEqual Set(1, 2, 3, 4, 5, 6, 7, 8, 9)

    memClient.sadd("key5", 1, 2, 3) shouldEqual 3
    memClient.sunionstore[Int]("key5", "key", "key1", "key2") shouldEqual 1
    memClient.smembers[Int]("key5") shouldEqual Set(1, 2, 3, 4, 5, 6, 7, 8, 9)
  }

  "A srandmember" should "correctly retern random elem from set" in {
    memClient.sadd("key", "hello", "beaty", "world") shouldEqual 3

    val res = memClient.srandmember[String]("key")
    res should contain oneOf("hello", "beaty", "world")
  }

  "A spop " should "randomly remove and return some value from set" in {
    memClient.sadd("key-spop", "one", "two", "three") shouldEqual 3


    val a = memClient.spop[String]("key-spop")
    a should contain oneOf("one", "two", "three")

    a match {
      case Some("one") => memClient.smembers[String]("key-spop") shouldEqual Set("two", "three")
      case Some("two") => memClient.smembers[String]("key-spop") shouldEqual Set("one", "three")
      case Some("three") => memClient.smembers[String]("key-spop") shouldEqual Set("one", "two")
      case _ => throw new MatchError("no matches in spop method test")
    }

    memClient.spop[String]("key-spop") should contain oneOf("one", "two", "three")
    memClient.spop[String]("key-spop") should contain oneOf("one", "two", "three")
    memClient.spop[String]("key-spop") shouldEqual None
  }

}
