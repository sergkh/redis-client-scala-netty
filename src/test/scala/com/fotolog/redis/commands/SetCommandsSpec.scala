package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */
class SetCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A sadd method" should "correctly add value to set" in {
    client.sadd("key", 4) shouldEqual 1
    client.sadd("key", "Hello") shouldEqual 1
    client.sadd("key", 4) shouldEqual 0
  }

  "A smembers" should "correctly show set's member" in {
    client.sadd("key", "one")
    client.sadd("key", "two")
    client.smembers[String]("key") shouldEqual Set("one", "two")
  }

  "A srem method" should "correctly remove value from set" in {
    client.sadd("key", "one")
    client.sadd("key", "two")
    client.srem("key", "one") shouldBe true
    client.srem("key", "four") shouldBe false
  }

  "A spop method" should "randomly popup some value from set" in {
    client.sadd("key", "one")
    client.sadd("key", "two")
    client.sadd("key", "three")

    val a = client.spop[String]("key")
    a should contain oneOf("one", "two", "three")

    a match {
      case Some("one") => client.smembers[String]("key") shouldEqual Set("two", "three")
      case Some("two") => client.smembers[String]("key") shouldEqual Set("one", "three")
      case Some("three") => client.smembers[String]("key") shouldEqual Set("one", "two")
      case _ => throw new MatchError("no mathces in spop method test")
    }
  }

  "A smove method" should "move value from set to another set" in {
    client.sadd("key", "str1")
    client.sadd("key1","str2")
    client.smove[String]("key", "key1", "str1") shouldBe true
    client.smembers[String]("key1") shouldEqual Set("str2", "str1")
    client.smembers[String]("key") shouldEqual Set()
  }

  "A scard" should "get the number of element in set" in {
    client.sadd("key", 4) shouldEqual 1
    client.sadd("key", 8) shouldEqual 1
    client.sadd("key", 1) shouldEqual 1
    client.scard("key") shouldEqual 3
  }

  "A sismember" should " return if value is a member of the set" in {
    client.sadd("key", 4) shouldEqual 1
    client.sismember("key", 4) shouldBe true
  }
}
