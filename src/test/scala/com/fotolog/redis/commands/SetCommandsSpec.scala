package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */
class SetCommandsSpec extends FlatSpec with Matchers with TestClient {
  val c = createClient()
  c.flushall

  "A sadd method" should "return Int result" in {
    c.sadd("key", 4) shouldEqual 1
    c.sadd("key", "Hello") shouldEqual 1
    c.sadd("key", 4) shouldEqual 0
  }

  "A smembers" should "return Set[T] result" in {
    c.sadd("key", "one")
    c.sadd("key", "two")
    c.smembers[String]("key") shouldEqual Set("one", "two")
  }

  "A srem method" should "return Boolean result" in {
    c.sadd("key", "one")
    c.sadd("key", "two")
    c.srem("key", "one") shouldBe true
    c.srem("key", "four") shouldBe false
  }

  "A spop method" should "return Option[T] result" in {
    c.sadd("key", "one")
    c.sadd("key", "two")
    c.sadd("key", "three")

    val a = c.spop[String]("key")
    a should contain oneOf("one", "two", "three")

    a match {
      case Some("one") => c.smembers[String]("key") shouldEqual Set("two", "three")
      case Some("two") => c.smembers[String]("key") shouldEqual Set("one", "three")
      case Some("three") => c.smembers[String]("key") shouldEqual Set("one", "two")
      case _ => throw new MatchError("no mathces in spop method test")
    }
  }
}
