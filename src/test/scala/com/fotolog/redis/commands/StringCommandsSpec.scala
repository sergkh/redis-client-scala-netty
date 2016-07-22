package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */
class StringCommandsSpec extends FlatSpec with Matchers with TestClient {
  val c = createClient()
  c.flushall

  "A append method" should "return Int result" in {
    c.exists("key") shouldBe false
    c.append("key", "Hello") shouldEqual 5
    c.append("key", " World") shouldEqual 11
  }

  "A set method" should "return Boolean result" in {
    c.set("key", 3) shouldBe true
    c.set("key1", "Hello") shouldBe true
    c.set("key", 14) shouldBe true
  }

  "A get method" should "return Option[T]" in {
    c.get[Int]("key1") shouldEqual None
    c.set("key1", "String Value") shouldBe true
    c.get[String]("key1") shouldEqual Some("String Value")
  }

  "A set(keys*) method" should "return Boolean result" in {
    c.set[String](("key", "Hello"), ("key1", "World")) shouldBe true
    c.get[String]("key") shouldEqual Some("Hello")
    c.set[Int](("key", 13), ("key1", 15)) shouldBe true
    c.get[Int]("key") shouldEqual Some(13)
  }

  "A setNx method" should "return Boolean result" in {
    c.setNx("key", "Str") shouldBe true
    c.setNx("key", "Str") shouldBe false
  }

  "A setNx(keys) method" should "return Boolean result" in {
    c.setNx[String](("key", "Hello"), ("key1", "World")) shouldBe true
    c.setNx[String](("key", "Hello"), ("key1", "String")) shouldBe false
  }

  "A setXx method" should "return Boolean result" in {
    c.set("key", 15) shouldBe true
    c.setXx("key", 15, 2) shouldBe true
    c.get[Int]("key") shouldEqual Some(15)
    c.ttl("key") shouldEqual 2
    Thread.sleep(2000)
    c.ttl("key") shouldEqual -2
    c.get[Int]("key") shouldEqual None
  }

  "A get(keys*) method" should "return Seq(Option[T])" in {
    c.set[Int](("key", 1), ("key1", 2)) shouldBe true
    c.get[Int]("key", "key1") shouldEqual Seq(Some(1), Some(2))
  }

  "A mget method" should "return Map[String, T]" in {
    c.set[Int](("key", 1), ("key1", 2)) shouldBe true
    c.mget[Int]("key", "key1") shouldEqual Map[String, Int]("key" -> 1, "key1" -> 2)
    c.mget[Int]("key", "key1", "key2") shouldEqual Map[String, Int]("key" -> 1, "key1" -> 2)
  }

  "A getrange method" should "return Option[T]" in {
    c.set("key", "This is test string") shouldBe true
    c.getrange[String]("key", 0, 4) shouldEqual Some("This ")
    c.getrange[String]("key", 0, -1) shouldEqual Some("This is test string")
    c.getrange[String]("key", -4, -1) shouldEqual Some("ring")
  }

  "A getset method" should "return Option[T]" in {
    c.set("key", "Hello") shouldBe true
    c.getset[String]("key", "World") shouldEqual Some("Hello")
    c.get[String]("key") shouldEqual Some("World")
  }

}
