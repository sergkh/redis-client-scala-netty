package com.fotolog.redis.commands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 22.07.16.
  */

class StringCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A append method" should "return Int result" in {
    client.append("key", "Hello") shouldEqual 5
    client.append("key", " World") shouldEqual 11
  }

  "A set method" should "return Boolean result" in {
    client.set("key", 3) shouldBe true
    client.set("key1", "Hello") shouldBe true
    client.set("key", 14) shouldBe true
  }

  "A get method" should "return Option[T]" in {
    client.get[Int]("key1") shouldEqual None
    client.set("key1", "String Value") shouldBe true
    client.get[String]("key1") shouldEqual Some("String Value")
  }

  "A set(keys*) method" should "return Boolean result" in {
    client.set[String](("key", "Hello"), ("key1", "World")) shouldBe true
    client.get[String]("key") shouldEqual Some("Hello")
    client.set[Int](("key", 13), ("key1", 15)) shouldBe true
    client.get[Int]("key") shouldEqual Some(13)
  }

  "A setNx method" should "return Boolean result" in {
    client.setNx("key", "Str") shouldBe true
    client.setNx("key", "Str") shouldBe false
  }

  "A setNx(keys) method" should "return Boolean result" in {
    client.setNx[String](("key", "Hello"), ("key1", "World")) shouldBe true
    client.setNx[String](("key", "Hello"), ("key1", "String")) shouldBe false
  }

  "A setXx method" should "return Boolean result" in {
    client.set("key", 15) shouldBe true
    client.setXx("key", 15, 2) shouldBe true
    client.get[Int]("key") shouldEqual Some(15)
    client.ttl("key") shouldEqual 2

      Thread.sleep(2000)

    client.ttl("key") shouldEqual -2
    client.get[Int]("key") shouldEqual None
  }

  "A get(keys*) method" should "return Seq(Option[T])" in {
    client.set[Int](("key", 1), ("key1", 2)) shouldBe true
    client.get[Int]("key", "key1") shouldEqual Seq(Some(1), Some(2))
  }

  "A mget method" should "return Map[String, T]" in {
    client.set[Int](("key", 1), ("key1", 2)) shouldBe true
    client.mget[Int]("key", "key1") shouldEqual Map[String, Int]("key" -> 1, "key1" -> 2)
    client.mget[Int]("key", "key1", "key2") shouldEqual Map[String, Int]("key" -> 1, "key1" -> 2)
  }

  "A getrange method" should "return Option[T]" in {
    client.set("key", "This is test string") shouldBe true
    client.getrange[String]("key", 0, 4) shouldEqual Some("This ")
    client.getrange[String]("key", 0, -1) shouldEqual Some("This is test string")
    client.getrange[String]("key", -4, -1) shouldEqual Some("ring")
    client.getrange[String]("key", 5, 9) shouldEqual Some("is te")
  }

  "A getset method" should "return Option[T]" in {
    client.set("key", "Hello") shouldBe true
    client.getset[String]("key", "World") shouldEqual Some("Hello")
    client.get[String]("key") shouldEqual Some("World")
  }

}
