package com.fotolog.redis.commands

/**
  * Created by faiaz on 12.07.16.
  */
import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

class HashCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A hset" should "setting value in hash " in {
    client.hset("key", "f0", "value0") shouldBe true
    client.hset("key", "f0", "value1") shouldBe false
  }

  "A hget" should "getting value from hash" in {
    client.hset("key", "f0", "value0") shouldBe true
    client.hget[String]("key", "f0") shouldEqual Some("value0")
    client.hget[String]("key", "f1") shouldEqual None
  }

  "A hdel" should "delete value from hash" in {
    client.hset("key", "f0", "value0") shouldBe true
    client.hdel("key", "f0") shouldBe true
    client.hdel("key", "f0") shouldBe false
  }

  "A hmset/hmget" should "correctly set and get" in {
    client.hmset("key", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hmget[String]("key", "f0", "f1") shouldEqual Map[String, String]("f0" -> "Hello", "f1" -> "World")
  }

  "A hincr" should "increase value in hash" in {
    client.hset("key", "f2", 25)
    client.hincr("key", "f2", 5) shouldEqual 30
    client.hincr("key", "f2", -6) shouldEqual 24
    client.hincr("key", "f2") shouldEqual 25
  }

  "A hexists" should "check for the presence in a hash" in {
    client.hset("key", "f0", "value0") shouldBe true
    client.hexists("key", "f0") shouldBe true
    client.hexists("key", "f4") shouldBe false
  }

  "A hlen" should "return length of the field in a hash" in {
    client.hmset("key", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hlen("key") shouldEqual 2
  }

  "A keys" should "sequence of keys" in {
    client.hmset("key", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hkeys("key") shouldEqual Seq[String]("f0", "f1")
  }

  "A hvals" should "return Seq[String] result" in {
    client.hmset("key", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hmset("key2", ("f0", 13), ("f1", 15))
    client.hvals[String]("key") shouldEqual Seq[String]("Hello", "World")
    client.hvals[Int]("key2") shouldEqual Seq[Int](13, 15)
  }

  "A hgetAll" should "return Map[String, T] result" in {
    client.hmset("key", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hmset("key2", ("f0", 13), ("f1", 15))
    client.hgetall[String]("key") shouldEqual Map[String, String](("f0", "Hello"), ("f1", "World"))
    client.hgetall[Int]("key2") shouldEqual Map[String, Int](("f0", 13), ("f1", 15))
  }

  "A hstrlen" should "return Int result" in {
    client.hset("key", "f0", "Hello")
    client.hstrlen("key", "f0") shouldEqual 5
  }

  "A hsetnx" should "return Boolean result" in {
    client.hset("key", "f0", "Hello") shouldBe true
    client.hsetnx("key", "f0", "Hello Vesteros") shouldBe false
    client.hsetnx("key3", "f3", "field3") shouldBe true
    client.hget[String]("key3", "f3") shouldEqual Some("field3")
  }

  "A hincrbyfloat" should "return Double result" in {
    client.hset("key", "f0", 25.0)
    client.hincrbyfloat[Double]("key", "f0", 25.0) shouldEqual 50.0
    client.hincrbyfloat[Double]("key", "f0", -24.0) shouldEqual 26.0
    client.hincrbyfloat[Double]("key", "f0") shouldEqual 27.0
  }
}


