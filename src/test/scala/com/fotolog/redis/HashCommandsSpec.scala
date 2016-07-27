package com.fotolog.redis

/**
  * Created by faiaz on 12.07.16.
  */
import org.scalatest.{FlatSpec, Matchers}

class HashCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A hset" should "allow to put and get sets" in {
    client.hset("key0", "f0", "value0") shouldBe true
    client.hset("key0", "f0", "value1") shouldBe true
    client.hget[String]("key0", "f0") shouldEqual Some("value1")
    client.hget[String]("key0", "f1") shouldEqual None
  }

  "A hdel" should "correctly delete results" in {
    client.hset("key0", "f0", "value0") shouldBe true
    client.hdel("key0", "f0") shouldBe true
    client.hdel("key0", "f0") shouldBe false
  }

  "A hmset" should "allow to set and retrieve multiple values" in {
    client.hmset("key1", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hmget[String]("key1", "f0", "f1") shouldEqual Map[String, String]("f0" -> "Hello", "f1" -> "World")
  }

  "A hincr" should "correctly increment ints" in {
    client.hset("key1", "f2", 25)
    client.hincr("key1", "f2", 5) shouldEqual 30
    client.hincr("key1", "f2", -6) shouldEqual 24
    client.hincr("key1", "f2") shouldEqual 25
  }

  "A hexists" should "return Boolean result" in {
    client.hset("key", "f0", "xx") shouldBe true
    client.hexists("key", "f0") shouldBe true
    client.hexists("key", "f4") shouldBe false
  }

  "A hlen" should "return length of a hash set" in {
    client.hmset("key1", ("f0", "Hello"), ("f1", "World")) shouldBe true
    client.hlen("key1") shouldEqual 2
  }

  "A keys" should "return keys sequence" in {
    client.hmset("key1", ("f1", "Hello"), ("f2", "World"), ("f3", "World")) shouldBe true
    client.hkeys("key1") shouldEqual Seq[String]("f1", "f2", "f3")
  }

  "A hvals" should "return values sequence" in {
    client.hmset("key2", ("f0", 13), ("f1", 15)) shouldBe true
    client.hmset("key1", ("f1", "Hello"), ("f2", "World"), ("f3", "!")) shouldBe true

    client.hvals[String]("key1") shouldEqual Seq[String]("Hello", "World", "!")
    client.hvals[Int]("key2") shouldEqual Seq[Int](13, 15)
  }

  "A hgetAll" should "return all stored values" in {
    client.hmset("key1", ("f1", "Hello"), ("f2", "World"), ("f3", "!")) shouldBe true
    client.hmset("key2", ("f0", 13), ("f1", 15)) shouldBe true

    client.hgetall[String]("key1") shouldEqual Map[String, String](("f1", "Hello"), ("f2", "World"), ("f3", "!"))
    client.hgetall[Int]("key2") shouldEqual Map[String, Int](("f0", 13), ("f1", 15))
  }

  // TODO: uncomment when will be able to run tests against Redis 3.2
  "A hstrlen" should "return length of a value of hash field" ignore {
    client.hmset("key1", ("f1", "Hi"), ("f2", "World")) shouldBe true
    client.hstrlen("key1", "f1") shouldEqual 2
    client.hstrlen("key1", "f2") shouldEqual 5
  }

  "A hsetnx" should "return Boolean result" in {
    client.hset("key3", "f0", "Hello") shouldBe true
    client.hsetnx("key3", "f0", "Hello Vesteros") shouldBe false
    client.hsetnx("key3", "f3", "field3") shouldBe true
    client.hget[String]("key3", "f3") shouldEqual Some("field3")
  }

  "A hincrbyfloat" should "correctly increment and decrement hash value" in {
    client.hmset("key1", ("f2", 25)) shouldBe true
    client.hincrbyfloat[Double]("key1", "f2", 25.0) shouldEqual 50.0
    client.hincrbyfloat[Double]("key1", "f2", -24.0) shouldEqual 26.0
    client.hincrbyfloat[Double]("key1", "f2") shouldEqual 27.0

    client.hincrbyfloat[Double]("key1", "f1", 30.0) shouldEqual 30.0
  }
}
