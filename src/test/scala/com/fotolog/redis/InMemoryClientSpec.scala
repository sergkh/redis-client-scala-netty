package com.fotolog.redis

import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions
import com.fotolog.redis.utils.SortedSetOptions.ZaddOptions.{NX, XX, NO}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Will be merged with RedisClientTest when in-memory client will support
 * full set of operations.
 */
class InMemoryClientSpec extends FlatSpec with Matchers with TestClient {

  override val client = createInMemoryClient

  "A In Memory Client" should "respond to ping" in {
    client.ping() shouldBe true
  }

  it should "support get, set, exists and type operations" in {
    client.exists("key_foo") shouldBe false
    client.set("key_foo", "bar", 2592000) shouldBe true

    client.exists("key_foo") shouldBe true

    client.get[String]("key_foo").get shouldEqual "bar"
    client.keytype("key_foo") shouldEqual KeyType.String

    // keys test
    client.set("key_boo", "baz") shouldBe true
    client.set("key_baz", "bar") shouldBe true
    client.set("key_buzzword", "bar") shouldBe true

    client.keys("key_?oo") shouldEqual Set("key_foo", "key_boo")
    client.keys("*") shouldEqual Set("key_foo", "key_boo", "key_baz", "key_buzzword")
    client.keys("???????") shouldEqual Set("key_foo", "key_boo", "key_baz")
    client.keys("*b*") shouldEqual Set("key_baz", "key_buzzword", "key_boo")

    client.del("key_foo") shouldEqual 1
    client.exists("key_foo") shouldBe false

    client.del("key_foo") shouldEqual 0

    // rename
    client.rename("key_baz", "key_rbaz") shouldBe true
    client.exists("key_baz") shouldBe false
    client.get[String]("key_rbaz") shouldEqual Some("bar")

    // rename nx
    client.rename("key_rbaz", "key_buzzword", true) shouldBe false
  }

  it should "support inc/dec operations" in {
    client.set("inc_foo", 1) shouldBe true
    client.incr("inc_foo", 10) shouldEqual 11
    client.incr("inc_foo", -11) shouldEqual 0
    client.incr("unexistent", -5) shouldEqual -5
  }

  it should "fail to rename nonexistent key" in {
    intercept[RedisException] {
      client.rename("non-existent", "newkey")
    }
  }

  it should "fail to increment nonexistent key" in {
    client.set("baz", "bar") shouldBe true

    intercept[RedisException] {
      client.incr("baz")
    }

  }

  it should "support hash commands" in {

    assert(client.hset[String]("hash_foo", "one", "another"), "Problem with creating hash")
    assert(client.hmset("hash_bar", "baz1" -> "1", "baz2" -> "2"), "Problem with creating 2 values hash")

    client.hget[String]("hash_foo", "one") shouldBe Some("another")
    client.hget[String]("hash_bar", "baz1") shouldBe Some("1")

    assert(Map("baz1" -> "1", "baz2" -> "2") === client.hmget[String]("hash_bar", "baz1", "baz2"), "Resulting map with 2 values")
    assert(Map("baz2" -> "2") === client.hmget[String]("hash_bar", "baz2"), "Resulting map with 1 values")

    assert(7 === client.hincr("hash_bar", "baz2", 5), "Was 2 plus 5 has to give 7")
    assert(-3 === client.hincr("hash_bar", "baz1", -4), "Was 1 minus 4 has to give -3")

    assert(Map("baz1" -> "-3", "baz2" -> "7") === client.hmget[String]("hash_bar", "baz1", "baz2"), "Changed map has to have values 7, -3")

    assert(client.hmset[String]("hash_zoo-key", "foo" -> "{foo}", "baz" -> "{baz}", "vaz" -> "{vaz}", "bzr" -> "{bzr}", "wry" -> "{wry}"))

    val map = client.hmget[String]("hash_zoo-key", "foo", "bzr", "vaz", "wry")

    for(k <- map.keys) {
      assert("{" + k + "}" == map(k).toString, "Values don't correspond to keys in result")
    }

    assert(Map("vaz" -> "{vaz}", "bzr" -> "{bzr}", "wry" -> "{wry}")=== client.hmget[String]("hash_zoo-key", "boo", "bzr", "vaz", "wry"))
    assert(5 === client.hlen("hash_zoo-key"), "Length of map elements should be 5")
    assert(client.hdel("hash_zoo-key", "bzr"), "Problem with deleting")
    client.hget[String]("hash_zoo-key", "bzr") shouldBe None

    assert(client.hexists("hash_zoo-key", "vaz"), "Key 'vaz' should exist in map `zoo-key`")
    assert(4 === client.hlen("hash_zoo-key"), "Length of map elements should be 2")
    client.hkeys("hash_zoo-key") shouldBe Seq("wry", "vaz", "baz", "foo")
    client.hkeys("hash_nonexistent-key") shouldBe Nil
  }

  it should "support sorted set commands" in {
    client.zadd("key-zadd", (1.0F, "one")) shouldEqual 1
    client.zadd("key-zadd", (1.0F, "one"), (2.5F, "two")) shouldEqual 1
    client.zadd("key-zadd", (1.0F, "one")) shouldEqual 0
    client.zadd("key-zadd", ZaddOptions(XX), (1.0F, "new-member")) shouldEqual 0
    client.zadd("key-zadd", ZaddOptions(NX), (2.0F, "one")) shouldEqual 0

    client.zadd("key-zadd", ZaddOptions(XX, true), (5.0F, "one"), (1.0F, "new-member")) shouldEqual 1
    client.zadd("key-zadd", ZaddOptions(NX, true), (3.0F, "one")) shouldEqual 0
    
    the [RedisException] thrownBy {
      client.zadd("key-zadd", ZaddOptions(NO, false, true), (2.0F, "two"), (3.0F, "three"))
    } should have message "ERR INCR option supports a single increment-element pair"
  }

  /*
  @Test def testKeyTtl() {
    assertTrue(client.set("foo", "bar", 5))
    assertTrue(client.ttl("foo") <= 5)

    assertTrue(client.set("baz", "foo"))

    assertEquals("Ttl if not set should equal -1", -1, client.ttl("baz"))

    assertEquals("Ttl of nonexistent entity has to be -2", -2, client.ttl("bar"))

    assertTrue(client.set("bee", "test", 100))
    assertTrue(client.persist("bee"))
    assertEquals("Ttl of persisted should equal -1", -1, client.ttl("bee"))
  }

  @Test def testSet() {
    assertEquals("Should add 2 elements and create set", 2, client.sadd("sport", "tennis", "hockey"))
    assertEquals("Should add only one element", 1, client.sadd("sport", "football"))
    assertEquals("Should not add any elements", 0, client.sadd("sport", "hockey"))

    assertTrue("Elements should be in set", client.sismember("sport", "hockey"))
    assertFalse("Elements should not be in set", client.sismember("sport", "ski"))
    assertFalse("No set – no elements", client.sismember("drink", "ski"))

    assertEquals("Resulting set has to contain all elements", Set("tennis", "hockey", "football"), client.smembers[String]("sport"))
  }

  @Test def testRedlockScript() {
    import com.fotolog.redis.primitives.Redlock._

    assertTrue(client.set("redlock:key", "redlock:value"))
    assertEquals("Should not unlock redis server with nonexistent value", Set(0), client.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "non:value"))
    assertEquals("Should unlock redis server", Set(1), client.eval[Int](UNLOCK_SCRIPT, "redlock:key" -> "redlock:value"))

  }

  @Test def testPubSub() {
    val latch = new CountDownLatch(1)
    var invoked = false

    val subscribtionRes = RedisClient("mem:test").subscribe[String]("f*", "foo", "f*", "bar") { (channel, msg) =>
      invoked = true
      latch.countDown()
    }

    assertEquals(Seq(1, 2, 2, 3), subscribtionRes)

    client.publish("fee", "message")

    latch.await(5, TimeUnit.SECONDS)

    assertTrue(invoked)

  }
  */
}
