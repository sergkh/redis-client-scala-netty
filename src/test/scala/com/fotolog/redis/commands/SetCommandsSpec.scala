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
}
