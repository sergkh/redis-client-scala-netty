package com.fotolog.redis.memCommands

import com.fotolog.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by faiaz on 02.08.16.
  */
class MemHashCommandsSpec extends FlatSpec with Matchers with TestClient{
  
  "A hstrlen" should "return stored fields hash" in {
    memClient.hset("key", "f", "hello") shouldBe true
    memClient.hstrlen("key", "f") shouldEqual 5
  }
  "A hincrbyfloat" should "return updated value" in {
    memClient.hset("key", "f", 25.0) shouldBe true
    memClient.hincrbyfloat("key", "f", 2.0) shouldEqual 27.0
  }
}
