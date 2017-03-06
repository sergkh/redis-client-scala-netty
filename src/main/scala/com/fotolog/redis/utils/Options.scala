package com.fotolog.redis.utils

import java.nio.charset.Charset

/**
  *
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>
  *         created on 06/03/17
  */
object Options {

  val charset = Charset.forName("UTF-8")

  case class Limit(offset: Int, count: Int) {
    def asBin = Seq("LIMIT".getBytes(charset), offset.toString.getBytes(charset), count.toString.getBytes(charset))
  }

}
