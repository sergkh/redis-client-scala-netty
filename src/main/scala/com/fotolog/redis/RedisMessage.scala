package com.fotolog.redis

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 12.03.2017.
  */
sealed trait RedisMessage

case class StringRedisMessage(content: String) extends RedisMessage

case object NullRedisMessage extends RedisMessage
case class RawRedisMessage(content: Array[Byte]) extends RedisMessage
case class IntRedisMessage(number: Int) extends RedisMessage
case class ErrorRedisMessage(error: String) extends RedisMessage
case class ArrayHeaderRedisMessage(length: Int) extends RedisMessage {
  def isNull: Boolean = length == -1
}

case class ArrayRedisMessage(children: List[RedisMessage], length: Int) extends RedisMessage {
  def addMessage(msg: RedisMessage) = copy(children = this.children :+ msg)
  def needPart = children.length < length
  def needLastPart = length - children.length == 1
  def isFull = children.length == length
}