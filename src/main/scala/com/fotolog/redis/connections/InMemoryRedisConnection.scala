package com.fotolog.redis.connections

import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.fotolog.redis.{KeyType, RedisException}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

object InMemoryRedisConnection {
  private[connections] val fakeServers = new ConcurrentHashMap[String, FakeServer]

  val context = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

  private[redis] val cleaner = new Runnable {
    override def run() = {
      val servers = fakeServers.elements()

      while(servers.hasMoreElements) {
        val map = servers.nextElement().map

        val it = map.entrySet.iterator
        while (it.hasNext) {
          if(it.next.getValue.expired) it.remove()
        }
      }
    }
  }

  private[connections] val ok = SingleLineResult("OK")
  private[connections] val bulkNull = BulkDataResult(None)
  private[connections] val multibulkEmpty = MultiBulkDataResult(Nil)

}

/**
 * Fake redis connection that can be used for testing purposes.
 */
class InMemoryRedisConnection(dbName: String) extends RedisConnection {
  import com.fotolog.redis.connections.ErrMessages._
  import com.fotolog.redis.connections.InMemoryRedisConnection._

  fakeServers.putIfAbsent(dbName, FakeServer())
  val server = fakeServers.get(dbName)
  val map = server.map
  var inSubscribedMode = false

  override def send(cmd: Cmd): Future[Result] = {
    context.execute(cleaner)
    Future {
      if(inSubscribedMode && !(cmd.isInstanceOf[Subscribe] || cmd.isInstanceOf[Unsubscribe])) {
        throw new RedisException(ERR_SUBSCRIBE_MODE)
      } else {
        syncSend(cmd)
      }

    }(context)
  }

  private[this] def hashCmd: PartialFunction[Cmd, Result] = {
    case hmset: Hmset =>
      map.put(hmset.key, Data.hash(Map(hmset.kvs:_*)))
      ok

    case hmget: Hmget =>
      optVal(hmget.key).map { data =>
        val m = data.asMap
        hmget.fields.map(f => BulkDataResult(m.get(f)) ) match {
          case Seq(one) => one
          case bulks: Seq[BulkDataResult] => MultiBulkDataResult(bulks)
        }
      } getOrElse bulkNull

    case hset: Hset =>
      optVal(hset.key) match {
        case Some(data) =>
          println(data)
          map.put(hset.key, Data.hash(data.asMap + (hset.field -> hset.value)))
          0
        case None =>
          map.put(hset.key, Data.hash(Map(hset.field -> hset.value)))
          1
      }

    case Hdel(key, field) =>
      optVal(key) match {
        case Some(data) =>
          val mapData = data.asMap
          mapData.get(field) match {
            case Some(_) =>
              map.put(key, Data.hash(mapData - field))
              1
            case None => 0
          }
        case None => 0
      }

    case Hget(k, fld) =>
      optVal(k) flatMap { _.asMap.get(fld).map( v => bytes2res(v) ) } getOrElse bulkNull

    case h: Hincrby =>
      val updatedMap = optVal(h.key).map { data =>
        val m = data.asMap
        val oldVal = m.get(h.field).map(a => bytes2int(a, ERR_INVALID_HASH_NUMBER)).getOrElse(0) + h.delta
        m.updated(h.field, int2bytes(oldVal))
      } getOrElse Map(h.field -> int2bytes(h.delta))

      map.put(h.key, Data.hash(updatedMap))

      int2res(bytes2int(updatedMap(h.field), ERR_INVALID_HASH_NUMBER))

    case Hexists(key, field) =>
      int2res(optVal(key).map(_.asMap.get(field).fold(0)(_ => 1)).getOrElse(0))

    case Hlen(key) => int2res(optVal(key).map(_.asMap.size).getOrElse(0))

    case Hkeys(key) =>
      optVal(key).map(_.asMap.keys).map(_.map(k => BulkDataResult(Some(k.getBytes))))
        .map(kvs => MultiBulkDataResult(kvs.toSeq)).getOrElse(multibulkEmpty)

    case Hvals(key) =>
      optVal(key).map(_.asMap.values).map(_.map(k => BulkDataResult(Some(k))))
        .map(kvs => MultiBulkDataResult(kvs.toSeq)).getOrElse(multibulkEmpty)

    case Hstrlen(key, field) =>
      optVal(key) match {
        case Some(data) =>
        val mapData = data.asMap
          mapData.get(field) match {
            case Some(value) => int2res(value.length)
            case None => 0
          }
        case None => 0
      }

    case h: Hincrbyfloat =>
      val updatedMap = optVal(h.key).map { data =>
        val m = data.asMap
        val oldVal = m.get(h.field).map(a => bytes2double(a, ERR_INVALID_HASH_NUMBER)).getOrElse(0.0) + h.delta
        m.updated(h.field, double2bytes(oldVal))
      } getOrElse Map(h.field -> double2bytes(h.delta))

      map.put(h.key, Data.hash(updatedMap))

      double2res(bytes2double(updatedMap(h.field), ERR_INVALID_HASH_NUMBER))

  }

  private[this] def setsCmd: PartialFunction[Cmd, Result] = {
    case set: SetCmd if set.nx =>
      Option( map.putIfAbsent(set.key, Data(set.v, set.expTime)) ) map(_ => bulkNull) getOrElse ok

    case set: SetCmd if set.xx =>
      Option( map.replace(set.key, Data(set.v, set.expTime)) ) map (_ => ok) getOrElse bulkNull

    case set: SetCmd =>
      map.put(set.key, Data(set.v, set.expTime))
      ok

    case sadd: Sadd =>
      val args = sadd.values.map(BytesWrapper).toSet
      val orig = optVal(sadd.key) map(_.asSet) getOrElse Set()
      map.put(sadd.key, Data.set(orig ++ args))
      args.diff(orig).size

    case sisMember: Sismember =>
      int2res(optVal(sisMember.key).map { data =>
        if(data.asSet.contains(BytesWrapper(sisMember.v))) 1 else 0
      } getOrElse 0)

    case Smembers(key) =>
      optVal(key) map (data =>
        MultiBulkDataResult(data.asSet.map(wrapper => bytes2res(wrapper.bytes)).toSeq)
        ) getOrElse MultiBulkDataResult(Seq())

    case Scard(key) => int2res(optVal(key).map(_.asSet.size).getOrElse(0))

    case srem: Srem =>
      val orig = optVal(srem.key) map (_.asSet) getOrElse Set()
      if (orig.isEmpty) 0
      else {
        val diff = BytesWrapper(srem.value)
        if (orig.contains(diff)) {
          map.put(srem.key, Data.set(orig - diff))
          1
        }
        else 0
      }

    case smove: Smove =>
      val srcSet = optVal(smove.srcKey) map (_.asSet) getOrElse Set()
      val destSet = optVal(smove.destKey) map (_.asSet) getOrElse Set()
      val v = BytesWrapper(smove.value)
      if (srcSet contains v) {
        map.put(smove.srcKey, Data.set(srcSet - v))
        map.put(smove.destKey, Data.set(destSet + v))
        1
      }
      else 0

    case sinter: Sinter =>
      val sets = sinter.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val res = sets reduceLeft(_ & _) map (f => bytes2res(f.bytes))
      MultiBulkDataResult(res.toSeq)

    case sinterstore: Sinterstore =>
      val sets = sinterstore.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val destSet = optVal(sinterstore.destKey) map (_.asSet) getOrElse Set()
      val res = sets reduceLeft(_ & _)
      map.put(sinterstore.destKey, Data.set(res))
      1

    case sdiff: Sdiff =>
      val sets = sdiff.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val res = sets reduceLeft (_ &~ _) map (f => bytes2res(f.bytes))
      MultiBulkDataResult(res.toSeq)

    case sdiffstore: Sdiffstore =>
      val sets = sdiffstore.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val destSet = optVal(sdiffstore.destKey) map (_.asSet) getOrElse Set()
      val res = sets reduceLeft(_ &~ _)
      map.put(sdiffstore.destKey, Data.set(res))
      1

    case sunion: Sunion =>
      val sets = sunion.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val res = sets reduceLeft (_ | _) map (f => bytes2res(f.bytes))
      MultiBulkDataResult(res.toSeq)

    case sunionstore: Sunionstore =>
      val sets = sunionstore.keys map (optVal(_) map (_.asSet) getOrElse Set())
      val destSet = optVal(sunionstore.destKey) map (_.asSet) getOrElse Set()
      val res = sets reduceLeft(_ | _)
      map.put(sunionstore.destKey, Data.set(res))
      1

    case Srandmember(key) =>
      val set = optVal(key) map (_.asSet) getOrElse Set()
      val randIndex = scala.util.Random.nextInt(set.size)
      val res = set.toSeq.get(randIndex)
      bytes2res(res.bytes)

    case Spop(key) =>
      val set = optVal(key) map (_.asSet) getOrElse Set()
      if (set.isEmpty) bulkNull
      else {
        val randIndex = scala.util.Random.nextInt(set.size)
        val res = set.toSeq.get(randIndex)
        map.put(key, Data.set(set - res))
        bytes2res(res.bytes)
      }
  }

  private[this] def listCmd: PartialFunction[Cmd, Result] = {

    case Rpush(key, value) =>
      val orig = optVal(key) map (_.asList) getOrElse Nil
      map.put(key, Data.list(orig :+ BytesWrapper(value)))
      orig.size + 1

    case Lpush(key, value) =>
      val orig = optVal(key) map (_.asList) getOrElse Nil
      map.put(key, Data.list(orig.::(BytesWrapper(value))))
      orig.size + 1

    case Llen(key) =>
      int2bytes(optVal(key) map (_.asList.size) getOrElse 0)

    case Lrange(key, start, end) =>
      implicit val orig = optVal(key) map (_.asList) getOrElse Nil

      //iterating between negative values
      def forNeg(begin: Int, stop: Int)(implicit lst: List[BytesWrapper]) = {
        val res = for { i <- begin to stop by 1 } yield lst(lst.length + i)
        res.toList
      }

      //iterating between positive values
      def forPos(start: Int, stop: Int)(implicit lst: List[BytesWrapper]) = {
        val res = for { i <- start to stop by 1} yield lst(i)
        res.toList
      }


      (start, end) match {
        case (s, e) if (s * e > 0 && s > e) || s > orig.size => multibulkEmpty
        case (s, e) if math.abs(s) > orig.size => orig
        case (s, e) if s < 0 && e >= orig.size => orig
        case (s, e) if s >= 0 && e >=orig.size => forPos(s, orig.size - 1)
        case (s, e) if s == e && s >= 0 => forPos(s, s)
        case (s, e) if s == e && s < 0 => forNeg(s, s)
        case (s, e) if s < 0 && e < 0 => forNeg(s, e)
        case (s, e) if s < 0 && e >= 0 => forNeg(s, -1) ::: forPos(0, e)
        case (s, e) if s >= 0 && e < 0 => forPos(s, orig.indexOf(orig.length + e))
        case (s, e) if s > 0 && e > 0 => forPos(s, e)
      }
      //TODO :fix lrange function

    case Ltrim(key, start, end) =>
      implicit val orig = optVal(key) map (_.asList) getOrElse Nil

      //puting values in storage
       def trimAcc(key: String, lst: List[BytesWrapper]): Result = {
        map.put(key, Data.list(lst))
        ok
      }
      //iterating between negative values
      def forNeg(start: Int, stop: Int)(implicit lst: List[BytesWrapper]) = {
        val res = for {i <- start to stop by 1} yield lst(lst.length + i)
        res.toList
      }

      //iterating between positive values
      def forPos(start: Int, stop: Int)(implicit lst: List[BytesWrapper]) = {
        val res = for { i <- start to stop by 1} yield lst(i)
        res.toList
      }

      (start, end) match {
        case (s, e) if (s > e && s*e > 0) || s > orig.size =>
          trimAcc(key, Nil)
        case (s, e) if s < 0 && e < 0  && math.abs(s) <= orig.size =>
          trimAcc(key, forNeg(s, e))
        case (s, e) if s < 0 && math.abs(s) <= orig.size =>
          trimAcc(key, forNeg(s, -1) ::: forPos(0, e))
        case (s, _) if s < 0 && math.abs(s) > orig.size =>
          trimAcc(key, orig)
        case (s, e) if e > orig.size =>
          trimAcc(key, forPos(s, orig.size -1))
        case (s, e) if e < 0 =>
          trimAcc(key, forPos(s, orig.indexOf(orig(orig.length + e))))
        case _ =>
          trimAcc(key, forPos(start, end))
      }

    case Lindex(key, index) =>
      val orig = optVal(key) map (_.asList) getOrElse Nil
      index match {
        case i: Int if math.abs(i) > orig.size - 1 => bulkNull
        case i: Int if i < 0 => BulkDataResult(Some(orig(orig.length + i).bytes))
        case i: Int if i >= 0 => BulkDataResult(Some(orig(i).bytes))
      }

    case Lset(key, index, value) =>

      val orig = optVal(key) map (_.asList) getOrElse Nil
      index match {
        case ind if ind > 0 => {
          val res = orig.updated(ind, BytesWrapper(value))
          map.put(key, Data.list(res))
          ok
        }
      }

  }

  private[this] def pubSubCmd: PartialFunction[Cmd, Result] = {

    case s: Subscribe =>
      val subscriptions = s.channels.map { ptrn =>
        val tuple = (this, ptrn, s)
        server.pubSub += tuple
        int2res(server.countUnique(this))
      }

      inSubscribedMode = true

      MultiBulkDataResult(subscriptions)

    case Publish(channel, data) =>
      val subscribers = server.matchingSubscribers(channel)

      subscribers.foreach {
        case (conn, pattern, subscribe) if subscribe.hasPattern =>
          subscribe.handler(MultiBulkDataResult(Seq(
            str2res("pmessage"), str2res(pattern), str2res(channel), bytes2res(data)
          )))
        case (conn, pattern, subscribe) =>
          subscribe.handler(MultiBulkDataResult(Seq(
            str2res("message"), str2res(channel), bytes2res(data)
          )))
      }

      subscribers.length

    case Unsubscribe(channels) =>
      val unsubsriptions = channels.map { ptrn =>
        server.pubSub = server.pubSub.filterNot {
          case (client, pattern, subscription) =>
            pattern == ptrn && client == this
        }

        int2res(server.countUnique(this))
      }

      if(server.countUnique(this) == 0) inSubscribedMode = false

      MultiBulkDataResult(unsubsriptions)
  }

  private[this] def scriptingCmd: PartialFunction[Cmd, Result] = {
    case eval: Eval =>
      import com.fotolog.redis.primitives.Redlock._

      // hardcoded support for Redlock implementation
      eval.script.equals(UNLOCK_SCRIPT) match {
        case true =>
          val (key, value) = eval.kv.head
          if (BytesWrapper(map.get(key).asBytes).equals(BytesWrapper(value))) {
            map.remove(key)
            1
          } else {
            0
          }

        case _ =>
          throw new RedisException(ERR_UNSUPPORTED_SCRIPT + eval.script)
      }
  }

  private[this] def keyCmd: PartialFunction[Cmd, Result] = {
    case Get(key) =>
      BulkDataResult(
        optVal(key) filterNot(_.expired) map (_.asBytes)
      )

    case Incr(key, delta) =>
      val newVal = (optVal(key) map { a => bytes2int(a.asBytes, ERR_INVALID_NUMBER) } getOrElse 0) + delta
      map.put(key, Data.str(int2bytes(newVal)))
      newVal

    case Keys(pattern) =>
      MultiBulkDataResult(
        map.keys()
          .filter(_.matches(pattern.replace("*", ".*?").replace("?", ".?")))
          .map(k => BulkDataResult(Some(k.getBytes))).toSeq
      )

    case Expire(key, seconds) =>
      int2res(optVal(key) map { d => map.put(key, d.copy(ttl = seconds)); 1 } getOrElse 0)

    case Exists(key) =>
      if(optVal(key).exists(!_.expired)) 1 else 0

    case Type(key) =>
      SingleLineResult(
        optVal(key) map ( _.keyType.name ) getOrElse KeyType.None.name
      )

    case Persist(key) =>
      int2res(optVal(key) map { d => map.put(key, d.copy(ttl = -1)); 1 } getOrElse 0)

    case Ttl(key) =>
      int2res(optVal(key) map (_.secondsLeft) getOrElse -2)

    case d: Del =>
      d.keys.count(k => Option(map.remove(k)).isDefined)

    case Rename(key, newKey, nx) =>
      optVal(key) match {
        case Some(v) =>
          if(nx && optVal(newKey).exists(!_.expired)) int2res(0)
          else {
            map.remove(key)
            map.put(newKey, v)
            int2res(1)
          }
        case None =>
          throw new RedisException(ERR_NO_SUCH_KEY)
      }

    case f: FlushAll =>
      map.clear()
      ok
  }

  private[this] def serverCmd: PartialFunction[Cmd, Result] = {
    case p: Ping => SingleLineResult("PONG")
  }

  private[this] def unsupportedCmd: PartialFunction[Cmd, Result] = {
    case unsupported =>
      throw new RedisException("ERR unsupported command " + unsupported)
  }

  private[this] def syncSend: PartialFunction[Cmd, Result] =
    hashCmd orElse setsCmd orElse pubSubCmd orElse listCmd orElse scriptingCmd orElse keyCmd orElse serverCmd orElse unsupportedCmd

  private[this] implicit def int2res(v: Int): BulkDataResult = BulkDataResult(Some(v.toString.getBytes))
  private[this] implicit def double2res(v: Double): BulkDataResult = BulkDataResult(Some(v.toString.getBytes))

  private[this] def bytes2int(b: Array[Byte], msg: String) = try {
    new String(b).toInt
  } catch {
    case p: IllegalArgumentException =>
      throw new RedisException(msg)
  }

  private[this] def bytes2double(b: Array[Byte], msg: String) = try {
    new String(b).toDouble
  } catch {
    case p: IllegalArgumentException =>
      throw new RedisException(msg)
  }

  private[this] implicit def bytes2res(a: Array[Byte]): BulkDataResult = BulkDataResult(Some(a))
  private[this] implicit def str2res(s: String): BulkDataResult = BulkDataResult(Some(s.getBytes))
  private[this] implicit def list2multres(lst: List[BytesWrapper]): MultiBulkDataResult = MultiBulkDataResult(lst map (f => bytes2res(f.bytes)))
  private[this] def int2bytes(i: Int): Array[Byte] = i.toString.getBytes
  private[this] def double2bytes(d: Double): Array[Byte] = d.toString.getBytes
  private[this] def optVal(key: String) = Option(map.get(key))

  override def isOpen: Boolean = true

  override def shutdown() {}
}


case class BytesWrapper(bytes: Array[Byte]) {

  override def hashCode() = util.Arrays.hashCode(bytes)

  override def equals(obj: Any): Boolean = obj match {
    case another: BytesWrapper => util.Arrays.equals(bytes, another.bytes)
    case _ => false
  }

  override def toString = s"DateWrapper: " + new String(bytes)

}

private object ErrMessages {
  val ERR_INVALID_NUMBER = "ERR value is not an integer or out of range"
  val ERR_INVALID_HASH_NUMBER = "ERR hash value is not an integer"
  val ERR_INVALID_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
  val ERR_UNSUPPORTED_SCRIPT= "ERR Operation not support for script:"
  val ERR_NO_SUCH_KEY = "ERR no such key"
  val ERR_SUBSCRIBE_MODE = "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context"
}

private[connections] case class Data(v: AnyRef, ttl: Int = -1, keyType: KeyType = KeyType.String, stamp: Long = Platform.currentTime) {
  def asBytes = keyType match {
    case KeyType.String => v.asInstanceOf[Array[Byte]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def asMap = keyType match {
    case KeyType.Hash => v.asInstanceOf[Map[String, Array[Byte]]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def asSet = keyType match {
    case KeyType.Set => v.asInstanceOf[Set[BytesWrapper]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def asList = keyType match {
    case KeyType.List => v.asInstanceOf[List[BytesWrapper]]
    case _ => throw new RedisException(ErrMessages.ERR_INVALID_TYPE)
  }

  def expired = ttl != -1 && Platform.currentTime - stamp > (ttl * 1000L)
  def secondsLeft = if (ttl == -1) -1 else (ttl - (Platform.currentTime - stamp) / 1000).toInt
}

private[connections] object Data {
  def str(d: Array[Byte], ttl: Int = -1) = Data(d, ttl, keyType = KeyType.String)
  def hash(map: Map[String, Array[Byte]], ttl: Int = -1) = Data(map, ttl, keyType = KeyType.Hash)
  def set(set: Set[BytesWrapper], ttl: Int = -1) = Data(set, ttl, keyType = KeyType.Set)
  def list(list: List[BytesWrapper], ttl: Int = -1) = Data(list, ttl, keyType = KeyType.List)
}

private[connections] case class FakeServer(
  map: ConcurrentHashMap[String, Data] = new ConcurrentHashMap[String, Data](),
  var pubSub: ListBuffer[(RedisConnection, String, Subscribe)] = ListBuffer.empty
) {

  def matchingSubscribers(channel: String) = pubSub.filter {
    case (connection, pattern, subscription) => channel.matches(pattern.replace("*", ".*?").replace("?", ".?"))
  }

  def countUnique(connection: RedisConnection) = pubSub.filter(_._1 == connection).map(_._2).toSet.size
}