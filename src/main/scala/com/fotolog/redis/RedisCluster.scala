package com.fotolog.redis

import java.util.concurrent.atomic.AtomicReference


class RedisCluster[Shard](hash: (Shard) => Int, uris: String*) {
    val h2cRef = new AtomicReference[Map[String, RedisClient]](Map.empty)

    def apply(s: Shard): RedisClient = {
        val h = uris(hash(s).abs % uris.length)
        h2cRef.get.get(h) match {
            case Some(c) => if (c.isConnected) c else newClient(h)
            case None => newClient(h)
        }
    }

    private[RedisCluster] def newClient(uri: String): RedisClient = uri.synchronized {
        var h2c = h2cRef.get

        def newClientThreadSafe(): RedisClient = {
            val c = RedisClient(uri)
            while(!h2cRef.compareAndSet(h2c, h2c + (uri -> c))) h2c = h2cRef.get
            c
        }

        h2c.get(uri) match {
            case None => newClientThreadSafe()
            case Some(c) => if (c.isConnected){ c } else { c.shutdown(); newClientThreadSafe() }
        }
    }
}

