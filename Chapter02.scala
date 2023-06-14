package com.nirvana.test.redis

import redis.clients.jedis.Jedis

import java.lang.reflect.Method
import java.net.{MalformedURLException, URL}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Chapter02 {
  /**检查登录 cookie*/
  def checkToken(conn: Jedis, token: String): String = conn.hget("login:", token)

  /**更新token*/
  def updateToken(conn: Jedis, token: String, user: String, item: String): Unit = {
    val timestamp = System.currentTimeMillis / 1000
    conn.hset("login:", token, user)
    conn.rpush("recent:", token)
    if (item != null) {
      conn.zadd("viewed:" + token, timestamp, item) //用户最近浏览过的商品有序集合
      conn.zremrangeByRank("viewed:" + token, 0, -26) //修剪有序集合，只保留25个
      conn.zincrby("viewed:", -1, item) //所有商品的浏览次数，被浏览最多的商品在有序集合的索引0位置上
    }
  }

  def updateTokenPipeline(conn: Jedis, token: String, user: String, item: String): Unit = {
    val timestamp = System.currentTimeMillis / 1000
    val pipe = conn.pipelined
    pipe.multi
    pipe.hset("login:", token, user)
    pipe.zadd("recent:", timestamp, token)
    if (item != null) {
      pipe.zadd("viewed:" + token, timestamp, item)
      pipe.zremrangeByRank("viewed:" + token, 0, -26)
      pipe.zincrby("viewed:", -1, item)
    }
    pipe.exec
  }

  def benchmarkUpdateToken(conn: Jedis, duration: Int): Unit = {
    try {
      //@SuppressWarnings(Array("rawtypes"))
      val args = Array[Class[_]](classOf[Jedis], classOf[String], classOf[String], classOf[String])
      val methods = Array[Method](this.getClass.getDeclaredMethod("updateToken", args:_*), this.getClass.getDeclaredMethod("updateTokenPipeline", args:_*))
      for (method <- methods) {
        var count = 0
        val start = System.currentTimeMillis
        val end = start + (duration * 1000)
        while (System.currentTimeMillis < end) {
          count += 1
          method.invoke(this, conn, "token", "user", "item")
        }
        val delta = System.currentTimeMillis - start
        System.out.println(method.getName + ' ' + count + ' ' + (delta / 1000) + ' ' + (count / (delta / 1000)))
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  class CleanSessionsThread(private var limit: Int, val conn: Jedis) extends Thread {
    def apply(limit: Int, conn: Jedis): Unit = {
      conn.select(15)
    }

    private var quit = false

    def setQuit(): Unit = {
      quit = true
    }

    override def run(): Unit = {
      while (!quit) {
        val size = conn.llen("recent:") //找出目前已有令牌的数量
        if (size <= limit) { //未超过限制则休眠
          try Thread.sleep(1000)
          catch {
            case _: InterruptedException =>
              Thread.currentThread.interrupt()
          }
        } else {
          val endIndex = Math.min(size - limit, 100)
          val tokens = conn.lrange("recent:", 0, endIndex - 1).asScala.toArray //需要移除的令牌ID
          val sessionKeys = ListBuffer.empty[String]
          for (token <- tokens) {
            sessionKeys += ("viewed:" + token)
            sessionKeys += ("cart:" + token)
          }
          conn.del(sessionKeys.toArray: _*) //清理用户最近浏览记录、购物车的数据
          conn.hdel("login:", tokens: _*) //清理用户登录信息
          conn.ltrim("recent:", endIndex, -1)//清理最近登录令牌
        }

      }
    }
  }

  /**购物车添加、删除物品*/
  def addToCart(conn: Jedis, session: String, item: String, count: Int): Unit = {
    if (count <= 0) conn.hdel("cart:" + session, item)
    else conn.hset("cart:" + session, item, String.valueOf(count))
  }

  /**缓存网页请求*/
  def cacheRequest(conn: Jedis, request: String, callback: Callback ): String = {
    if (!canCache(conn, request)) return if (callback != null) callback.call(request) else null
    val pageKey = "cache:" + hashRequest(request)
    var content = conn.get(pageKey)
    if (content == null && callback != null) {
      content = callback.call(request) //页面还没被缓存时生成页面
      conn.setex(pageKey, 300, content) //新生成的页面放到缓存里面
    }
    content
  }

  def canCache(conn: Jedis, request: String): Boolean = try {
    val url = new URL(request)
    val params = mutable.Map.empty[String, String]
    if (url.getQuery != null) {
      for (param <- url.getQuery.split("&")) {
        val pair = param.split("=", 2)
        params.put(pair(0), if (pair.length == 2) pair(1) else null)
      }
    }
    val itemId = extractItemId(params)
    if (itemId == null || isDynamic(params)) return false
    val rank = conn.zrank("viewed:", itemId)
    rank != null && rank < 10000
  } catch {
    case _: MalformedURLException  =>
      false
  }

  def hashRequest(request: String): String = String.valueOf(request.hashCode)

  def isDynamic(params: mutable.Map[String, String]): Boolean = params.containsKey("_")

  def extractItemId(params: mutable.Map[String, String]): String = params("item")

  trait Callback {
    def call(request: String): String
  }

  def scheduleRowCache(conn: Jedis, rowId: String, delay: Int): Unit = {
    conn.zadd("delay:", delay, rowId) //先设置延迟值
    conn.zadd("schedule:", System.currentTimeMillis / 1000, rowId) //立即对需要缓存的数据进行调度
  }

  class CacheRowsThread(val conn: Jedis) extends Thread {
    def apply(conn: Jedis): Unit = {
      conn.select(15)
    }
    private var quit = false
    def setQuit(): Unit = {
      quit = true
    }

    override def run(): Unit = {
      while (!quit) {
        val range = conn.zrangeWithScores("schedule:", 0, 0) //尝试获取下一个需要被缓存的数据行以及该行的调度时间戳
        val next = if (range.size > 0) range.iterator.next else null
        val now = System.currentTimeMillis / 1000
        if (next == null || next.getScore > now) {
          try Thread.sleep(50)
          catch {
            case _: InterruptedException =>
              Thread.currentThread.interrupt()
          }
        } else {
          val rowId = next.getElement
          val delay = conn.zscore("delay:", rowId) //获取下一次调度的延迟时间
          if (delay <= 0) { //不必再缓存这个行，从缓存中删除
            conn.zrem("delay:", rowId)
            conn.zrem("schedule:", rowId)
            conn.del("inv:" + rowId)
          } else {
            val row = Inventory.get(rowId)
            conn.zadd("schedule:", now + delay, rowId)
            conn.set("inv:" + rowId, row.toString)
            println("the data is cache")
          }
        }
      }
    }
  }

  object Inventory {
    def get(id: String) = new Inventory(id)
  }

  class Inventory private(private var id: String) {
    private var data: String = "data to cache..."
    private var time = System.currentTimeMillis / 1000
  }

  def rescaleViewed(conn: Jedis): Unit = {
    var quit = false

    while (!quit) {
      conn.zremrangeByRank("viewed:", 0, -20001)
      conn.zinterstore("viewed:", {"viewed:"})
    }
  }
}
