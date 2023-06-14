package com.nirvana.test.redis

import com.nirvana.redis.JedisWrapper
import redis.clients.jedis.{Jedis, PipelineBase, Transaction, Tuple}

import java.lang.reflect.Method
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.break


object Chapter08 {

  private val HOME_TIMELINE_SIZE = 1000
  private val POSTS_PER_PASS = 1000
  private val REFILL_USERS_STEP = 50

  def acquireLockWithTimeout(conn: Jedis, _lockName: String, acquireTimeout: Int, lockTimeout: Int): String = {
    val id = UUID.randomUUID.toString
    val lockName = "lock:" + _lockName
    val end = System.currentTimeMillis + (acquireTimeout * 1000)
    while (System.currentTimeMillis < end) {
      if (conn.setnx(lockName, id) >= 1) {
        conn.expire(lockName, lockTimeout)
        return id
      }
      else if (conn.ttl(lockName) <= 0) conn.expire(lockName, lockTimeout)
      try Thread.sleep(1)
      catch {
        case _: InterruptedException => Thread.interrupted
      }
    }
    null
  }

  def releaseLock(conn: Jedis, _lockName: String, identifier: String): Boolean = {
    val lockName = "lock:" + _lockName
    while (true) {
      conn.watch(lockName)
      if (identifier == conn.get(lockName)) {
        val trans = conn.multi
        trans.asInstanceOf[PipelineBase].del(lockName)
        val result = trans.exec
        // null response indicates that the transaction was aborted due
        // to the watched key changing.
        if (result != null) return true
      }
      conn.unwatch
      break
    }
    false
  }

  def createUser(conn: Jedis, login: String, name: String): Long = {
    val llogin = login.toLowerCase
    val lock = acquireLockWithTimeout(conn, "user:" + llogin, 10, 1)
    if (lock == null) return -1
    if (conn.hget("users:", llogin) != null) return -1
    val id = conn.incr("user:id:")
    val trans = conn.multi
    trans.hset("users:", llogin, String.valueOf(id))
    val values = mutable.HashMap.empty[String, String]
    values.put("login", login)
    values.put("id", String.valueOf(id))
    values.put("name", name)
    values.put("followers", "0")
    values.put("following", "0")
    values.put("posts", "0")
    values.put("signup", String.valueOf(System.currentTimeMillis))
    trans.hmset("user:" + id, values.asJava)
    trans.exec
    releaseLock(conn, "user:" + llogin, lock)
    id
  }

  def createStatus(conn: Jedis, uid: Long, message: String, _data: mutable.HashMap[String, String]): Long = {
    var trans = conn.multi
    trans.hget("user:" + uid, "login")
    trans.incr("status:id:")
    val response = trans.exec
    val login = response.get(0).asInstanceOf[String] //用户名
    val id = response.get(1).asInstanceOf[Long] //新的消息id
    if (login == null) return -1
    val data = if (_data == null) mutable.HashMap.empty[String, String] else _data
    data.put("message", message)
    data.put("posted", String.valueOf(System.currentTimeMillis))
    data.put("id", String.valueOf(id))
    data.put("uid", String.valueOf(uid))
    data.put("login", login)
    trans = conn.multi
    trans.hmset("status:" + id, data.asJava)
    trans.hincrBy("user:" + uid, "posts", 1)
    trans.exec
    id
  }

  def getStatusMessages(conn: Jedis, uid: Long, page: Int, count: Int) = {
    val statusIds = conn.zrevrange("home:" + uid, (page - 1) * count, page * count - 1)
    val trans = conn.multi
    for (id <- statusIds.asScala) {
      trans.hgetAll("status:" + id)
    }
    var statuses = List.empty[Map[String, String]]
    for (result <- trans.exec.asScala) {
      val status = result.asInstanceOf[Map[String, String]]
      if (status != null && status.nonEmpty) statuses +:= status
    }
    statuses
  }

  def followUser(conn: Jedis, uid: Long, otherUid: Long): Boolean = {
    val fkey1 = "following:" + uid
    val fkey2 = "followers:" + otherUid
    if (conn.zscore(fkey1, String.valueOf(otherUid)) != null) return false
    val now = System.currentTimeMillis
    var trans = conn.multi
    trans.zadd(fkey1, now, String.valueOf(otherUid))
    trans.zadd(fkey2, now, String.valueOf(uid))
    trans.zcard(fkey1)
    trans.zcard(fkey2)
    trans.zrevrangeWithScores("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1)
    val response = trans.exec
    val following = response.get(response.size - 3).asInstanceOf[Long]
    val followers = response.get(response.size - 2).asInstanceOf[Long]
    val statuses = response.get(response.size - 1).asInstanceOf[mutable.Set[Tuple]]
    trans = conn.multi
    trans.hset("user:" + uid, "following", String.valueOf(following))
    trans.hset("user:" + otherUid, "followers", String.valueOf(followers))
    if (statuses.nonEmpty) {
      for (status <- statuses) {
        trans.zadd("home:" + uid, status.getScore, status.getElement)
      }
    }
    trans.zremrangeByRank("home:" + uid, 0, 0 - HOME_TIMELINE_SIZE - 1)
    trans.exec
    true
  }

  def unfollowUser(conn: Jedis, uid: Long, otherUid: Long): Boolean = {
    val fkey1 = "following:" + uid
    val fkey2 = "followers:" + otherUid
    if (conn.zscore(fkey1, String.valueOf(otherUid)) == null) return false
    var trans = conn.multi
    trans.zrem(fkey1, String.valueOf(otherUid))
    trans.zrem(fkey2, String.valueOf(uid))
    trans.zcard(fkey1)
    trans.zcard(fkey2)
    trans.zrevrange("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1)
    val response = trans.exec
    val following = response.get(response.size - 3).asInstanceOf[Long]
    val followers = response.get(response.size - 2).asInstanceOf[Long]
    val statuses = response.get(response.size - 1).asInstanceOf[mutable.Set[String]]
    trans = conn.multi
    trans.hset("user:" + uid, "following", String.valueOf(following))
    trans.hset("user:" + otherUid, "followers", String.valueOf(followers))
    if (statuses.nonEmpty) {
      for (status <- statuses) {
        trans.zrem("home:" + uid, status)
      }
    }
    trans.exec
    true
  }

  def postStatus(conn: Jedis, uid: Long, message: String, data: mutable.HashMap[String, String]): Long = {
    val id = createStatus(conn, uid, message, data)
    if (id == -1) return -1
    val postedString = conn.hget("status:" + id, "posted")
    if (postedString == null) return -1
    val posted = java.lang.Long.parseLong(postedString)
    conn.zadd("profile:" + uid, posted, String.valueOf(id)) //将状态消息添加到用户的个人时间线里面
    syndicateStatus(conn, uid, id, posted, 0) //推送状态消息给用户的关注者
    id
  }

  def syndicateStatus(conn: Jedis, uid: Long, postId: Long, postTime: Long, _start: Double): Unit = {
    val followers = conn.zrangeByScoreWithScores("followers:" + uid, String.valueOf(_start), "inf", 0, POSTS_PER_PASS)
    val trans = conn.multi
    import scala.collection.JavaConversions._
    var start = 0D
    for (tuple <- followers) {
      val follower = tuple.getElement
      start = tuple.getScore
      trans.zadd("home:" + follower, postTime, String.valueOf(postId)) //添加到用户关注者的主页时间线
      trans.zrange("home:" + follower, 0, -1)
      trans.zremrangeByRank("home:" + follower, 0, 0 - HOME_TIMELINE_SIZE - 1)
    }
    trans.exec

    if (followers.size >= POSTS_PER_PASS) { //关注者超过1000人, 在延时队列执行剩余的更新操作
      try {
        val method = getClass.getDeclaredMethod("syndicateStatus", classOf[Nothing], java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Double.TYPE)
        executeLater("default", method, uid, postId, postTime, start)
      } catch {
        case e: Exception =>
          throw new RuntimeException(e)
      }
    }
  }

  def executeLater(queue: String, method: Method, args: AnyRef*): Unit = {
    val thread = new MethodThread(this, method, args.toArray)
    thread.start()
  }

  class MethodThread(private var instance: AnyRef, private var method: Method, private var args: Array[AnyRef]) extends Thread {
    override def run(): Unit = {
      val conn = JedisWrapper.createJedisWrapper("192.168.7.58", 6379, "123456").getConnection
      conn.select(15)
      val args = new Array[AnyRef](this.args.length + 1)
      System.arraycopy(this.args, 0, args, 1, this.args.length)
      args(0) = conn
      try method.invoke(instance, args)
      catch {
        case e: Exception =>
          throw new RuntimeException(e)
      }
    }
  }

  def deleteStatus(conn: Jedis, uid: Long, statusId: Long): Boolean = {
    val key = "status:" + statusId
    val lock = acquireLockWithTimeout(conn, key, 1, 10)
    if (lock == null) return false
    try {
      if (!(String.valueOf(uid) == conn.hget(key, "uid"))) return false
      val trans = conn.multi
      trans.asInstanceOf[PipelineBase].del(key) //删除指定状态消息
      trans.zrem("profile:" + uid, String.valueOf(statusId)) //从用户的个人时间线里面移除被删除的状态消息
      trans.zrem("home:" + uid, String.valueOf(statusId)) //这里是用户主页的时间线
      trans.hincrBy("user:" + uid, "posts", -1) //减少已发布状态消息的数量
      trans.exec
      true
    } finally releaseLock(conn, key, lock)
  }

}
