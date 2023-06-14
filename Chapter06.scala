package com.nirvana.test.redis

import com.alibaba.fastjson.JSON
import redis.clients.jedis.{Jedis, PipelineBase, ZParams}

import java.util.UUID
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break


object Chapter06 {

  def addUpdateContact(conn: Jedis, user: String, contact: String): Unit = {
    val acList = "recent:" + user
    val trans = conn.multi
    trans.lrem(acList, 0, contact)
    trans.lpush(acList, contact)
    trans.ltrim(acList, 0, 99)
    trans.exec
  }

  def removeContact(conn: Jedis, user: String, contact: String): Unit = {
    conn.lrem("recent:" + user, 0, contact)
  }

  def fetchAutocompleteList(conn: Jedis, user: String, prefix: String): List[String] = {
    val candidates = conn.lrange("recent:" + user, 0, -1).asScala
    val matches = ListBuffer.empty[String]
    for (candidate <- candidates) {
      if (candidate.toLowerCase.startsWith(prefix))
        matches += candidate
    }
    matches.toList
  }

  //前面和后面都至少留一个字符
  private val VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{"

  def findPrefixRange(prefix: String): Array[String] = {
    val posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length - 1)) //最后一个字符在字符列表的位置
    val suffix = VALID_CHARACTERS.charAt(if (posn > 0) posn - 1 else 0)
    val start = prefix.substring(0, prefix.length - 1) + suffix + '{' //替换最后一个字符为字符列表的前一个字符，再加上{
    val end = prefix + '{'
    Array[String](start, end)
  }

  def autocompleteOnPrefix(conn: Jedis, guild: String, prefix: String): Set[String] = {
    val range = findPrefixRange(prefix)
    var start = range(0)
    var end = range(1)
    val identifier = UUID.randomUUID.toString //防止多个相同的起始和结束元素被添加到集合里
    start += identifier
    end += identifier
    val zsetName = "members:" + guild
    conn.zadd(zsetName, 0, start)
    conn.zadd(zsetName, 0, end)
    var items: Set[String] = null
    while (true) {
      conn.watch(zsetName) //防止被其他客户端修改
      val sindex = conn.zrank(zsetName, start).intValue //aa{ abc ab{
      val eindex = conn.zrank(zsetName, end).intValue
      val erange = Math.min(sindex + 9, eindex - 2) //每次取10个
      val trans = conn.multi
      trans.zrem(zsetName, start)
      trans.zrem(zsetName, end)
      trans.zrange(zsetName, sindex, erange) //abc,sindex=0,eindex=0
      val results = trans.exec
      if (results != null) {
        items = results.get(results.size - 1).asInstanceOf[Set[String]]
        break
      }
    }
    val iterator = items.iterator
    while (iterator.hasNext)
      if (iterator.next.indexOf('{') == -1) //如果有其他自动补全操作正在执行那么从获取到的元素里面移除起始元素和结束元素
        iterator.remove()
    items
  }

  def joinGuild(conn: Jedis, guild: String, user: String): Unit = {
    conn.zadd("members:" + guild, 0, user) //score必须设置为0, zset才会以member进行排序
  }

  def leaveGuild(conn: Jedis, guild: String, user: String): Unit = {
    conn.zrem("members:" + guild, user)

    conn.watch()
    conn.unwatch()
    val transaction = conn.multi()
    transaction.exec()

    conn.zrank(",","")
  }

  def acquireLock(conn: Jedis, lockName: String, acquireTimeout: Long = 100): String = {
    val identifier = UUID.randomUUID.toString
    val end = System.currentTimeMillis + acquireTimeout
    while (System.currentTimeMillis < end) {
      if (conn.setnx("lock:" + lockName, identifier) == 1) return identifier
      try Thread.sleep(1)
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
    null
  }

  def releaseLock(conn: Jedis, lockName: String, identifier: String): Boolean = {
    val lockKey = "lock:" + lockName
    while (true) {
      conn.watch(lockKey)
      if (identifier == conn.get(lockKey)) { //检查是否还持有锁
        val trans = conn.multi
        trans.asInstanceOf[PipelineBase].del(lockKey)
        val results = trans.exec
        if (results != null) return true
      }
      conn.unwatch
      break //todo: break is not supported
    }
    false
  }

  def acquireLockWithTimeout(conn: Jedis, lockName: String, acquireTimeout: Long, lockTimeout: Long): String = {
    val identifier = UUID.randomUUID.toString
    val lockKey = "lock:" + lockName
    val lockExpire = (lockTimeout / 1000).toInt
    val end = System.currentTimeMillis + acquireTimeout
    while (System.currentTimeMillis < end) {
      if (conn.setnx(lockKey, identifier) == 1) { //先加锁
        conn.expire(lockKey, lockExpire) //设置过期时间
        return identifier
      }
      if (conn.ttl(lockKey) == -1) conn.expire(lockKey, lockExpire) //再次检验锁的过期时间, 防止加完锁还没设置过期时间直接crash
      try Thread.sleep(1)
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
    // null indicates that the lock was not acquired
    null
  }

  /**
   * 公平信号量, 根据请求的先后而不是时间戳来确定谁获取到信号量
   * @param conn    jedis
   * @param semname 超时有序集合
   * @param limit   信号量数量限制
   * @param timeout 超时
   * @return
   */
  def acquireFairSemaphore(conn: Jedis, semname: String, limit: Int, timeout: Long): String = {
    val identifier = UUID.randomUUID.toString
    val czset = semname + ":owner" //信号量拥有者(uuid-count)
    val ctr = semname + ":counter" //计数器, 用于实现谁最先获取信号量, 而不是根据时间来确定谁先获取信号量
    val now = System.currentTimeMillis
    var trans = conn.multi
    trans.zremrangeByScore(semname.getBytes, "-inf".getBytes, String.valueOf(now - timeout).getBytes) //"-inf"代表负无穷, 清理过期的信号量持有者
    val params = new ZParams()
    params.weights(1, 0) //集合的权重, 结果的score = zset1.score*权重 + zset2.score*权重
    trans.zinterstore(czset, params, czset, semname) //交集, 结果存到czset
    trans.incr(ctr)
    var results = trans.exec
    val counter = results.get(results.size - 1).asInstanceOf[Long].intValue
    trans = conn.multi
    trans.zadd(semname, now, identifier) //尝试获取信号量
    trans.zadd(czset, counter, identifier)
    trans.zrank(czset, identifier) //检查排名
    results = trans.exec
    val result = results.get(results.size - 1).asInstanceOf[Long].intValue
    if (result < limit) return identifier //排名足够低则获取成功直接返回
    trans = conn.multi
    trans.zrem(semname, identifier) //失败则删除前面添加的标识符
    trans.zrem(czset, identifier)
    trans.exec
    null
  }

  def releaseFairSemaphore(conn: Jedis, semname: String, identifier: String): Boolean = {
    val trans = conn.multi
    trans.zrem(semname, identifier)
    trans.zrem(semname + ":owner", identifier)
    val results = trans.exec
    results.get(results.size - 1).asInstanceOf[Long] == 1
  }

  def refreshFairSemaphore(conn: Jedis, semname: String, identifier: String): Boolean = {
    if (conn.zadd(semname, System.currentTimeMillis(), identifier) == 1) { //更新score,如果返回1则代表键不存在
      releaseFairSemaphore(conn, semname, identifier)
      return false
    }

    true
  }

  def acquireFairSemaphoreWithLock(conn: Jedis, semname: String, limit: Int, timeout: Long): String = {
    val identifier = acquireLock(conn, semname)
    if (identifier != null) {
      try acquireFairSemaphore(conn, semname, limit, timeout)
      finally releaseLock(conn, semname, identifier)
    }
    identifier
  }

  class PollQueueThread(val conn: Jedis) extends Thread {
    def apply(conn: Jedis): Unit = {
      conn.select(15)
    }
    private var quit = false
    def setQuit(): Unit = {
      quit = true
    }

    override def run(): Unit = {
      while (!quit) {
        val items = conn.zrangeWithScores("delayed:", 0, 0) //从任务队列获取第一个任务
        val item = if (items.size > 0) items.iterator.next else null
        if (item == null || item.getScore > System.currentTimeMillis) { //没有任务或者任务的执行时间未到
          try Thread.sleep(10)
          catch {
            case _: InterruptedException =>
              Thread.interrupted
          }
        } else {
          val json = item.getElement
          val values = JSON.parseObject(json, classOf[Array[String]])
          val identifier = values(0)
          val queue = values(1)
          val locked = acquireLock(conn, identifier)
          if (locked != null) {
            if (conn.zrem("delayed:", json) == 1) conn.rpush("queue:" + queue, json)
            releaseLock(conn, identifier, locked)
          }
        }
      }
    }
  }

}
