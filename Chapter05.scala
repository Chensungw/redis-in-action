package com.nirvana.test.redis

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.mchange.v2.csv.CsvBufferedReader
import com.nirvana.redis.JedisWrapper
import redis.clients.jedis.{Jedis, ZParams}

import java.io.{BufferedReader, File, FileReader}
import java.text.{Collator, SimpleDateFormat}
import java.util.{Collections, Date, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.break




object Chapter05 {
  val DEBUG = "debug"
  val INFO = "info"
  val WARNING = "warning"
  val ERROR = "error"
  val CRITICAL = "critical"

  val COLLATOR: Collator = Collator.getInstance

  val TIMESTAMP = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
  private val ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00")

  val PRECISION: Array[Int] = Array[Int](1, 5, 60, 300, 3600, 18000, 86400)


  private val CONFIGS = mutable.Map.empty[String, mutable.Map[String,Object]]
  private val CHECKED = mutable.Map.empty[String, Long]

  val REDIS_CONNECTIONS = mutable.Map.empty[String, Jedis]

  def redisConnection(component: String): Jedis = {
    var configConn = REDIS_CONNECTIONS.getOrElse("config", null)
    if (configConn == null) {
      configConn = JedisWrapper.createJedisWrapper("192.168.7.58", 6379, "123456").getConnection
      configConn.select(15)
      REDIS_CONNECTIONS.put("config", configConn)
    }
    val key = "config:redis:" + component
    val oldConfig = CONFIGS.get(key)
    val config = getConfig(configConn, "redis", component)
    if (!config.equals(oldConfig)) {
      val conn = JedisWrapper.createJedisWrapper("192.168.7.58", 6379, "123456").getConnection
      if (config.contains("db")) conn.select(config.get("db").asInstanceOf[Double].intValue)
      REDIS_CONNECTIONS.put(key, conn)
    }
    REDIS_CONNECTIONS(key)
  }

  def getConfig(conn: Jedis, `type`: String, component: String): mutable.Map[String, Object] = {
    val wait = 1000
    val key = "config:" + `type` + ':' + component
    val lastChecked = CHECKED.getOrElse(key, 0L)
    if (lastChecked < System.currentTimeMillis - wait) {
      CHECKED.put(key, System.currentTimeMillis)
      val value = conn.get(key)
      var config: mutable.Map[String, Object] = null
      if (value != null) {
        config = JSON.parseObject(value).toJavaObject(classOf[mutable.Map[String,Object]])
      }
      else config = mutable.Map.empty
      CONFIGS.put(key, config)
    }
    CONFIGS(key)
  }

  def setConfig(conn: Jedis, `type`: String, component: String, config: Map[String, Object]): Unit = {
    conn.set("config:" + `type` + ':' + component, JSON.toJSON(config).toString)
  }

  private var lastChecked = 0L
  private var underMaintenance = false

  def isUnderMaintenance(conn: Jedis): Boolean = {
    if (lastChecked < System.currentTimeMillis - 1000) {
      lastChecked = System.currentTimeMillis
      val flag = conn.get("is-under-maintenance")
      underMaintenance = "yes" == flag
    }
    underMaintenance
  }

  def findCityByIp(conn: Jedis, ipAddress: String): Array[String] = {
    val score = ipToScore(ipAddress)
    val results = conn.zrevrangeByScore("ip2cityid:", score, 0, 0, 1)
    if (results.size == 0) return null
    var cityId = results.iterator.next
    cityId = cityId.substring(0, cityId.indexOf('_'))
    JSON.parseArray(conn.hget("cityid2city:", cityId)).toJavaObject(classOf[Array[String]])
  }

  def importCitiesToRedis(conn: Jedis, file: File): Unit = {
    var reader: FileReader = null
    try {
      reader = new FileReader(file)
      val parser = new CsvBufferedReader(new BufferedReader(reader))
      var line = parser.readSplitLine()
      while (line != null) {
        if (!(line.length < 4 || !Character.isDigit(line(0).charAt(0)))) {
          val cityId = line(0)
          val country = line(1)
          val region = line(2)
          val city = line(3)
          val json = JSON.toJSON(List[Object](city, region, country).asJava)
          conn.hset("cityid2city:", cityId, json.toString)
          line = parser.readSplitLine()
        }

      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally try reader.close()
    catch {
      case e: Exception =>


      // ignore
    }
  }

  def importIpsToRedis(conn: Jedis, file: File): Unit = {
    var reader: FileReader = null
    try {
      reader = new FileReader(file)
      val parser = new CsvBufferedReader(new BufferedReader(reader))
      var count = 0
      var line = parser.readSplitLine()
      while (line != null) {
        val startIp = if (line.length > 1) line(0) else ""
        if (startIp.toLowerCase.indexOf('i') == -1) {
          var score = 0L
          if (startIp.indexOf('.') != -1)
            score = ipToScore(startIp)
          else
            score = java.lang.Long.parseLong(startIp, 10)

          val cityId = line(2) + '_' + count
          conn.zadd("ip2cityid:", score, cityId)
          count += 1
          line = parser.readSplitLine()
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally try reader.close()
    catch {
      case e: Exception =>


      // ignore
    }
  }

  def ipToScore(ipAddress: String): Long = {
    var score = 0L
    for (v <- ipAddress.split("\\.")) {
      score = score * 256 + Integer.parseInt(v, 10)
    }
    score
  }

  def updateStats(conn: Jedis, context: String, `type`: String, value: Double): List[Object] = {
    val timeout = 5000
    val destination = "stats:" + context + ':' + `type` //存储统计数据的键
    val startKey = destination + ":start"
    val end = System.currentTimeMillis + timeout
    while (System.currentTimeMillis < end) {
      conn.watch(startKey)
      val hourStart = ISO_FORMAT.format(new Date())
      val existing = conn.get(startKey)
      val trans = conn.multi
      if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
        trans.rename(destination, destination + ":last")
        trans.rename(startKey, destination + ":pstart")
        trans.set(startKey, hourStart)
      }
      val tkey1 = UUID.randomUUID.toString //临时键
      val tkey2 = UUID.randomUUID.toString
      trans.zadd(tkey1, value, "min")
      trans.zadd(tkey2, value, "max")
      trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MIN), destination, tkey1)
      trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MAX), destination, tkey2)
      trans.del(tkey1, tkey2)
      trans.zincrby(destination, 1, "count")
      trans.zincrby(destination, value, "sum")
      trans.zincrby(destination, value * value, "sumsq")
      val results = trans.exec
      if (results  != null) return results.subList(results.size - 3, results.size).asScala.toList
    }
    null
  }

  class CleanCountersThread(val conn: Jedis, var sampleCount: Int = 100, val timeOffset: Long) extends Thread {
    conn.select(15)
    private var quit = false

    def setQuit(): Unit = {
      quit = true
    }

    override def run(): Unit = {
      var passes = 0
      while (!quit) {
        val start = System.currentTimeMillis + timeOffset
        var index = 0
        while (index < conn.zcard("known:")) {
          val hashSet = conn.zrange("known:", index, index)
          index += 1
          if (hashSet.size == 0) break //todo: break is not supported
          val hash = hashSet.iterator.next
          val prec = hash.substring(0, hash.indexOf(':')).toInt
          var bprec = Math.floor(prec / 60).toInt
          if (bprec == 0) bprec = 1
          if ((passes % bprec) == 0) {
            val hkey = "count:" + hash
            val cutoff = String.valueOf(((System.currentTimeMillis + timeOffset) / 1000) - sampleCount * prec)
            var samples = conn.hkeys(hkey).asScala.toList
            samples = samples.sorted(Ordering[String])
            val remove = bisectRight(samples, cutoff)
            if (remove != 0) {
              conn.hdel(hkey, samples.slice(0, remove).toArray:_*)
              if (remove == samples.size) {
                conn.watch(hkey)
                if (conn.hlen(hkey) == 0) {
                  val trans = conn.multi
                  trans.zrem("known:", hash)
                  trans.exec
                  index -= 1
                }
                else conn.unwatch
              }
            }
          }

        }
        passes += 1
        val duration = Math.min((System.currentTimeMillis + timeOffset) - start + 1000, 60000)
        try Thread.sleep(Math.max(60000 - duration, 1000))
        catch {
          case _: InterruptedException =>
            Thread.currentThread.interrupt()
        }
      }
    }

    // mimic python's bisect.bisect_right
    def bisectRight(values: List[String], key: String): Int = {
      val index = Collections.binarySearch(values.asJava, key)
      if (index < 0) Math.abs(index) - 1
      else index + 1
    }
  }

  def updateCounter(conn: Jedis, name: String, count: Int, now: Long): Unit = {
    val trans = conn.multi
    for (prec <- PRECISION) {
      val pnow = (now / prec) * prec //取得当前时间片开始时间
      val hash = String.valueOf(prec) + ':' + name
      trans.zadd("known:", 0, hash) //目前使用的计数器
      trans.hincrBy("count:" + hash, String.valueOf(pnow), count) //每个时间片的计数
    }
    trans.exec
  }

  def getCounter(conn: Jedis, name: String, precision: Int) = {
    val hash = String.valueOf(precision) + ':' + name
    val data = conn.hgetAll("count:" + hash)
    var results = List.empty[(Long, Long)]
    data.asScala.foreach(entry => results +:= (entry._1.toLong, entry._2.toLong))
    results = results.sorted
    results
  }

  def logRecent(conn: Jedis, name: String, message: String, severity: String): Unit = {
    val destination = "recent:" + name + ':' + severity
    val pipe = conn.pipelined
    pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message)
    pipe.ltrim(destination, 0, 99) //剪枝
    pipe.sync()
  }

  def logCommon(conn: Jedis, name: String, message: String, severity: String, timeout: Int): Unit = {
    val commonDest = "common:" + name + ':' + severity
    val startKey = commonDest + ":start"
    val end = System.currentTimeMillis + timeout
    while (System.currentTimeMillis < end) {
      conn.watch(startKey)
      val hourStart = ISO_FORMAT.format(new Date())
      val existing = conn.get(startKey)
      val trans = conn.multi
      if (existing != null && COLLATOR.compare(existing, hourStart) < 0) { //如果是上个小时的日志，将旧日志归档
        trans.rename(commonDest, commonDest + ":last")
        trans.rename(startKey, commonDest + ":pstart")
        trans.set(startKey, hourStart)
      }
      trans.zincrby(commonDest, 1, message)
      val recentDest = "recent:" + name + ':' + severity
      trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message)
      trans.ltrim(recentDest, 0, 99)
      val results = trans.exec
      // null response indicates that the transaction was aborted due to
      // the watched key changing.
      if (results != null) return
    }
  }

}
