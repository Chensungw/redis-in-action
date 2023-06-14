package com.nirvana.test.redis

import com.nirvana.test.redis.Chapter07.Ecpm.Ecpm
import redis.clients.jedis.{Jedis, Transaction, ZParams}

import java.util.UUID
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Chapter07 {

  private val QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}")
  private val WORDS_RE = Pattern.compile("[a-z']{2,}")
  private var STOP_WORDS = Set.empty[String]

  for (word <- ("able about across after all almost also am among " +
    "an and any are as at be because been but by can " +
    "cannot could dear did do does either else ever " +
    "every for from get got had has have he her hers " +
    "him his how however if in into is it its just " +
    "least let like likely may me might most must my " +
    "neither no nor not of off often on only or other " +
    "our own rather said say says she should since so " +
    "some than that the their them then there these " +
    "they this tis to too twas us wants was we were " +
    "what when where which while who whom why will " +
    "with would yet you your").split(" ")) {
    STOP_WORDS += word
  }


  def tokenize(content: String): Set[String] = {
    var words = Set.empty[String]
    val matcher = WORDS_RE.matcher(content)
    while (matcher.find) {
      val word = matcher.group.trim
      println(word)
      if (word.length > 2 && !STOP_WORDS.contains(word)) words += word
    }
    words
  }

  def indexDocument(conn: Jedis, docid: String, content: String): Int = {
    val words = tokenize(content)
    val trans = conn.multi
    for (word <- words) {
      trans.sadd("idx:" + word, docid)
    }
    trans.exec.size
  }

  private def setCommon(trans: Transaction, method: String, ttl: Int, items: String*) = {
    val keys = new Array[String](items.length)
    for (i <- 0 until items.length) {
      keys(i) = "idx:" + items(i)
    }
    val id = UUID.randomUUID.toString
    try trans.getClass.getDeclaredMethod(method, classOf[String], classOf[Array[String]]).invoke(trans, "idx:" + id, keys)
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
    trans.expire("idx:" + id, ttl)
    id
  }

  def intersect(trans: Transaction, ttl: Int, items: String*): String = setCommon(trans, "sinterstore", ttl, items:_*)

  def union(trans: Transaction, ttl: Int, items: String*): String = setCommon(trans, "sunionstore", ttl, items:_*)

  def difference(trans: Transaction, ttl: Int, items: String*): String = setCommon(trans, "sdiffstore", ttl, items:_*)

  def parse(queryString: String): Query = {
    val query = new Query()
    val current = mutable.HashSet.empty[String]
    val matcher = QUERY_RE.matcher(queryString.toLowerCase)
    while (matcher.find) {
      var word = matcher.group.trim
      val prefix = word.charAt(0)
      if (prefix == '+' || prefix == '-') word = word.substring(1)
      if (!(word.length < 2 || STOP_WORDS.contains(word))) {
        if (prefix == '-') {
          query.unwanted += word
        } else {
          if (current.nonEmpty && prefix != '+') {
            query.all +:= current.toList
            current.clear
          }
          current += word
        }
      }
    }
    if (current.nonEmpty) query.all +:= current.toList
    query
  }

  class Query {
    final var all = List.empty[List[String]]
    final var unwanted = Set.empty[String]
  }

  def parseAndSearch(conn: Jedis, queryString: String, ttl: Int): String = {
    val query = parse(queryString)
    if (query.all.isEmpty) return null
    var toIntersect = List.empty[String]
    for (syn <- query.all) {
      if (syn.size > 1) {
        val trans = conn.multi
        toIntersect +:= union(trans, ttl, syn:_*)
        trans.exec
      }
      else toIntersect +:= syn.head
    }
    var intersectResult: String = null
    if (toIntersect.size > 1) {
      val trans = conn.multi
      intersectResult = intersect(trans, ttl, toIntersect:_*)
      trans.exec
    }
    else intersectResult = toIntersect.head
    if (query.unwanted.nonEmpty) {
      val keys = query.unwanted.toArray
      keys(keys.length - 1) = intersectResult
      val trans = conn.multi
      intersectResult = difference(trans, ttl, keys:_*)
      trans.exec
    }
    intersectResult
  }

  private val AVERAGE_PER_1K = mutable.HashMap.empty[Ecpm, Double]

  def indexAd(conn: Jedis, id: String, locations: Array[String], content: String, `type`: Ecpm, value: Double): Unit = {
    val trans = conn.multi //一次通信往返完成
    for (location <- locations) {
      trans.sadd("idx:req:" + location, id) //关联位置->广告
    }
    val words = tokenize(content)
    for (word <- tokenize(content)) {
      trans.zadd("idx:" + word, 0, id)
    }
    val avg = if (AVERAGE_PER_1K.contains(`type`)) AVERAGE_PER_1K(`type`) else 1
    val rvalue = toEcpm(`type`, 1000, avg, value)
    trans.hset("type:", id, `type`.toString.toLowerCase) //记录广告类型
    trans.zadd("idx:ad:value:", rvalue, id) //
    trans.zadd("ad:base_value:", value, id)
    for (word <- words) {
      trans.sadd("terms:" + id, word)
    }
    trans.exec
  }

  object Ecpm extends Enumeration {
    type Ecpm = Value
    val CPC, CPA, CPM = Value
  }

  def toEcpm(`type`: Ecpm, views: Double, avg: Double, value: Double): Double = {
    `type` match {
      case Ecpm.CPC =>
      case Ecpm.CPA =>
        return 1000.0 * value * avg / views
      case Ecpm.CPM =>
        return value
    }
    value
  }


  def matchLocation(trans: Transaction, locations: Array[String]): String = {
    val required = new Array[String](locations.length)
    for (i <- locations.indices) {
      required(i) = "req:" + locations(i)
    }
    union(trans, 300, required:_*)
  }

  def indexJob(conn: Jedis, jobId: String, skills: String*): Unit = {
    val trans = conn.multi
    val unique = mutable.HashSet.empty[String]
    for (skill <- skills) {
      trans.sadd("idx:skill:" + skill, jobId) //skill->jobs
      unique.add(skill)
    }
    trans.zadd("idx:jobs:req", unique.size, jobId) //job->needSkillCount
    trans.exec
  }

  def findJobs(conn: Jedis, candidateSkills: String*) = {
    val keys = new Array[String](candidateSkills.length)
    val weights = new Array[Int](candidateSkills.length)
    for (i <- 0 until candidateSkills.length) {
      keys(i) = "skill:" + candidateSkills(i)
      weights(i) = 1
    }
    val trans = conn.multi
    val jobScores = zunion(trans, 30, new ZParams().weights(weights: _*), keys: _*)
    val finalResult = zintersect(trans, 30, new ZParams().weights(-1, 1), jobScores, "jobs:req")
    trans.exec
    conn.zrangeByScore("idx:" + finalResult, 0, 0)
  }

  private def zsetCommon(trans: Transaction, method: String, ttl: Int, params: ZParams, sets: String*) = {
    val keys = new Array[String](sets.length)
    for (i <- 0 until sets.length) {
      keys(i) = "idx:" + sets(i)
    }
    val id = UUID.randomUUID.toString
    try trans.getClass.getDeclaredMethod(method, classOf[String], classOf[Nothing], classOf[Array[String]]).invoke(trans, "idx:" + id, params, keys)
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
    trans.expire("idx:" + id, ttl)
    id
  }

  def zunion(trans: Transaction, ttl: Int, params: ZParams, sets: String*): String = zsetCommon(trans, "zunionstore", ttl, params, sets:_*)

  def zintersect(trans: Transaction, ttl: Int, params: ZParams, sets: String*): String = zsetCommon(trans, "zinterstore", ttl, params, sets:_*)
}
