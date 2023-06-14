package com.nirvana.test.redis

import redis.clients.jedis.{Jedis, ZParams}

import java.util
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * article:1001 | hash(title,link,time,votes)  | 文章信息
 * time:        | zset(article:1001 | 发布时间) | 根据发布时间排序文章的有序集合
 * score:       | zset(article:1001 | 分数)    | 根据评分排序文章的有序集合
 * voted:1001   | set(用户)                    | 为1001文章投过票的用户
 */

object Chapter01 {
  private val ONE_WEEK_IN_SECONDS = 7 * 86400
  private val VOTE_SCORE = 432
  private val ARTICLES_PER_PAGE = 25


  /**根据某个排序从群组里面获取文章*/
  def getGroupArticles(conn: Jedis, group: String, page: Int, order: String = "score:"): List[mutable.Map[String, String]] = {
    val key = order + group
    if (!conn.exists(key)) {
      val params = new ZParams().aggregate(ZParams.Aggregate.MAX) //挑选相同成员中的最大分值
      conn.zinterstore(key, params, "group:" + group, order)
      conn.expire(key, 60)
    }
    getArticles(conn, page, key)
  }

  /**为文章分组*/
  def addGroups(conn: Jedis, articleId: String, toAdd: Array[String]): Unit = {
    val article = "article:" + articleId
    for (group <- toAdd) {
      conn.sadd("group:" + group, article)
    }
  }

  /** 获取文章列表 */
  def getArticles(conn: Jedis, page: Int, order: String = "score:"): List[mutable.Map[String, String]] = {
    val start = (page - 1) * ARTICLES_PER_PAGE
    val end = start + ARTICLES_PER_PAGE - 1
    val ids = conn.zrevrange(order, start, end) //逆序取数据，直接取出最高评分的文章、最新发布的文章
    val articles = ListBuffer.empty[mutable.Map[String, String]]
    for (id <- ids) {
      val articleData = conn.hgetAll(id)
      articleData.put("id", id)
      articles += articleData.asScala
    }

    articles.toList
  }

  /**发布文章*/
  def postArticle(conn: Jedis, user: String, title: String, link: String): String = {
    val articleId = String.valueOf(conn.incr("article:")) //通过计数器生成文章id
    val voted = "voted:" + articleId //文章的投票者的集合
    conn.sadd(voted, user) //作者加入到已投票的集合
    conn.expire(voted, ONE_WEEK_IN_SECONDS) //设置投票集合过期时间
    val now = System.currentTimeMillis / 1000
    val article = "article:" + articleId
    val articleData = new util.HashMap[String, String]()
    articleData.put("title", title)
    articleData.put("link", link)
    articleData.put("user", user)
    articleData.put("now", String.valueOf(now))
    articleData.put("votes", "1")
    conn.hmset(article, articleData) //设置文章详细信息
    conn.zadd("score:", VOTE_SCORE, article) //设置文章的初始分数
    conn.zadd("time:", now, article) //记录文章发布时间
    articleId
  }

  /**对文章投票*/
  def articleVote(conn: Jedis, user: String, article: String): Unit = {
    val cutoff = (System.currentTimeMillis / 1000) - ONE_WEEK_IN_SECONDS
    if (conn.zscore("time:", article) < cutoff) return //七天前的文章不可以投票

    val articleId = article.substring(article.indexOf(':') + 1)
    if (conn.sadd("voted:" + articleId, user) == 1) {
      conn.zincrby("score:", VOTE_SCORE, article)
      conn.hincrBy(article, "votes", 1)
    }
  }


}
