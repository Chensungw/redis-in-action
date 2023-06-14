package com.nirvana.test.redis

import com.nirvana.redis.JedisWrapper
import redis.clients.jedis.Transaction

import java.io.File
import java.util
import java.util.function.Consumer
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}


object RedisClient extends App {
  val conn = JedisWrapper.createJedisWrapper("192.168.7.58", 6379, "123456").getConnection
  val connWrapper = JedisWrapper.createJedisWrapper("192.168.7.58", 6379, "123456")


  private val transaction: Transaction = conn.multi()
  transaction.get("strk")
  transaction.get("strk1")

  //private val list: util.List[AnyRef] = transaction.exec()
  transaction.exec().asScala.foreach( println)


  //breakable{
  //  var i = 0
  //  while (true) {
  //    println(i)
  //    i += 1
  //    if (i == 10) break
  //  }
  //}
  //
  //
  //println("end")



  //println(Chapter07.tokenize(
  //  """
  //    |Like many people, I first heard about this kind of technique by way of learning about the author William S. Burroughs and his cut-up technique.
  //    |This technique is virtually identical to what Tristan proposed in his earlier work. Burroughs was introduced to this idea by the writer Brion Gysin who seems to have accidentally stumbled upon the idea without prior knowledge of the early work of the Dadaists
  //    |
  //    |As the story goes, sometime in the early 1950s, Brion Gysin was working in his art studio cutting some paper up with a blade. Under the paper were newspapers he put there to protect the table from getting scratched.
  //    |
  //    |Brion noticed that the blade had also cut the newspaper articles and the resulting word fragments seemed fascinating to him. He noticed that the fragments had an unexpected poetic resonance about them. He was so inspired by this incident that he continued to refine and explore the technique in his artwork in the years ahead.
  //    |""".stripMargin))

  //println(conn.get("*"))
  //println(conn.zcard("zk"))

  //println(Chapter06.findPrefixRange("ab").mkString("Array(", ", ", ")")) //aa{ ab{ 之间的字符串，开区间

  //private val random = new Random()
  //for (i <- 0 until 5) {
  //  val ip = random.nextInt(255) + "." + random.nextInt(255) + "." + random.nextInt(255) + "." + random.nextInt(255)
  //  System.out.println(Chapter05.findCityByIp(conn, ip).mkString("Array(", ", ", ")"))
  //}

    //val file = new File("C:\\Users\\admin\\Desktop\\GeoIP_GeoLiteCity-Blocks.csv")
    //val file2 = new File("C:\\Users\\admin\\Desktop\\GeoIP_GeoLiteCity-Location.csv")
    //Chapter05.importIpsToRedis(RedisClient.conn, file)
    //Chapter05.importCitiesToRedis(RedisClient.conn, file2)

  //new Chapter05.CleanCountersThread(conn, timeOffset = 100L).start()
  //for (_ <- 1 to 999) Chapter05.updateCounter(conn, "clickCount", 1, System.currentTimeMillis() / 1000)
  //
  //Chapter05.PRECISION.foreach(p => {
  //  print(p + ":")
  //  Chapter05.getCounter(conn, "clickCount", p).foreach(print)
  //  println()
  //})

  //Chapter05.logRecent(conn, "costLog", "i am a log", Chapter05.WARNING)
  //
  //conn.lrange("recent:costLog:warning", 0, -1).forEach(new Consumer[String] {
  //  override def accept(t: String): Unit = println(t)
  //})


  //Chapter01.postArticle(conn, "mike", "a nature phenomenon", "http://mike.com")
  //Thread.sleep(1)
  //Chapter01.postArticle(conn, "joke", "joke is master", "http://joke.com")
  //Thread.sleep(2)
  //Chapter01.postArticle(conn, "lisa", "i am the queen", "http://lisa.com")
  //Thread.sleep(3)
  //Chapter01.articleVote(conn, "lisa-4", "article:1")
  //Chapter01.getArticles(conn, 1, "time:").foreach(e => {
  //  e.foreach(ee => {
  //    println((ee._1, ee._2))
  //  })
  //  println("-------------")
  //})
  //
  //Chapter02.scheduleRowCache(conn, "2023/06/05", 1)
  //
  //private val thread = new Chapter02.CacheRowsThread(conn)
  //thread.start()
  //while (true){}

  //for (_ <- 1 to 3) {
  //  new Thread(new Runnable {
  //    override def run(): Unit = {
  //      //Chapter03.notrans(connWrapper)
  //      Chapter03.trans(connWrapper)
  //    }
  //  }).start()
  //}
  ////println(connWrapper.getConnection.get("notrans"))
  //Thread.sleep(1000)

  //Chapter02.benchmarkUpdateToken(conn, 5)


}

