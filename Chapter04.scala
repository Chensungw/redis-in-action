package com.nirvana.test.redis

import redis.clients.jedis.Jedis


object Chapter04 {

  /**上架商品*/
  def listItem(conn: Jedis, itemId: String, sellerId: String, price: Double): Boolean = {
    val inventory = "inventory:" + sellerId
    val item = itemId + '.' + sellerId
    val end = System.currentTimeMillis + 5000
    while (System.currentTimeMillis < end) {
      conn.watch(inventory) //监视对应的键,事务执行过程中键被其他客户端改变的话事务会回滚
      if (!conn.sismember(inventory, itemId)) { //商品不存在时直接返回
        conn.unwatch
        return false
      }
      val trans = conn.multi
      trans.zadd("market:", price, item)
      trans.srem(inventory, itemId)
      val results = trans.exec
      // null response indicates that the transaction was aborted due to
      // the watched key changing.
      if (results != null) return true
    }
    false
  }

  /**购买商品*/
  def purchaseItem(conn: Jedis, buyerId: String, itemId: String, sellerId: String, lprice: Double): Boolean = {
    val buyer = "users:" + buyerId
    val seller = "users:" + sellerId
    val item = itemId + '.' + sellerId
    val inventory = "inventory:" + buyerId
    val end = System.currentTimeMillis + 10000
    while (System.currentTimeMillis < end) {
      conn.watch("market:", buyer) //监视买卖市场和买家个人信息
      val price = conn.zscore("market:", item)
      val funds = conn.hget(buyer, "funds").toDouble
      if (price != lprice || price > funds) { //检查价格变化和买家是否有足够的钱
        conn.unwatch
        return false
      }
      val trans = conn.multi
      trans.hincrBy(seller, "funds", price.toInt)
      trans.hincrBy(buyer, "funds", -price.toInt)
      trans.sadd(inventory, itemId)
      trans.zrem("market:", item)
      val results = trans.exec
      // null response indicates that the transaction was aborted due to
      // the watched key changing.
      if (results != null) return true
    }
    false
  }
}
