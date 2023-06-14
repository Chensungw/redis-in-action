package com.nirvana.test.redis

import com.nirvana.redis.JedisWrapper


object Chapter03 {
  def notrans(connWrapper: JedisWrapper): Unit = {
    println(connWrapper.getConnection.incr("notrans"))
    Thread.sleep(100)
    connWrapper.getConnection.incrBy("notrans", -1)
  }

  def trans(connWrapper: JedisWrapper): Unit = {
    val transaction = connWrapper.getConnection.multi() //开启事务
    transaction.incr("notrans")
    Thread.sleep(100)
    transaction.incrBy("notrans", -1)
    println(transaction.exec()) //提交事务

  }




}
