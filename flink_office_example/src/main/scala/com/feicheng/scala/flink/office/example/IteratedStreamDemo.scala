package com.feicheng.scala.flink.office.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Created by Administrator on 2018/10/30.
  */
object IteratedStreamDemo {


  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    env.setBufferTimeout(100)  //默认值是100毫秒  设置为-1 表示 移除超时这样只有当缓冲区满了数据才会被发送，如果想要来一条数据就处理一条可以设置timeout=0来关闭缓存，但是这通常应该避免这么去做。


    env.generateSequence(0,1000).iterate(iteration=>{
      val minusOne = iteration.map( v => v - 1)
      val stillGreaterThanZero = minusOne.filter (_ > 0)
      val lessThanZero = minusOne.filter(_ <= 0)
      (stillGreaterThanZero,lessThanZero)
    })
  }

}
