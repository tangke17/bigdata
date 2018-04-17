package com.loco
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.json.{JSONException, JSONObject}
object SparkStream_Kakfa_Main {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingKakfa").set("spark.driver.allowMultipleContexts", "true")
    //每隔4秒启动sparkstreaming读取kafka一次指定topic和group
    val ssc = new StreamingContext(conf, Seconds(4))
    val topicname = args(0)
    val group = args(1)
    val topicMap = Map(topicname -> 1)
    //    zookeeper quorums server list
    val zkQuorum = "localhost:2181";
    //   consumer group
    //val group = "test-consumer-group01"
    System.out.println("topicname:" + topicname + " ,group:" + group)
    var lines:DStream[String]=null
    //读取kafka中的一条条数据，_._2这是那条内容
    lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    var hdfshbasephoenix:hdfs_hbase_phoenix=null

//    if (tablename=="opcharge") {
//      val sc = ssc.sparkContext
//      hdfshbasephoenix.Main(lines, tablename, sc)
//      println("------------tablename:" + tablename + "---------------")
//      lines.map(x=>x+"---------tablename--------------------------").print()
//    }else{
//      println("------------tablename:" + tablename + "---------------")
//      lines.map(x=>x+"---------tablename--------------------------").print()
//    }
    println("111111111111111111111111111111111111")
    lines.print()
    println("222222222222222222222222222222222")
    if(lines != null) {
      //开始调用方法把sparkstreaming传过去处理
      hdfshbasephoenix = new hdfs_hbase_phoenix()
      val sc = ssc.sparkContext
      hdfshbasephoenix.Main(lines, sc)
    }
    ssc.start()   //Start the computation
    ssc.awaitTermination()   //Wait for the computation to termination
  }
  def isJson(S: String): Boolean = {
    try {
      new JSONObject(S)
      return true
    }
    catch {
      case e: JSONException => println(S)
        return false
    }
  }

}
