package com.loco

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, ListBuffer}
import org.json._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json

import scala.collection.mutable
class hdfs_hbase_phoenix {
  var ip:String=null
  var port:String=null
  var hdfsurl:String=null
  var hbaseconf=HBaseConfiguration.create()
  var connection:org.apache.hadoop.hbase.client.Connection=null
  var admin:org.apache.hadoop.hbase.client.Admin=null
  var sc:SparkContext=null
  var sparkstream:DStream[String]=null
  ip="192.168.12.245"
  port="2181"
  val phoenixclient: PhoenixClient = new PhoenixClient(ip, port);


  def Main(lines:DStream[String]=null,sc1:SparkContext):Unit={
    sc=sc1

    if(lines!=null){
      sparkstream=lines

    }


    var tablename:String=""
  //因为sparkstreaming是多个rdd所以要逐条处理
   sparkstream.foreachRDD(foreachFunc = rdd => {
     val jobConf=new JobConf(hbaseconf,this.getClass)
     var phoenixSQL: String =null

     def isJson(S: String): String = {
       try {
         val o = new JSONObject(S)
         var tblname = ""
         if (o.get("oper") == "request") {
           tblname = "request"
         }
         if (o.get("oper") == "log") {
           tblname = "log"
         }
         if (o.get("oper") == "reg") {
           tblname = "reg"
         }
         tblname
       }
       catch {
         case e: JSONException => println(S)
           return "error"
       }
     }
     def getHashCode(str: String): String = {
       val hashcode = (str).hashCode & 0x7FFFFFFF
       hashcode.toString
     }
     def createHbasetable(tablename:TableName): Unit ={
           val descrb=new HTableDescriptor(tablename)
           descrb.addFamily(new HColumnDescriptor("cf".getBytes()))
           admin.createTable(descrb)
           System.out.println("table have created!")

         }
     def convertTimeStamp(datestr: String) = {
       val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
       val c = sdf.parse(datestr).getTime / 100
       c
     }
     def connHbasetable(tablename: String): Unit = {
       jobConf.setOutputFormat(classOf[TableOutputFormat])
       jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
       hdfsurl="hdfs://zk1:9000/hbase"
       hbaseconf.set("hbase.rootdir",hdfsurl)
       hbaseconf.set("hbase.zookeeper.quorum",ip)
       hbaseconf.set("hbase.zookeeper.property.clientPort", port)
       hbaseconf.set("hbase.defaults.for.version.skip", "true")
       hbaseconf.set(TableInputFormat.INPUT_TABLE, tablename)
       connection = ConnectionFactory.createConnection(hbaseconf)
       admin = connection.getAdmin
       println(tablename+"---------------")
       val tbname = TableName.valueOf(tablename)
       if (!admin.tableExists(tbname)) {
         System.out.println(tablename + " is not Exist!")
         createHbasetable(tbname)
         //getHbaseData(tablename,"4_rtt_eee")
         var phoenixSQL = ""
         if (tablename == "stat_charge") {
           /////////////////////////
           sparkstream.map(x=>"-----------"+x+"-----------------").print()
           val rddcharge1 = rdd.filter(f => f.length > 0 && f.indexOf("Logtbl") != -1 && isJson(f) == "opcharge").map(x => new JSONObject(x)).map(x => (x.get("gid") + "_" +
             x.get("sid") + "_" + x.get("usid") + "." + x.get("optime").toString().split(" ")(0), x.get("charge").toString, x.get("optime").toString, x.get("deviceId")))
           val R1 = rddcharge1.map(x => (x._1, x._2.toInt)).reduceByKey(_ + _)
           val R2 = rddcharge1.map(x => (x._1, x._3))
           val R3 = rddcharge1.map(x => (x._1, x._2 + "|" + x._3 + "|" + x._4))
           val finalRdd = R1.join(R2).join(R3)
           val map = mutable.Map.empty[String, Int]
           //put hbase
           val rddcharge2 = finalRdd.map(arr => {
              val put = new Put(Bytes.toBytes(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1))
              val hbasecharge: Integer = getHbaseData(tablename, getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1.toString)
              val getValue = if (map.contains(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1)) map(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1) else 0
              map.put(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1, getValue + arr._2._2.split("\\|")(0).toInt)

              val getcharge: Int = arr._2._1._1

              val totalcharge = map(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1) + hbasecharge
              System.out.println("-----------key:" + arr._1 + " ,hbasecharge:" + hbasecharge + " ,getcharge:" + getcharge + ",totalcharge:" + totalcharge + " ,map(arr._1):" + map(getHashCode(arr._1.split("\\.")(0)) + "|" + arr._1) + "--------------")
              put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("sumcharge"), Bytes.toBytes(totalcharge.toString))
              put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("maxchargetime"), Bytes.toBytes(arr._2._1._2.toString))
              put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("charge"), Bytes.toBytes(getcharge.toString))
              put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("optime"), Bytes.toBytes(arr._2._2.split("\\|")(1)))
              put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("deviceId"), Bytes.toBytes(arr._2._2.split("\\|")(2)))
              (new ImmutableBytesWritable, put)
           })
           rddcharge2.saveAsHadoopDataset(jobConf)
           ///////////////////////
           phoenixSQL = "create table \"" + tablename + "\" (ROW1 varchar not null PRIMARY key,\"cf\".\"sumcharge\" varchar,\"cf\".\"maxchargetime\" varchar," +
             "\"cf\".\"charge\" varchar,\"cf\".\"optime\" varchar,\"cf\".\"deviceId\" varchar)";
//           PhoenixExecSQL("create", phoenixSQL, phoenixclient)
         }

       } else {
         System.out.println(tablename + " is Exist!")

       }

     }
     def getHbaseData(tablename: String, rowkey: String): Int = {
       val tbname = TableName.valueOf(tablename)
       val g = new Get(rowkey.getBytes)
       val table = connection.getTable(tbname)
       val result = table.get(g)
       val getstatus = result.toString.split("=")(1)
       System.out.println("getstatus:" + getstatus)
       if (getstatus == "NONE") {
         System.out.println("result:" + result.toString.split("=")(1))
         return 0
       }
       val value = Bytes.toString(result.getValue("cf".getBytes(), "sumcharge".getBytes()))
       System.out.println("hbaseDatavalue: " + value)

       value.toInt

     }
     def phoenixResult_process(array: JSONArray, phoenixclient: PhoenixClient): Unit = {
       // System.out.println(array)
       for (i <- 0 until array.length()) {
         val o = array.get(i) + "\n"
         //        println(o)
         val n = new json.JSONObject(o)
         var bool = n.has("C.sumcharge")

         if (!bool)
           n.put("C.sumcharge", "0")

         bool = n.has("C.maxchargetime")
         if (!bool)
           n.put("C.maxchargetime", "-1")

         bool = n.has("C.charge")
         n.put("C.charge", "-1")

         bool = n.has("C.optime")
         n.put("C.optime", "-1")

         bool = n.has("C.deviceId")
         n.put("C.deviceId", "-1")


         val n1 = "('" + n.get("L.ROW1") + "','" + n.get("C.sumcharge") + "','" + n.get("C.maxchargetime") + "','" + n.get("C.charge") + "','" + n.get("C.optime") + "','" +
           n.get("C.deviceId") + "','" + n.get("L.level") + "','" + n.get("L.channelid") + "','" + n.get("L.deviceId") + "','" + n.get("L.account") + "','" +
           n.get("L.optime") + "','" + n.get("R.maxregtime") + "','" + n.get("R.channelid") + "','" + n.get("R.deviceId") + "','" + n.get("R.account") + "','" + n.get("R.ip") + "','" + n.get("R.optime") + "','" +
           n.get("(TO_DATE(\"L.optime\", 'yyyy-MM-dd', null) - TO_DATE(\"R.optime\", 'yyyy-MM-dd', null))") + "')"

         val phoenixupdateSQL = "upsert into \"stat_charge_active_reg\" values" + n1
         println(phoenixupdateSQL)
//         phoenixclient.update(phoenixupdateSQL)
       }
     }
     def PhoenixExecSQL(exeType: String, phoenixSQL: String, phoenixclient: PhoenixClient): Unit = {
       if (exeType == "select") {
         val result = phoenixclient.execSql(phoenixSQL)
//         phoenixResult_process(result, phoenixclient)
         // System.out.println(result)
       }
       if (exeType == "create") {
//         phoenixclient.createTable(phoenixSQL)
       }
     }
     def Stat_Charge(jobConf: JobConf): Unit = {
       val tablename = "stat_charge"
       connHbasetable(tablename)

     }
     Stat_Charge(jobConf)

   })


 }
}
