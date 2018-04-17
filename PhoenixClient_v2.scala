package com.loco
import java.sql._

import scala.Array
import org.json.JSONException
import org.json.JSONObject

import scala.math._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection => _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable._
class PhoenixClient_v2(host:String,port:String) {
  private val url = "jdbc:phoenix:" + host + ":" + port
  private var conn: Connection = DriverManager.getConnection(url)
  private var ps: PreparedStatement = null
  try{
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  }
  catch{
    case e :ClassNotFoundException => println(e)
  }
  def Select_Sql(phoenixSQL: String):ArrayBuffer[immutable.Map[String,String]] = {
    if (phoenixSQL == null || (phoenixSQL.trim == "" )) println("PhoenixSQL is needed")
    //存放列名
    val cols = new ArrayBuffer[String]
    //定义一个数组缓冲，存放每一行的列名及值组成的映射
    val All_Res = new ArrayBuffer[immutable.Map[String,String]]()
    //定义连接和获取表
    //val url = "jdbc:phoenix:" + host + ":" + port
    //val conn = DriverManager.getConnection(url)
    val stmt = conn.createStatement()
    try {
      //      println("Select_Sql:"+phoenixSQL)
      val set = stmt.executeQuery(phoenixSQL)
      //      println("set = "+set.toString)
      val meta: ResultSetMetaData = set.getMetaData//获得ResultSetMeataData对象
      //      println("meta = "+meta.toString)
      //遍历set
      var KvMap =  new HashMap[String,String]
      //      var flag=false
      while (set.next()) {

        //如果存放列名的数组为空，则取列名
        if (cols.length != meta.getColumnCount) {
          for( i <- 1.to(meta.getColumnCount)){ cols += meta.getColumnLabel(i)}
        }
        cols.foreach(x=> KvMap(x) = set.getString(x) )
        //将每个映射添加到数组缓冲中
        All_Res += KvMap.toMap
      }
    }
    catch {
      case e: Exception => e.printStackTrace();println("SQL exec error:" + e.getMessage)
      case e: JSONException => e.printStackTrace();println("JSON error:" + e.getMessage)
    }
    finally {
      conn.close()
      stmt.close()
    }
    All_Res
  }
  def Update_Insert_Delete(phoenixSQL: String) = {
    var res = 0
    if (phoenixSQL == null || (phoenixSQL.trim ==  "")) {
      System.out.println("PhoenixSQL is needed")
      res = -1
    }else {
      //val url = "jdbc:phoenix:" + host + ":" + port
      //val conn = DriverManager.getConnection(url)
      val stmt = conn.createStatement()
      try {
        res = stmt.executeUpdate(phoenixSQL)
        conn.commit()
      }
      catch {
        case e: SQLException => println(phoenixSQL.trim.split(" ")(0)+" Except:" + e.toString)
      }
      finally {
        conn.close()
        stmt.close()
      }
    }
    res
  }

  def CreateTable(phoenixSQL: String) = {
    var res = 0
    System.out.println("phoenixSQL:" + phoenixSQL)
    val tablename: String = phoenixSQL.split(" ")(2).trim
    if (phoenixSQL == null || (phoenixSQL.trim eq "")) {
      System.out.println("PhoenixSQL is needed")
      res = -1
    }else {
      //      val url = "jdbc:phoenix:" + host + ":" + port
      //      val conn = DriverManager.getConnection(url)
      val stmt = conn.createStatement()
      try {
        val tab_cnt = stmt.executeQuery("select count(DISTINCT TABLE_NAME) as \"cnt\"from SYSTEM.CATALOG where table_name='" + tablename + "'")

        while(tab_cnt.next()) {
          if (tab_cnt.getInt("cnt") == 0) {
            println("executing create table...")
            res = stmt.executeUpdate(phoenixSQL)
          }
          else
            println(tablename + " is exists!")
        }
      }
      catch {
        case e: SQLException => println("create table except:" + e.getMessage)
      }
      finally {
        conn.close()
        stmt.close()
      }
    }
    res
  }
  //获取upsert语句
  def GetUpsertSql(ColumnArr:Array[String],tablename:String) = {
    //拼出插入语句
    var UpsertSql = "upsert into \""+tablename+"\"(\"pk\""
    ColumnArr.foreach(x=>UpsertSql += ",\""+x+"\"")
    UpsertSql += ") values(?"
    ColumnArr.foreach(x=> UpsertSql += ",?")
    UpsertSql += ")"
    println("UpsertSql"+UpsertSql)
    UpsertSql
  }
}
