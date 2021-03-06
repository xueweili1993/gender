package textCategory


import org.apache.spark.{SparkConf, SparkContext}
import java.sql.DriverManager

import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext

/**
  * Created by xinmei on 16/5/30.
  */


object app {





  def updatemysql(sc:SparkContext, appList:Array[String])={


    val sqlcmd = appList.map{x => "INSERT IGNORE INTO app (app_id, platform) VALUES ('" + x + "', 'ANDROID')"}



    println("lixuewei: total " + sqlcmd.size)


    //println("lixuewei log3 " + sqlcmd.mkString("\t"))
    //sql connection
    val conn = DriverManager.getConnection("jdbc:mysql://172.31.12.234/koala","mosh", "123456")

    if (!conn.isClosed())
    {
      println("\tSucceeded connecting to the Database!\n")
    }

    val stmt = conn.createStatement()

    for(sql <- sqlcmd){
      println("gyy-log " + sql)
      stmt.executeUpdate(sql)
    }

    stmt.close()
    conn.close()
  }





  def main(args:Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hdfspath = "hdfs:///gaoy/duid2AppsWithLabel/part-00000"
    //val hdfspath = "hdfs:///lxw/usertest"


    val savepath = "hdfs:///lxw/app"
    val savepath1 = "hdfs:///lxw/sql"
    val savepath2 = "hdfs:///lxw/sub"

    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)




  // read data from sql
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.12.234:3306/koala?user=mosh&password=123456",
        "dbtable" -> "app",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("app")

    //val sqlcmd = "select app_id, category from app where is_updated = 1"
    val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        x(0).toString
    }.distinct



//      .cache


 // read data from hdfs
    val text = sc.textFile(hdfspath)
      .flatMap{case line =>

        val first = line.replaceAll("\\(|\\)","")

        val lineArray = first.split(",",2)
//        val userId = lineArray(0)
        val items = lineArray(1)
        items.replaceAll(" +","").split(",")

      }.distinct
      .filter{case item =>

          item.length<100

      }


    // insert data into sql


    val subtracted = text.subtract(jdbc)
      .collect()
    updatemysql(sc:SparkContext, subtracted:Array[String])



    //HDFS.removeFile(savepath)
    //HDFS.removeFile(savepath1)
    //HDFS.removeFile(savepath2)

    //text. saveAsTextFile(savepath)
    //jdbc. saveAsTextFile(savepath1)
    //subtracted. saveAsTextFile(savepath2)

    sc.stop()
  }

}
