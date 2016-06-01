package textCategory


import org.apache.spark.{SparkConf, SparkContext}
import java.sql.DriverManager

import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext

/**
  * Created by xinmei on 16/5/30.
  */


object DataSql {


  def main(args:Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hdfspath = "hdfs:///lxw/AppId/part-00000"
    val savepath = "hdfs:///lxw/AppWithCate"


    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)




     /* val appId = sc.textFile(hdfspath)
      .map{case line =>

          val id = line.trim()
          id
      }*/




    // read data from sql
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.12.234:3306/koala?user=mosh&password=123456",
        "dbtable" -> "app",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("app")

    val sqlcmd = "select app_id, category from app where is_updated = 1"
    //val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        x(0).toString+ "\t"+x(1).toString
      }.distinct

    HDFS.removeFile(savepath)
    jdbc.saveAsTextFile(savepath)




    sc.stop()
  }

}
