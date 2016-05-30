package textCategory

import learn.HDFS
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.DriverManager

import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext

/**
  * Created by xinmei on 16/5/30.
  */


object app {

  def main(args:Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hdfspath = "hdfs:///lxw/usertest"

    val savepath = "hdfs:///lxw/app"

    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)


    val text = sc.textFile(hdfspath)
      .flatMap{case line =>

        val lineArray = line.split(",",2)
        val userId = lineArray(0)
        val items = lineArray(1)
        items.replaceAll(" +","").split(",")
          .map{word =>

            (word,userId)
          }
      }


    /*val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.12.234:3306/koala?user=mosh&password=123456",
        "dbtable" -> "app",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("app")

    val sqlcmd = "select app_id, category from app where is_updated = 1"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        (x(0).toString,x(1).toString)
    }*/


    HDFS.removeFile(savepath)
    text. saveAsTextFile(savepath)

    sc.stop()
  }

}
