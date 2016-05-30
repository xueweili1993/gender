package textCategory

import learn.HDFS
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by xinmei on 16/5/30.
  */


object app {

  def main(args:Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hdfspath = "hdfs:///gaoy/duid2AppsWithLabel/part-00000"

    val savepath = "hdfs:///lxw/app"

    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)


    val text = sc.textFile(hdfspath)
      .map{line =>

        val lineArray = line.split(",",2)
        val userId = lineArray(0)
        val items = lineArray(1)
        val itemsArray = items.split(",")
        val item1= itemsArray(0)
        (userId, item1)
      }

    HDFS.removeFile(savepath)
    text. saveAsTextFile(savepath)

    sc.stop()
  }

}
