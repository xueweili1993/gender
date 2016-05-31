package textCategory

import org.apache.spark.{SparkConf, SparkContext}
import java.sql.DriverManager

import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext

/**
  * Created by xinmei on 16/5/31.
  */
object doidGender {


  def main (args: Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hdfspath1 = "hdfs:///gaoy/duid2AppsWithLabel/part-00000"
    val hdfspath2 = "hdfs:////gaoy/genderLabeledData/part-00000"

    //val savepath1 = "hdfs:///lxw/"


    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)


    val idWithapp = sc.textFile(hdfspath1)
      .map{case line =>

        val first = line.replaceAll("\\(|\\)","")

        val lineArray = first.split(",",2)

        lineArray(0)


      }


    val idWithgender = sc.textFile(hdfspath2)
      .map{case line=>

          val linearray = line.split("\t")
          linearray(0)
      }
    val joined = idWithgender.intersection(idWithapp)

    val appnum= idWithapp.count()
    val gendernum = idWithgender.count()
    val joinnum = joined.count()

    println ("lxw1 " + appnum)
    println ("lxw1 " + gendernum)
    println ("lxw1 " + joinnum)

  }

}
