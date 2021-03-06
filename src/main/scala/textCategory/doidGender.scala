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

    val savepath1 = "hdfs:///lxw/joinedll"
    val savepath2 = "hdfs:///lxw/ratio"


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

        val userId = lineArray(0)
        val appIds = lineArray(1).replaceAll(" +","")

        (userId, appIds)
      }


    val idWithgender = sc.textFile(hdfspath2)
      .map{case line=>

          val linearray = line.split("\t")
          val userId = linearray(0)
          val gender = linearray(1)
        (userId, gender)
      }
      //.reduceByKey(_+_)

    val joined = idWithapp.join(idWithgender).cache()

    val female = joined
      .filter{case (userid, (appid,gender))=>
        gender == "female"
      }
      .sample(false, 0.001)


    val male = joined.filter{case (userid,(appid, gender))=>
      gender == "male"
    }.sample(false, 0.001)




    val appSetmale = male
      .flatMap{case (userId, (appid, gender)) =>


          val appArray= appid.split(",")
          appArray

      }.distinct
    val appSet = female
      .flatMap{case (userId, (appid, gender)) =>


        val appArray= appid.split(",")
        appArray

      }.distinct
      .union (appSetmale)


    val  pathAppId = "hdfs:///lxw/AppId/"
    HDFS.removeFile(pathAppId)
    appSet.repartition(1).saveAsTextFile(pathAppId)


    val pathFemale = "hdfs:///lxw/female/"
    HDFS.removeFile(pathFemale)
    female
      .map{case (x, (y, z)) =>
        x + "\t" + y + "\t" + z
      }
      .repartition(1).saveAsTextFile(pathFemale)


    val pathMale = "hdfs:///lxw/male/"
    HDFS.removeFile(pathMale)
    male
      .map{case (x, (y, z)) =>
        x + "\t" + y + "\t" + z
      }
      .repartition(1).saveAsTextFile(pathMale)













//
//



//    /*val appnum= idWithapp.count()
//    val gendernum = idWithgender.count()
//    val joinnum = joined.count()
//
//    println ("lxw1 " + appnum)
//    println ("lxw1 " + gendernum)
//    println ("lxw1 " + joinnum)*/
//
//    HDFS.removeFile(savepath1)
//    HDFS.removeFile(savepath2)
//
//    joined.saveAsTextFile(savepath1)
//    fmRatio.saveAsTextFile(savepath2)
//
//    sc.stop()

  }

}
