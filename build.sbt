name := "gender"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"


// http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"


    