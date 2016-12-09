name := "Load Csv"

version := "1.0"

/*scalaVersion in ThisBuild := "2.10.5"*/
scalaVersion := "2.10.5"

resolvers ++= Seq(
    "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.7.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.0-cdh5.7.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.6.0-cdh5.7.1" % "provided",
    "org.apache.spark" %% "spark-hive" % "1.6.0-cdh5.7.1" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.7.1" % "provided",
    "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.7.1" % "provided",
    "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.7.1" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.7.1" % "provided",
    "com.databricks" %% "spark-csv" % "1.4.0",
    "org.apache.commons" % "commons-io" % "1.3.2",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"
)

assemblyJarName in assembly := "loadcsv.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
/*libraryDependencies += "org.apache.hbase" %% "hbase-spark" % "2.0.0-SNAPSHOT"*/

