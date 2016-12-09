import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import au.com.bytecode.opencsv.CSVReader
import java.io.FileReader
import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.{FileSystem,Path}
import scala.collection.mutable.ListBuffer

object LoadCsv {
    def main(args: Array[String]){

        val usage = """LoadCsv --table|-t TABLE --database|-d END_key DATABASE --escape|-e ESC_CHAR --quote|-q QUOTE_CHAR --line-break|-l LB --field-break|-f SEP_CHAR --schema|-s SCHEMA FILENAME --null-value|-n NULL_VALUE""" 
        val arglist = args.toList 
        type OptionMap = Map[Symbol, Any]

        def nextOption(map : OptionMap, list: List[String]) : OptionMap = {

            def isSwitch(s : String) = (s(0) == '-')

            list match {
                case Nil => map
                case ("--table" | "-t") :: value:: tail => nextOption(map ++ Map('table-> value), tail)
                case ("--database" | "-d") :: value:: tail => nextOption(map ++ Map('dbname-> value), tail)
                case ("--escape" | "-e") :: value:: tail => nextOption(map ++ Map('escape-> value), tail)
                case ("--quote" | "-q") :: value:: tail => nextOption(map ++ Map('quote-> value), tail)
                case ("--field-break" | "-f") :: value :: tail => nextOption(map ++ Map('sep -> value), tail)
                case ("--line-break" | "-l") :: value :: tail => nextOption(map ++ Map('lb -> value), tail)
                case ("--create-schema" | "-s") :: value :: tail => nextOption(map ++ Map('schema -> value), tail)
                case ("--null-value" | "-n") :: value :: tail => nextOption(map ++ Map('null -> value), tail)
                case string :: opt2:: tail if isSwitch(opt2) =>  nextOption(map ++ Map('fname -> string), list.tail)
                case string :: Nil =>  nextOption(map ++ Map('fname -> string), list.tail)
                case option :: tail => println("Unknown option "+option) 
                sys.exit(1)
            }
        }

        val options = nextOption(Map(),arglist)

        val table = options.getOrElse('table, null).asInstanceOf[String]
        val database = options.getOrElse('dbname, null).asInstanceOf[String]
        val escape = options.getOrElse('escape, "\\").asInstanceOf[String]
        val quote = options.getOrElse('quote, "\"").asInstanceOf[String]
        val delimiter = options.getOrElse('sep, ",").asInstanceOf[String]
        val lb = options.getOrElse('lb, null).asInstanceOf[String]
        val int_sch = options.getOrElse('schema, "true").asInstanceOf[String]
        val data_file = options.getOrElse('fname, null).asInstanceOf[String]
        val null_value = options.getOrElse('null, "NULL").asInstanceOf[String]

        //println(s"escape: $escape")
        //println(s"quote: $quote")
        //println(s"delimiter: $delimiter")
        if(List(table, database, data_file).exists(x=>x!=null)){
            println("required arguments missing")
            println(usage)
        }

        val sparkConf = new SparkConf().setAppName("LoadCsv")
        if(lb != null)
            sparkConf.set("spark.hadoop.textinputformat.record.delimiter", lb)

        val sc = new SparkContext(sparkConf)

        val sqlContext = new HiveContext(sc)

        val df = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", int_sch match { case "true" => "true"; case _ => "false"})
            .option("delimiter", delimiter)
            .option("quote", quote)
            .option("escape", escape)
            //.option("delimiter", ",")
            //.option("quote", "\"")
            //.option("escape", "\"")
            .option("nullValue", null_value)
            .load(data_file)

        df.printSchema()
        
        val columns = df.columns

        val column_map = Map("group" -> "group_", "comment" -> "comment_")

        var ndf = df

        for(f<- columns){
            val nf = f.replaceAll("\\W+", " ").trim.replaceAll(" ", "_")
            if(nf == ""){
                ndf = ndf.drop(f)
            }else{
                if(column_map contains nf.toLowerCase){
                    ndf = ndf.withColumnRenamed(f, column_map(nf.toLowerCase))
                }else if(f != nf){
                    ndf = ndf.withColumnRenamed(f, nf)
                }
            }
        }
        
        ndf.dropDuplicates.registerTempTable("tmptbl")

        sqlContext.sql(s"drop table if exists ${database}.${table}")

        sqlContext.sql(s"create table ${database}.${table} stored as parquet as select * from tmptbl");
    }
}


object CheckFile{
    def main(args: Array[String]){

        val sparkConf = new SparkConf().setAppName("CheckFile")

        if(args.length < 1){
            println("file name required")
            sys.exit(1)
        }
        val file = args(0)
        if(args.length > 1)
            sparkConf.set("spark.hadoop.textinputformat.record.delimiter", args(1).substring(0, 1))

        val sc = new SparkContext(sparkConf)

        val f = sc.textFile(file).cache

        f.filter(x => { val c=x.count(_ == '"'); c > 22 && c<30} ).map(x => "ERRORLINE: "+x).saveAsTextFile("csv_errors_2230")

        val count = f.count
        val res = f.map(x => (x.count(_ == '"'), 1)).reduceByKey(_+_).collect()
        println("rows: "+count)
        for(x <- res){
            println(s"${x._1}: ${x._2}")
        }
    }
}

object CheckCsv{
    def main(args: Array[String]){

        val data_file = args(0)
        val lb = args(1)

        val sparkConf = new SparkConf().setAppName("CheckCsv")
        if(lb != null)
            sparkConf.set("spark.hadoop.textinputformat.record.delimiter", lb)

        val sc = new SparkContext(sparkConf)

        val sqlContext = new HiveContext(sc)

        val df = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", ",")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("nullValue", "NULL")
            .load(data_file)

        df.printSchema()
    }

}

object OpenCSVTest{
    def main(args: Array[String]){
        val reader = new CSVReader(new FileReader("csv_errors.txt"), ',', '"')
        for (row <- reader.readAll) {
            println("In " + row(0) + " there were " + row(1) + " cases")
            println("    seq    : "+row(8))
        }
    }
}

object CopyLocalFileToHdfs {

    def make_dirs_recursively(fs: FileSystem, targetPath: String):Unit = {
        println(s"create remote target path: $targetPath")
        val tokens = targetPath.split("/")
        for(i <- 2 to tokens.length){
            val remote_path = new Path(tokens.slice(0, i).mkString("/"))
            if(!fs.exists(remote_path)){
                println(s"create remote path: ${remote_path.getName}")
                fs.mkdirs(remote_path)
            }
        }
    }


    def main(args: Array[String]){

        val usage = """CopyLocalFileToHdfs --remote|-r HDFS_TARGET --local|-l LOCAL_PATH --overwrite|-o FORCE_OVERWRITE"""

        val arglist = args.toList 
        type OptionMap = Map[Symbol, Any]

        def nextOption(map : OptionMap, list: List[String]) : OptionMap = {

            def isSwitch(s : String) = (s(0) == '-')

            list match {
                case Nil => map
                case ("--remote" | "-r") :: value:: tail => nextOption(map ++ Map('remote-> value), tail)
                case ("--local" | "-l") :: value:: tail => nextOption(map ++ Map('local -> value), tail)
                case ("--overwrite" | "-o") :: tail => nextOption(map ++ Map('overwrite -> true), tail)
                case option :: tail => println("Unknown option "+option) 
                sys.exit(1)
            }
        }

        val options = nextOption(Map(),arglist)

        val remote = options.getOrElse('remote, null).asInstanceOf[String]
        val local = options.getOrElse('local, null).asInstanceOf[String]
        val overwrite = options.getOrElse('overwrite, false).asInstanceOf[Boolean]
        
        val sparkConf = new SparkConf().setAppName("CheckCsv")

        val sc = new SparkContext(sparkConf)

        val local_files = ListBuffer.empty[String]

        val f = new File(local)

        if(f.exists){
            if(f.isDirectory){
                println(s"$local is a directory, scan files")
                for( i <- f.listFiles){
                    if(i.isFile){
                        if(i.length > 0){
                            local_files += i.getPath
                        }else{
                            println(s"zero length file ignored: $i")
                        }
                    }
                }
            }else{
                println(s"$local is a file")
                local_files += local
            }
        }else{
            println(s"$local does not exists in local system")
            sys.exit(1)
        }

        println("file to copy from local system:")
        println(local_files.map(" "+_).mkString("\n")) 


        val fs = FileSystem.get(sc.hadoopConfiguration)
        val remote_path = new Path(remote)
        if(fs.exists(remote_path)){
            println(s"remote file $remote exists")
            if(fs.isDirectory(remote_path)){
                println(s"remote path $remote is directory")
            }else{
                println("remote directory required")
                sys.exit(1)
            }
        }else{
            make_dirs_recursively(fs, remote)
        }

        for(lf <- local_files){
            val fname = lf.split("/").last
            val remote_path = new Path(remote+"/"+fname)
            if(!fs.exists(remote_path) || overwrite){
                println(s"copy $fname to $remote")
                fs.copyFromLocalFile(false, true, new Path(lf), remote_path)
            }else{
                println(s"$fname exists on remote side, ignored")
            }
        }

    }

}
