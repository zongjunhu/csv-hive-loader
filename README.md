# csv-hive-loader
parse csv from hdfs, parse and then push it to hive table

Source codes have been tested in CDH 5.7.1. It is supposed to work in CDH version 5.7.1 and above.

# Download
```
git clone https://github.com/zongjunhu/csv-hive-loader
```

# Compile
```
cd csv-hive-loader
cd scala
sbt assembly
```
The result jar file will be saved in `scala/target/scala-2.10` as 'loadcsv.jar`.

# Sample job submission
The command line below will submit a spark job in yarn cluster with 40 executors
```
spark-submit --master yarn --num-executors 40 --class LoadCsv target/scala-2.10/loadcsv.jar -t TABLE_NAME -d DATABASE_NAME -f ',' --quote '"' -s false CSV_FILE_PATH_IN_HDFS
```
