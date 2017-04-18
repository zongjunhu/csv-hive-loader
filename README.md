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

# Usage
```
LoadCsv --table|-t TABLE --database|-d END_key DATABASE --escape|-e ESC_CHAR --quote|-q QUOTE_CHAR --line-break|-l LB --field-break|-f SEP_CHAR --schema|-s SCHEMA FILENAME --null-value|-n NULL_VALUE FILE_NAME
```
Options:

* --table | -t: table name, required
* --database | -d: data base namea, required
* --escape | -e: escape character, default to '"'
* --quote | -q: quote character, default to '"'
* --field-break | -f: field break character, default to ','
* --line-break | -l: line break character, default to '\n'
* --create-schema | -s: toggle schema auto-discovery, true or false, default to true
* --null-value | -n: null characters, default to 'NULL'
* FILE_NAME: csv file name in hdfs


# Sample job submission
The command line below will submit a spark job in yarn cluster with 40 executors
```
spark-submit --master yarn --num-executors 40 --class LoadCsv loadcsv.jar -t TABLE_NAME -d DATABASE_NAME -f ',' --quote '"' -s false CSV_FILE_PATH_IN_HDFS
```
