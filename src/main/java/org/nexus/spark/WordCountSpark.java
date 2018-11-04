package org.nexus.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class WordCountSpark {
    public static void main(String[] args) throws IOException {
        String logFile = "/Users/s0m0158/Desktop/tmp/scratch/opt/splunkforwarder/var/log/splunk/metrics.log";
        String outputPath = "/Users/s0m0158/Desktop/tmp/sample-out";

        //If output path exists then delete if
        Files.deleteIfExists(Paths.get(outputPath));

        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();

        RDD<String> fileRDD = sc.textFile(logFile, 1);

        JavaRDD<String> wordMap = fileRDD.toJavaRDD().flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCount = wordMap.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        wordCount.saveAsTextFile(outputPath);
    }
}
