package org.nexus.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class HelloSpark {
    public static void main(String[] args) {
        System.out.println("Sparking");
        String logFile = "/Users/s0m0158/github.com/spark-play1/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}

