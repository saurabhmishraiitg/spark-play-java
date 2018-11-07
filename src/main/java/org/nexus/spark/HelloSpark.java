package org.nexus.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HelloSpark {
    public static void main(String[] args) {
        System.out.println("Sparking");
        // Sample large log file - /Users/s0m0158/Desktop/tmp/scratch/opt/splunkforwarder/var/log/splunk/metrics.log
        String logFile = "/Users/s0m0158/Desktop/tmp/scratch/opt/splunkforwarder/var/log/splunk/metrics.log"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("INFO")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("ERROR")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}

