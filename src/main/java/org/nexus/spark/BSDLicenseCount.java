package org.nexus.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class BSDLicenseCount {
    public static void main(String[] args) {
        String licenseFile = "/Users/s0m0158/Desktop/utils/spark/LICENSE";
        SparkSession session = SparkSession.builder().appName("BSD License Count").config("spark.master", "local")
                .getOrCreate();

        SparkContext context = session.sparkContext();

        JavaRDD<String> licFileRDD = context.textFile(licenseFile, 1).toJavaRDD();

        JavaRDD<String> bsdLinesRDD = licFileRDD.filter(x -> x.contains("BSD"));

        //System.out.println("Line count for BSD <> " + bsdLinesRDD.count());

        bsdLinesRDD.take(5).forEach(t -> System.out.println(t));

        session.stop();
    }
}
