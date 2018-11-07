package org.nexus.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SquareCountSpark {
    public static void main1(String[] args) {
        SparkSession session = SparkSession.builder().appName("Compute Square of Numbers")
                .config("spark.master", "local").getOrCreate();

        SparkContext context = session.sparkContext();

        List<Integer> seqNumList = IntStream.rangeClosed(10, 20).boxed().collect(Collectors.toList());


        RDD<Integer> numRDD = context
                .parallelize(JavaConverters.asScalaIteratorConverter(seqNumList.iterator()).asScala()
                        .toSeq(), 2, scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        numRDD.toJavaRDD().foreach(x -> System.out.println(x));
        session.stop();
    }

    public static void main(String[] args) {
        //Alternate way to achieve the same results with JavaSparkContext
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        List<Integer> seqNumList = IntStream.rangeClosed(10, 20).boxed().collect(Collectors.toList());
        JavaRDD<Integer> numRDD = context.parallelize(seqNumList, 2);

        numRDD.map(x -> x*x).foreach(x -> System.out.println(x));

        System.out.println("Done");

        context.stop();
    }
}
