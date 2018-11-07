package org.nexus.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class DistinctClientSpark {
    /**
     * @param args
     */
    public static void main(String[] args) {
        simpleFlatMap(args);
    }

    /**
     * @param args
     */
    public static void simpleFlatMap(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Distinct Client");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> fileRDD = context.textFile("src/main/resources/client-ids.log");

        JavaRDD<String> flatRDD = fileRDD.flatMap(x -> Arrays.asList(x.split(",")).iterator());

        flatRDD.distinct().sortBy(a -> a, true, 1).foreach(x -> System.out.println(x));
        context.stop();
    }

    /**
     * @param args
     */
    public static void typeTransformFlatMap(String[] args) {
        //Transform to string and then sort the output
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Sorted Distinct");
        JavaSparkContext context = new JavaSparkContext(conf);
        String inputFile = "src/main/resources/client-ids.log";

        JavaRDD<String> fileRDD = context.textFile(inputFile);

        JavaRDD<String> flatRDD = fileRDD.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        JavaRDD<Integer> intRDD = flatRDD.map( x -> Integer.valueOf(x)).distinct();

        JavaRDD<Integer> sortedRDD = intRDD.sortBy(x -> x, true, 1);

        List<Integer> outList = sortedRDD.collect();
        List<String> outListStr =outList.stream().map(x -> x.toString()).collect(Collectors.toList());

        System.out.println(String.join(",", outListStr));
    }
}
