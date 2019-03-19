package com.sk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        /**
         * SparkConf：
         * 1.设置spark的运行模式；
         * 2.设置spark的webui显示应用名称
         * 3.设置当前spark应用运行资源，内存或core
         *
         *
         *
         * spark运行模式：
         * 1.local：在eclipse、idea中使用，多用于测试
         * 2.standalone：spark自带的资源调度框架，支持分布是搭建，spark任务可以依赖standalone调度资源
         * 3.yarn：hadoop生态圈资源调度框架，spark也可以基于yarn调度资源
         * 4.mesos：资源调度框架
         */
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount");

        /**
         * SparkContext 是通往集群的唯一通道
         */
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> text = context.textFile("D:\\word.txt");
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {

                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairWord = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduce = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> mapToPair = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey(false);

        JavaPairRDD<String, Integer> result = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        context.stop();
    }
}
